#!/usr/bin/env python3
import os, json, yaml, pathlib
from jsonschema import validate
from dotenv import load_dotenv
BASE = pathlib.Path(__file__).resolve().parents[1]
POLICY_DIR = BASE / 'policies'
SCHEMA = json.load(open(BASE / 'policy_schemas/policy.schema.json'))
OUTPUT_DIR = BASE / 'outputs'
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
DBT_MANIFEST = os.environ.get('DBT_MANIFEST', str(BASE / 'dbt_project/target/manifest.json'))
def read_policies():
    out = []
    for f in sorted(POLICY_DIR.glob('*.yaml')):
        data = yaml.safe_load(open(f))
        validate(instance=data, schema=SCHEMA)
        out.append(data)
    return out
def load_manifest(path):
    p = pathlib.Path(path)
    if not p.exists():
        return {'nodes':{}, 'sources':{}}
    return json.loads(p.read_text())
def extract_assets(manifest):
    nodes = {**manifest.get('nodes',{}), **manifest.get('sources',{})}
    assets = {}; edges = []
    for k,n in nodes.items():
        if n.get('resource_type') not in ('model','seed','source'): continue
        name = n.get('name') or n.get('alias') or k
        db = n.get('database') or 'DEMO_DB'; schema = n.get('schema') or 'PUBLIC'
        fq = f"{db}.{schema}.{name}"
        cols = {}
        for cname,cdef in (n.get('columns') or {}).items():
            cols[cname.upper()] = {'tags':[t.upper() for t in (cdef.get('tags') or [])]}
        assets[fq.upper()] = {'tags':[t.upper() for t in (n.get('tags') or [])], 'columns':cols}
        for parent in n.get('depends_on',{}).get('nodes',[]):
            if parent in nodes:
                pn = nodes[parent]; pname = pn.get('name') or pn.get('alias')
                pdb = pn.get('database') or 'DEMO_DB'; pschema = pn.get('schema') or 'PUBLIC'
                pfq = f"{pdb}.{pschema}.{pname}"
                edges.append((pfq.upper(), fq.upper()))
    return assets, edges
def column_matches(coltags, tags_any, tags_all):
    st = set([t.upper() for t in (coltags or [])])
    any_ok = True if not tags_any else bool(st.intersection(set([t.upper() for t in tags_any])))
    all_ok = True if not tags_all else set([t.upper() for t in tags_all]).issubset(st)
    return any_ok and all_ok
def target_columns(assets, scope):
    cols = []
    include = [s.upper() for s in (scope.get('include') or [])]
    exclude = [s.upper() for s in (scope.get('exclude') or [])]
    tags_any = scope.get('tags_any') or []; tags_all = scope.get('tags_all') or []
    for tbl, meta in assets.items():
        if include and tbl not in include: continue
        if exclude and tbl in exclude: continue
        for c, cmeta in (meta.get('columns') or {}).items():
            if column_matches(cmeta.get('tags', []), tags_any, tags_all):
                cols.append((tbl, c))
    return cols
def masking_expr(strategy):
    s = (strategy or '').lower()
    if s == 'email_partial': return "REGEXP_REPLACE(val,'(.*)@','***@')"
    if s == 'phone_last4': return "REGEXP_REPLACE(val,'(.*)([0-9]{4})$','***\\2')"
    if s == 'full_redact': return "'***'"
    if s == 'cc_last4': return "REGEXP_REPLACE(val,'(.*)([0-9]{4})$','****\\2')"
    if s == 'address_city_only': return "REGEXP_REPLACE(val,'^(.*),\\s*([^,]+)$','\\2')"
    if s == 'ip_anonymize': return "REGEXP_REPLACE(val,'(\\d+\\.\\d+\\.)(\\d+)(\\.\\d+)','\\1***\\3')"
    if s == 'pan_mask': return "REGEXP_REPLACE(val,'.*(\\d{4})$','****\\1')"
    if s == 'bank_last4': return "REGEXP_REPLACE(val,'(.*)([0-9]{4})$','***\\2')"
    if s == 'gps_city': return "'CITY_LEVEL'"
    if s == 'device_hash': return "MD5(val)"
    if s == 'always_false': return "FALSE"
    return "'***'"
def policy_sql(policy, assets):
    sqls=[]; rollback=[]; impact=[]
    subjects = policy.get('subjects',{}); allow = subjects.get('allow_roles',[])
    allow_list = ",".join([f"'{r}'" for r in allow]) if allow else "''"
    targets = target_columns(assets, policy.get('scope',{}))
    for action in policy.get('actions',[]):
        t = action.get('type')
        if t in ('MASK','REDACT'):
            expr = masking_expr((action.get('masking') or {}).get('strategy'))
            for tbl,col in targets:
                polname = f"POL_{policy['id']}_{tbl.replace('.','_')}_{col}"
                ddl = f"CREATE OR REPLACE MASKING POLICY {polname} AS (val STRING, role STRING) RETURNS STRING ->\nCASE WHEN role IN ({allow_list}) THEN val ELSE {expr} END;"
                alter = f"ALTER TABLE {tbl} MODIFY COLUMN {col} SET MASKING POLICY {polname} USING ({col}, CURRENT_ROLE());"
                rb1 = f"ALTER TABLE {tbl} MODIFY COLUMN {col} UNSET MASKING POLICY;"
                rb2 = f"DROP MASKING POLICY IF EXISTS {polname};"
                sqls.extend([ddl, alter]); rollback.extend([rb1, rb2]); impact.append({'table':tbl,'column':col,'policy':polname,'action':t})
        elif t == 'ROW_FILTER':
            where = masking_expr((action.get('masking') or {}).get('strategy'))
            for tbl,_ in set(targets):
                polname = f"RAP_{policy['id']}_{tbl.replace('.','_')}"
                ddl = f"CREATE OR REPLACE ROW ACCESS POLICY {polname} AS (role STRING, REGION STRING) RETURNS BOOLEAN ->\nCASE WHEN role IN ({allow_list}) THEN TRUE ELSE ({where}) END;"
                alter = f"ALTER TABLE {tbl} ADD ROW ACCESS POLICY {polname} ON (REGION);"
                rb1 = f"ALTER TABLE {tbl} DROP ROW ACCESS POLICY {polname};"
                rb2 = f"DROP ROW ACCESS POLICY IF EXISTS {polname};"
                sqls.extend([ddl, alter]); rollback.extend([rb1, rb2])
    return sqls, rollback, impact
def mermaid(edges, highlight):
    lines=['```mermaid','graph LR']
    for u,v in edges: lines.append(f'  "{u}" --> "{v}"')
    for h in highlight: lines.append(f'  style "{h}" fill:#fdd,stroke:#f66,stroke-width:2px')
    lines.append('```'); return '\n'.join(lines)
def main(simulate=True):
    manifest = load_manifest(DBT_MANIFEST); assets, edges = extract_assets(manifest)
    policies = read_policies(); all_sql=[]; all_rb=[]; impact=[]; highlight=set()
    for pol in policies:
        sqls, rbs, cols = policy_sql(pol, assets)
        all_sql.extend(sqls); all_rb.extend(rbs); impact.extend(cols)
        for c in cols: highlight.add(c['table'])
    out_apply = OUTPUT_DIR / ('snowflake_apply.sql' if not simulate else 'snowflake_simulate.sql')
    out_rb = OUTPUT_DIR / 'snowflake_rollback.sql'
    out_apply.write_text(';\n'.join(all_sql) + ';\n')
    out_rb.write_text(';\n'.join(all_rb[::-1]) + ';\n')
    (OUTPUT_DIR / 'impact.json').write_text(json.dumps({'columns':impact, 'tables': sorted(list(highlight))}, indent=2))
    (OUTPUT_DIR / 'lineage.md').write_text(mermaid(edges, list(highlight)))
    print(f'Generated apply: {len(all_sql)} statements; rollback: {len(all_rb)}')
if __name__ == '__main__':
    import argparse; ap = argparse.ArgumentParser(); ap.add_argument('--simulate', action='store_true'); args = ap.parse_args(); main(simulate=args.simulate)

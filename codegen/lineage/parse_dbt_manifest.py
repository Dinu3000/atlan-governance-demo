import json, pathlib
def load_manifest(path):
    p = pathlib.Path(path)
    if not p.exists(): return {'nodes':{}, 'sources':{}}
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

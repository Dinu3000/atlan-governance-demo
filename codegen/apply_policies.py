#!/usr/bin/env python3
import os, pathlib, json, datetime
from dotenv import load_dotenv
BASE = pathlib.Path(__file__).resolve().parents[1]
OUT_SQL = BASE / 'outputs' / 'snowflake_apply.sql'
AUDIT = BASE / 'logs' / 'audit.jsonl'
def load_sql():
    if not OUT_SQL.exists():
        raise FileNotFoundError('Run generate_snowflake_sql.py first')
    raw = OUT_SQL.read_text(); return [s.strip() for s in raw.split(';\n') if s.strip()]
def audit(evt):
    AUDIT.parent.mkdir(parents=True, exist_ok=True)
    evt['ts'] = datetime.datetime.utcnow().isoformat()
    with open(AUDIT,'a') as f: f.write(json.dumps(evt)+'\n')
def main():
    try:
        from dotenv import load_dotenv as ld; ld(BASE/'.env')
    except Exception: pass
    account = os.environ.get('SNOWFLAKE_ACCOUNT'); user = os.environ.get('SNOWFLAKE_USER'); pwd = os.environ.get('SNOWFLAKE_PASSWORD')
    if not (account and user and pwd):
        print('Snowflake creds missing — simulation only.')
        for s in load_sql(): audit({'type':'SIMULATE','stmt':s,'ok':True})
        print('Audit written.'); return
    import snowflake.connector
    conn = snowflake.connector.connect(account=account,user=user,password=pwd,role=os.environ.get('SNOWFLAKE_ROLE'),warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),database=os.environ.get('SNOWFLAKE_DATABASE'),schema=os.environ.get('SNOWFLAKE_SCHEMA'))
    cur = conn.cursor()
    try:
        for s in load_sql():
            try: cur.execute(s); audit({'type':'EXECUTE','stmt':s,'ok':True})
            except Exception as e: audit({'type':'EXECUTE','stmt':s,'ok':False,'error':str(e)}); raise
        conn.commit(); print('Applied; audit logged.')
    finally:
        cur.close(); conn.close()
if __name__ == '__main__': main()

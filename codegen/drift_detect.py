#!/usr/bin/env python3
import os, pathlib, json
BASE = pathlib.Path(__file__).resolve().parents[1]
OUT_SQL = BASE / 'outputs' / 'snowflake_apply.sql'
DRIFT = BASE / 'outputs' / 'drift_report.json'
def main():
    try:
        from dotenv import load_dotenv as ld; ld(BASE/'.env')
    except Exception: pass
    account = os.environ.get('SNOWFLAKE_ACCOUNT'); user = os.environ.get('SNOWFLAKE_USER'); pwd = os.environ.get('SNOWFLAKE_PASSWORD')
    warehouse_state = {'masking':[], 'row_access':[]}
    if account and user and pwd:
        import snowflake.connector
        conn = snowflake.connector.connect(account=account,user=user,password=pwd)
        cur = conn.cursor()
        try:
            cur.execute('SHOW MASKING POLICIES'); rows = cur.fetchall(); warehouse_state['masking'] = [r[1] for r in rows]
            cur.execute('SHOW ROW ACCESS POLICIES'); rows = cur.fetchall(); warehouse_state['row_access'] = [r[1] for r in rows]
        finally:
            cur.close(); conn.close()
    desired = OUT_SQL.read_text() if OUT_SQL.exists() else ''
    report = {'desired_len': len(desired), 'warehouse': warehouse_state}
    DRIFT.write_text(json.dumps(report, indent=2))
    print('Drift report written')
if __name__ == '__main__': main()

import os, pathlib, streamlit as st
BASE = pathlib.Path(__file__).resolve().parents[1]
OUT = BASE / 'outputs'
LOG = BASE / 'logs' / 'audit.jsonl'
st.set_page_config(page_title='Atlan Governance Demo', layout='wide')
st.title('Atlan Governance Control-Plane Demo (Local)')
col1, col2 = st.columns([1,3])
with col1:
    if st.button('Simulate (generate SQL)'): os.system(f'python {BASE}/codegen/generate_snowflake_sql.py --simulate'); st.success('Simulation generated.')
    if st.button('Generate apply + rollback'): os.system(f'python {BASE}/codegen/generate_snowflake_sql.py'); st.success('Apply + rollback generated.')
    if st.button('Apply to Snowflake (uses .env)'): os.system(f'python {BASE}/codegen/apply_policies.py'); st.success('Apply (or simulation) finished.')
    if st.button('Run drift detection'): os.system(f'python {BASE}/codegen/drift_detect.py'); st.success('Drift detection run.')
with col2:
    st.subheader('Impact preview (impact.json)')
    ip = OUT / 'impact.json'
    if ip.exists(): st.code(ip.read_text(), language='json')
    else: st.info('Run simulate to produce impact.json')
    st.subheader('Lineage (Mermaid)')
    lm = OUT / 'lineage.md'
    if lm.exists(): st.markdown(lm.read_text())
    else: st.info('Run simulate to produce lineage.md')
    st.subheader('SQL (simulate)'); sim = OUT / 'snowflake_simulate.sql'
    if sim.exists(): st.code(sim.read_text(), language='sql')
    st.subheader('SQL (apply)'); app = OUT / 'snowflake_apply.sql'
    if app.exists(): st.code(app.read_text(), language='sql')
    st.subheader('Rollback SQL'); rb = OUT / 'snowflake_rollback.sql'
    if rb.exists(): st.code(rb.read_text(), language='sql')
    st.subheader('Audit log (last 200 lines)')
    if LOG.exists():
        with open(LOG) as f: lines = f.readlines()[-200:]; 
        for ln in lines: st.text(ln.strip())
    else: st.info('No audit log yet.')

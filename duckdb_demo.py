import pandas as pd, pathlib
BASE = pathlib.Path(__file__).resolve().parent
def mask_email(v): 
    if pd.isna(v): return v
    parts = str(v).split('@'); return '***@' + parts[1] if len(parts)>1 else '***'
def phone_last4(v):
    s = str(v); return '***' + s[-4:] if len(s)>=4 else '***'
def main():
    c = pd.read_csv(BASE / 'data' / 'customers.csv', dtype=str)
    print('--- BEFORE ---'); print(c.head().to_string())
    c['EMAIL_MASKED'] = c['EMAIL'].apply(mask_email)
    c['PHONE_MASKED'] = c['PHONE'].apply(phone_last4)
    print('\n--- AFTER ---'); print(c[['CUSTOMER_ID','EMAIL_MASKED','PHONE_MASKED']].to_string())
if __name__ == '__main__': main()

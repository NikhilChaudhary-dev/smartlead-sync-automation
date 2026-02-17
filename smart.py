import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
import os
import json

# --- CONFIGURATION FROM SECRETS ---
SMARTLEAD_API_KEY = os.environ.get('SMARTLEAD_API_KEY')
GOOGLE_JSON_DATA = os.environ.get('GOOGLE_SHEETS_JSON') # JSON string format 
SHEET_NAME = "Smartlead_Jan2026_Leads"

BASE_URL = "https://server.smartlead.ai/api/v1"
CUTOFF_DATE = datetime(2026, 1, 1)

def setup_gsheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    creds_dict = json.loads(GOOGLE_JSON_DATA)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).sheet1
    
    first_row = sheet.row_values(1)
    headers = ["Campaign Name", "Lead Name", "Email", "Opens", "Sent At", "Open At", "Gap (Min)"]
    if not first_row or first_row[0] != "Campaign Name":
        sheet.insert_row(headers, 1)
    
    existing_emails = set(sheet.col_values(3)) 
    return sheet, existing_emails

def run_live_automation():
    if not SMARTLEAD_API_KEY or not GOOGLE_JSON_DATA:
        print("❌ Error: API Key or Google JSON Secret is missing!")
        return

    sheet, existing_emails = setup_gsheet()
    params = {'api_key': SMARTLEAD_API_KEY}
    
    print(f"🚀 SYNC STARTED: Campaigns after {CUTOFF_DATE.date()}...")
    
    response = requests.get(f"{BASE_URL}/campaigns", params=params)
    if response.status_code != 200: return

    campaigns = response.json()

    for camp in campaigns:
        created_at_str = camp.get('created_at', '')
        clean_ts = created_at_str.split('.')[0].replace('Z', '')
        camp_date = datetime.strptime(clean_ts, '%Y-%m-%dT%H:%M:%S')

        if camp_date >= CUTOFF_DATE:
            print(f"🔍 Checking: {camp['name']}")
            stats_url = f"{BASE_URL}/campaigns/{camp['id']}/statistics"
            offset = 0
            camp_leads_batch = []

            while True:
                s_res = requests.get(stats_url, params={'api_key': SMARTLEAD_API_KEY, 'offset': offset, 'limit': 100})
                if s_res.status_code != 200: break
                
                leads = s_res.json().get('data', [])
                if not leads: break

                for lead in leads:
                    email = lead.get('lead_email')
                    if email in existing_emails: continue

                    if lead.get('open_count', 0) >= 2:
                        s_time = lead.get('sent_time')
                        o_time = lead.get('open_time')

                        if s_time and o_time:
                            s_dt = datetime.strptime(s_time.split('.')[0].replace('Z', ''), '%Y-%m-%dT%H:%M:%S')
                            o_dt = datetime.strptime(o_time.split('.')[0].replace('Z', ''), '%Y-%m-%dT%H:%M:%S')
                            gap_seconds = (o_dt - s_dt).total_seconds()
                            
                            if gap_seconds >= 120:
                                gap_minutes = round(gap_seconds / 60, 2)
                                row = [camp['name'], lead.get('lead_name'), email,
                                       lead['open_count'], str(s_dt), str(o_dt), f"{gap_minutes} min"]
                                camp_leads_batch.append(row)
                                existing_emails.add(email)

                if len(leads) < 100: break
                offset += 100
                time.sleep(0.5)

            if camp_leads_batch:
                try:
                    sheet.append_rows(camp_leads_batch)
                    print(f"✅ SAVED: {len(camp_leads_batch)} leads from {camp['name']}")
                    time.sleep(2)
                except Exception as e:
                    print(f"❌ Save Error: {e}")

    print("🎉 Sync Finished.")

if __name__ == "__main__":
    run_live_automation()


from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
import os
import json
from dotenv import load_dotenv
load_dotenv()
def read_google_sheet_with_url (execl_url):
    service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
    if service_account_json:
        service_account_data = json.loads(service_account_json)
    scope = ['https://spreadsheets.google.com/feeds']
    # credentials = ServiceAccountCredentials.from_json_keyfile_name('bisnow-389208-96357e36702b.json', scope)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(service_account_data, scope)

    client = gspread.authorize(credentials)
    working_sheet = client.open_by_url(execl_url)
    # worksheet = spreadsheet.sheet1
    workbook_all_metadata = working_sheet.fetch_sheet_metadata()
    workbook_metadata = workbook_all_metadata['sheets']
    for m in workbook_metadata:
        print(m)
        if m['properties']:
            sheet_index = m['properties']['index']
            sheet_name = m['properties']['title']
            # sys.exit()
            sheet_data = working_sheet.get_worksheet(sheet_index)
            try:
                expected_headers = sheet_data.row_values(1)
                while '' in expected_headers:
                    expected_headers.remove('')
                all_records = sheet_data.get_all_records(expected_headers =expected_headers)
                sheet_dataframe = pd.DataFrame(all_records)
                print(sheet_dataframe)
                return sheet_dataframe
            except Exception as e:
                print(e)

# read_google_sheet_with_url('https://docs.google.com/spreadsheets/d/1XcJ7EKPRJk7YQ2cO1moyzqHQcazsS03_E7-bhkfB7Ac/edit#gid=0')

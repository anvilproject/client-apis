"""Extract AnVIL Data Ingestion Tracker spreadsheet."""

import gspread
from oauth2client.service_account import ServiceAccountCredentials


def download_projects(spreadsheet_key, json_keyfile_path):
    """Read projects from master spreadsheet."""
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(credentials)
    sheet = client.open_by_key(spreadsheet_key)
    index = 0
    try:
        for index, worksheet in enumerate(sheet.worksheets()):
            if 'Dave Using' in worksheet.title:
                break
    except Exception as e:
        raise Exception(f"Cannot read spreadsheet key={spreadsheet_key}") from e
    #  Extract all the rows
    list_of_lists = sheet.get_worksheet(index).get_all_values()
    # table headers
    keys = list_of_lists.pop(0)

    def normalize_value(v):
        """Fix the value."""
        if v == 'None':
            return None
        if v == '':
            return None
        if 'no' == str(v).lower():
            return False
        if 'yes' == str(v).lower():
            return True
        if str(v).isnumeric():
            return int(v)
        return v

    for lst in list_of_lists:
        project = {keys[i]: normalize_value(v) for i, v in enumerate(lst)}
        if project['name']:
            yield {keys[i]: normalize_value(v) for i, v in enumerate(lst)}

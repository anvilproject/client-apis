import xmltodict
from pprint import pprint
import json
from ftplib import FTP

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from collections import defaultdict


def known_projects(spreadsheet_key='165sxLdPMz26iyc0jaXVRYODDLv5-tXMBfJvvLF9Msps', json_keyfile_path='client_secret.json'):
    """Reads projects from master spreadsheet."""
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_key)
    for index, worksheet in enumerate(sheet.worksheets()):
        if worksheet.title == 'Master':
            break
    master_worksheet = sheet.get_worksheet(index)
    # Extract all the rows
    list_of_hashes = master_worksheet.get_all_records()
    # fix the column headers
    keys =  {
        'Project': 'project_name',
        'Sequencing Center': 'sequencing_center',
        'Cohort Name': 'cohort_name',
        'Datatype': 'data_type',
        'Workspace Name': 'workspace_name',
        'Workspace Link': 'workspace_link',
        'Bucket Path': 'bucket_path',
        'phsID (if available)': 'dbgap_id',
        'Gen3 Indexed': 'gen3_indexed',
        'Sample Count': 'sample_count',
        'CRAM Count': 'CRAM_count',
        'CRAI count': 'CRAI_count',
        'md5 Count': 'md5_count',
        'VCF Count': 'VCF_count',
        'gVCF Count': 'gVCF_count',
        'Updated on': 'updated_on',
        'Notes': 'notes',
    }

    def normalize_value(v):
        """Fixes the values."""
        if v == 'None':
            return None
        if v == '':
            return None
        if 'no' == str(v).lower():
            return False
        if 'yes' == str(v).lower():
            return True
        return v

    def normalize(hash):
        """Maps the keys and fixes the values."""
        return {keys[p]:normalize_value(hash[p]) for p in hash}

    return list(map(normalize, list_of_hashes))


def dbgap_projects(projects=known_projects()):
    """Returns a object indexed by dbGapId."""
    dbgap_projects = defaultdict(list)
    for p in [p for p in projects if p['dbgap_id']]:
        dbgap_projects[p['dbgap_id']].append(p['workspace_name'])
    return dbgap_projects

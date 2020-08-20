"""Extract AnVIL Data Ingestion Tracker spreadsheet."""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from collections import OrderedDict
import os

DEFAULT_OUTPUT_PATH = f"{os.path.dirname(__file__)}/../data/data_ingestion_tracker.json"


def download_projects(spreadsheet_key, json_keyfile_path):
    """Read projects from master spreadsheet."""
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(credentials)
    sheet = client.open_by_key(spreadsheet_key)
    for index, worksheet in enumerate(sheet.worksheets()):
        if 'Ingestion' in worksheet.title:
            break
    #  Extract all the rows
    list_of_lists = sheet.get_worksheet(index).get_all_values()
    list_of_lists.pop(0)  # junk
    # table headers
    keys = list_of_lists.pop(0)
    # re-write headers
    headers = OrderedDict({
        '': '',
        'Date AnVIL_Ingestion added as Reader to Workspace': 'date_AnVIL_ingestion_added_as_reader',
        'Date AnVIL_Ingestion added to Auth Domain': 'date_AnVIL_ingestion_added_to_auth_domain',
        'Date Auth Domain Added to Workspace as Reader': 'date_authorization_domain_added_to_workspace',
        'Date Authorization Domain Setup': 'date_authorization_domain_setup',
        'Date Consortium List Added to Auth Domain': 'date_consortium_added_to_auth_domain',
        'Date DUL/Consent Exists': 'date_dul_consent_exists',
        'Date Manifest File Created and Deposited to Box': 'date_manifest_file_deposited',
        'Date Terra Data Tables are Complete ': 'date_terra_data_tables_are_complete ',
        'Date Workspace Description Added': 'date_workspace_description_added',
        'Date dbGaP Linkage to Auth Domain': 'date_dbGaP_linkage_to_auth_domain',
        'Expected Final Number of Samples': 'expected_final_number_of_samples',
        'External Data Source = AnVIL in dbGaP': 'external_data_source',
        'FULL dbGaP Accession or other ACL\n(phs00000.v0.p0.c0)': 'dbGaP_accession',
        'Gen Data Transfer': 'gen_data_transfer',
        'Gen3 Program': 'gen3_program',
        'Gen3 Project': 'gen3_project',
        'INT Google Bucket Access Group': 'INT_google_bucket_access_group',
        'INT Service Account': 'INT_service_account',
        'Indexing Status': 'indexing_status',
        'Latest released version consent codes': 'latest_released_version_consent_codes',
        'Metric File Exists in Bucket': 'metric_file_exists_in_bucket',
        'Metrics Merged into Terra Data Table': 'metrics_merged_into_terra_data_table',
        'Minimal Metadata Transfer': 'minimal_metadata_transfer',
        'Model Build': 'model_build',
        'Model Definition': 'model_definition',
        'N Samples in Dataset Attributes': 'n_samples_in_dataset_attributes',
        'Name of Authorization Domain': 'name_of_authorization_domain',
        'PROD Google Bucket Access Group': 'prod_google_bucket_access_group',
        'PROD Service Account': 'prod_service_account',
        'Pheno Data Transfer': 'pheno_data_transfer',
        'Pheno QC': 'pheno_qc',
        'Pheno. Submission': 'pheno_submission',
        'Project': 'project',
        'Proposed Consent Code': 'proposed_consent_code',
        'Proposed Gen3 Project Name': 'proposed_gen3_project_name',
        'QC': 'QC',
        'QC Metadata FIle': 'qc_metadata_file',
        'Ready to Index': 'ready_to_index',
        'Relevant Consortium List': 'relevant_consortium_list',
        'Reprocessing Complete': 'reprocessing_complete',
        'Reprocessing Needed?': 'reprocessing_needed',
        'Run QA WDL': 'run_qa_wdl',
        'STAGING Google Bucket Access Group': 'staging_google_bucket_access_group',
        'Sequencing Center': 'sequencing_center',
        'Sequencing Strategy (WGS/WES/Array/Multiple)': 'sequencing_strategy',
        'Service Account': 'service_account',
        'Should be displayed on Portal': 'display_on_portal',
        'Summary Statistics Loaded to Portal?': 'summary_statistics_loaded_to_portal',
        'Terra Bucket': 'terra_bucket',
        'Workspace Link': 'workspace_link',
        'Workspace Name': 'workspace_name',
        'dbGaP Linking Auth Group': 'dbGaP Linking Auth Group',
        'dbGaP Release Status (Released, In Progress, Not Begun, Not Intending To)': 'dbGaP_release_status',
        'simple Accession\n(or "Pending")': 'simple_accession',
        'AnVIL_Discovery added to Workspace as Reader': 'AnVIL_Discovery_added_to_workspace',
        'BSB': 'BSB',
        'Consent Category and Research Use Limitations': 'consent_category',
        'Contact Email': 'contact_email',
        'Contact Person': 'contact_person',
        'Current Samples in Workspace (Data)': 'current_samples_in_workspace',
        'Data Tables Updates with Reprocessed Files': 'data_tables_updated_with_reprocessed_files',
        'Data Transformation': 'data_transformation',
        'Data Use Codes': 'data_use_codes',
        'Date AnVIL_Devs added to Auth Domain': 'date_anvil_devs_added_to_auth_domain'
    })

    keys = [headers.get(k, k) for k in keys]

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
        if project['workspace_name']:
            yield {keys[i]: normalize_value(v) for i, v in enumerate(lst)}

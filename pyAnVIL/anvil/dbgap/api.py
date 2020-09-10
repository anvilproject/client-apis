"""Query dbGap for study information."""

import logging
import requests
import xmltodict
import json
import os
from anvil.util.cache import memoize

logger = logging.getLogger('anvil.dbgap.api')

url_base = 'https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id='

# sync with setup.py utility data_ingestion_tracker
DEFAULT_OUTPUT_PATH = f"{os.path.dirname(__file__)}/../data/data_ingestion_tracker.json"


@memoize
def get_study(accession):
    """Return tuple (qualified_accession, schema)."""
    qualified_accession = None
    try:
        r = requests.get(f"{url_base}{accession}", allow_redirects=False)
        assert r.status_code == 302, r.status_code
        qualified_accession = r.headers['location'].split('=')[1]
        assert len(qualified_accession) > 0, f"No qualified study for {accession}"
        r = requests.get(f"https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetSampleStatus.cgi?study_id={qualified_accession}&rettype=xml")
        assert r.status_code == 200, f"status_code: {r.status_code} text:{r.text}"
        d = xmltodict.parse(r.text)
        return ((qualified_accession, d))
    except Exception as e:
        logger.warn(f"{accession}/{qualified_accession} error: {e}")
        return None


@memoize
def get_projects():
    """Open embedded extract from google spreadsheet, indexed by workspace."""
    with open(DEFAULT_OUTPUT_PATH, 'r') as ins:
        return {s['workspace_name']: s for s in json.load(ins)}


def get_accession(namespace, project_id):
    """Return simple_accession (dbGap phsid)."""
    projects = get_projects()
    key = f"{namespace}/{project_id}"
    if key not in projects:
        return None
    simple_accession = projects[key]['simple_accession']
    if not projects[key]['simple_accession']:
        logger.warning(f"{project_id} missing dbGap accession")
        return None
    if not projects[key]['simple_accession'].startswith('phs'):
        logger.warning(f"{project_id} invalid dbGap {simple_accession}")
        return None
    return simple_accession

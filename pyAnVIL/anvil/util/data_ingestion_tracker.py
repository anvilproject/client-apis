#!/usr/bin/env python3

"""Extract AnVIL Data Ingestion Tracker spreadsheet."""

import json
import logging
import requests
from io import StringIO
import csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

URL = 'https://raw.githubusercontent.com/anvilproject/anvil-portal/main/plugins/utils/dashboard-source-anvil.tsv'


def download_projects():
    """Read projects from master spreadsheet. Call `projects = [project for project in download_projects()]`"""

    response = requests.get(URL)
    response.raise_for_status()

    list_of_lists = []
    with StringIO(response.text) as fd:
        rd = csv.reader(fd, delimiter="\t", quotechar='"')
        for row in rd:
            list_of_lists.append(row)

    logger.debug(f"fetched {len(list_of_lists)} rows")

    # table headers
    keys = list_of_lists.pop(0)

    def normalize_value(v):
        """Fix the value."""
        if isinstance(v, str):
            v = v.strip()
        if v in ['None', '', 'NA', '--', 'Unspecified', None]:
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


def data_ingestion_tracker(output_path):
    """Read spreadsheet, write to json file."""
    abbreviated_projects = [
        {'workspace_name': p['name'],
         'study_accession': p['phsId'],
         'dataUseRestriction': p['library:dataUseRestriction'],
         'indication': p['library:indication']} for p in list(download_projects())]
    with open(output_path, 'w') as fp:
        json.dump(abbreviated_projects, fp)
    logger.info(f"Read {len(abbreviated_projects)} projects from {URL}. Wrote to {output_path}")



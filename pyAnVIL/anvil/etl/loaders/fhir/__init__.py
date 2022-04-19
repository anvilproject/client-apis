import json
import os
from collections import defaultdict
from anvil.etl.utilities.shell_helper import run_cmd


def _extract_workspace_mapping(output_path):
    """Read ResearchStudy identifiers to retrieve the consortium, data_store and id."""
    list_script = f"""
        find {output_path} -name ResearchStudy.ndjson -print
    """
    mapping = {'consortium': defaultdict(list), 'data_store': defaultdict(list), 'workspace_name': defaultdict(list)}
    for path in [line for line in run_cmd(list_script).split('\n')]:
        research_studies = []
        with open(path) as input_stream:
            research_studies.append(json.load(input_stream))
        for research_study in research_studies:
            identifiers = research_study['identifier']
            consortium = next(iter([identifier['value'] for identifier in identifiers if identifier['system'] == "https://anvil.terra.bio/#consortium"]), None)
            workspace = next(iter([identifier['value'] for identifier in identifiers if identifier['system'] == "https://anvil.terra.bio/#workspaces/anvil-datastorage/"]), None)
            data_store = next(iter([identifier['value'] for identifier in identifiers if identifier['system'] == "https://anvil.terra.bio/#FHIR/data-store"]), None)
            _id = research_study['id']
            obj = {'name': workspace, 'id': _id, 'consortium': consortium, 'data_store': data_store}
            mapping['consortium'][consortium].append(obj)
            mapping['data_store'][data_store].append(obj)
            mapping['workspace_name'][workspace].append(obj)
    return mapping


def _log_console_link(logger):
    """Log a message with a link to the GCP console"""
    project = os.environ['GOOGLE_PROJECT']
    location = os.environ['GOOGLE_LOCATION']
    data_set = os.environ['GOOGLE_DATASET']
    logger.info(
        f"Data loading, check GCP console for progress. https://console.cloud.google.com/healthcare/browser/locations/{location}/datasets/{data_set}/operations?project={project}")

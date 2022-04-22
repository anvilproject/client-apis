import subprocess
from collections import namedtuple
import os
import logging
logger = logging.getLogger(__name__)

PROPERTIES_LIST = """
    GOOGLE_PROJECT_NAME 
    GOOGLE_LOCATION 
    GOOGLE_PROJECT 
    GOOGLE_DATASET 
    TOKEN 
    GOOGLE_DATASTORES 
    GOOGLE_DATASTORE 
    GOOGLE_BUCKET 
    OUTPUT_PATH 
    IMPLEMENTATION_GUIDE_PATH""".strip().split()


def run_cmd(command_line):
    """Run a command line, return stdout."""
    try:
        logger.debug(command_line)
        return subprocess.check_output(command_line, shell=True).decode("utf-8").rstrip()
    except Exception as e:
        logger.error(e)
        raise e


def ensure_env_variables():
    """Fill in environment variables, ensure FHIR context."""

    variables = {property_: os.environ.get(property_) for property_ in PROPERTIES_LIST if os.environ.get(property_, None)}

    variables["GOOGLE_PROJECT_NAME"] = variables.get("GOOGLE_PROJECT_NAME", 'fhir-test-16')
    variables["GOOGLE_LOCATION"] = variables.get("GOOGLE_LOCATION", 'us-west2')
    variables["GOOGLE_DATASET"] = variables.get("GOOGLE_DATASET", 'anvil-test')
    variables["GOOGLE_DATASTORE"] = variables.get("GOOGLE_DATASTORE", 'public')
    variables["OUTPUT_PATH"] = variables.get("OUTPUT_PATH", './DATA')
    variables["IMPLEMENTATION_GUIDE_PATH"] = variables.get("IMPLEMENTATION_GUIDE_PATH", f'{variables["OUTPUT_PATH"]}/fhir/IG')

    if not variables.get('GOOGLE_PROJECT', None):
        variables['GOOGLE_PROJECT'] = run_cmd(f'gcloud projects list --filter=name={variables["GOOGLE_PROJECT_NAME"]} --format="value(projectId)"')

    if not variables.get("GOOGLE_BUCKET", None):
        variables["GOOGLE_BUCKET"] = variables['GOOGLE_PROJECT']

    for k in ["GOOGLE_DATASET", "GOOGLE_LOCATION", 'GOOGLE_PROJECT', "GOOGLE_BUCKET"]:
        assert variables[k], f"Should have set variable {k}"

    if not variables.get('GOOGLE_DATASTORES', None):
        variables['GOOGLE_DATASTORES'] = run_cmd(f'gcloud beta healthcare fhir-stores list --dataset={variables["GOOGLE_DATASET"]}  --location={variables["GOOGLE_LOCATION"]}  --format="table[no-heading](ID)"')
    if isinstance(variables['GOOGLE_DATASTORES'], str):
        variables['GOOGLE_DATASTORES'] = variables['GOOGLE_DATASTORES'].split()

    if not variables.get('GOOGLE_BUCKET', None):
        variables['GOOGLE_BUCKET'] = variables['GOOGLE_PROJECT']

    if not variables.get('TOKEN', None):
        variables['TOKEN'] = run_cmd(f'gcloud auth print-access-token')

    for k in ["TOKEN", "GOOGLE_BUCKET", 'GOOGLE_DATASTORES']:
        assert variables[k], f"Should have set variable {k}"

    ETLVariables = namedtuple('ETLVariables', PROPERTIES_LIST)
    return ETLVariables(**variables)



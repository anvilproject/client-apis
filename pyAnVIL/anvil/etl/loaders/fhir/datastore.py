import json
import os

import click
import logging

import requests

from anvil.etl.loaders.fhir import _extract_workspace_mapping, _log_console_link
from anvil.etl.utilities.shell_helper import run_cmd

logger = logging.getLogger(__name__)


@click.group(name='data-store')
@click.pass_context
def data_store_cli(ctx):
    pass  # dummy


@data_store_cli.command(name='ls')
@click.option('--consortium', default=None, help='Filter, only this consortium.')
@click.option('--workspace', default=None, help='Filter, only this workspace.')
@click.pass_context
def _data_stores(ctx, consortium, workspace):
    """Print transformed ResearchStudy data-store info to stdout."""
    workspace_mapping = _extract_workspace_mapping(ctx.obj['output_path'])
    print(json.dumps(workspace_mapping['data_store']))


@data_store_cli.command(name='create')
@click.pass_context
def create_data_stores(ctx, token=None):
    """Create data_stores, enable IG."""
    # TODO - filter consortium, workspace, data-store
    output_path = ctx.obj["output_path"]
    workspace_mapping = _extract_workspace_mapping(output_path)
    list_cmd = f'gcloud beta healthcare fhir-stores list --dataset=$GOOGLE_DATASET  --location=$GOOGLE_LOCATION  --format="table[no-heading](ID)"'
    data_store_list = run_cmd(list_cmd)
    cmds = [f"gcloud beta healthcare fhir-stores create {data_store} --dataset=$GOOGLE_DATASET --location=$GOOGLE_LOCATION --version=R4 --enable-update-create" for data_store in workspace_mapping['data_store'].keys() if data_store not in data_store_list]
    if len(cmds) > 0:
        run_cmd("\n".join(cmds))
    data_store_list = run_cmd(list_cmd)
    error = False
    for data_store in workspace_mapping['data_store']:
        if data_store not in data_store_list:
            logger.error(f"{data_store} not found in:\n{data_store_list}")
            error = True
    if not error:
        logger.info("All data stores exist. Loading IG...")
    cmds = [f"gcloud beta healthcare fhir-stores import gcs  {data_store}  --dataset=$GOOGLE_DATASET  --location=$GOOGLE_LOCATION --gcs-uri=gs://$GOOGLE_BUCKET/IG/*.json --content-structure=resource-pretty" for data_store in workspace_mapping['data_store'].keys() if data_store in data_store_list]
    if len(cmds) > 0:
        print(run_cmd("\n".join(cmds)))
    logger.info("IG loaded on all. Enabling IG...")
    if not token:
        token = run_cmd("gcloud auth application-default print-access-token")
    for data_store in workspace_mapping['data_store'].keys():
        if data_store in data_store_list:
            _enable_ig(ctx, data_store, token)


@data_store_cli.command(name='upload')
@click.option('--consortium', default=None, help='Filter, only this consortium.')
@click.option('--workspace', default=None, help='Filter, only this workspace')
@click.option('--drop', default=True, help='Delete FHIR daa in bucket first')
@click.pass_context
def upload(ctx, consortium, workspace, drop):
    """Copy FHIR resources to bucket."""
    output_path = ctx.obj["output_path"]
    workspace_consortium_mapping = _extract_workspace_mapping(output_path)['consortium']
    copy_commands = []
    check_commands = []
    uploaded_workspaces = []
    for consortium_name, workspaces in workspace_consortium_mapping.items():
        for mapping in workspaces:
            if consortium and consortium != consortium_name:
                continue
            if workspace and workspace != mapping['name']:
                continue
            assert 'name' in mapping, mapping
            dir_name = f"{output_path}/fhir/{consortium_name}/{mapping['name']}"
            obj_name = f"fhir/{consortium_name}/{mapping['name']}"
            assert os.path.isdir(dir_name), f"{dir_name} does not exist?"
            # gzip, recursive, no clobber
            if drop:
                copy_commands.append(f'gsutil -m rm -r gs://{os.environ["GOOGLE_BUCKET"]}/{obj_name}/public')
                copy_commands.append(f'gsutil -m rm -r gs://{os.environ["GOOGLE_BUCKET"]}/{obj_name}/protected')
            copy_commands.append(f'gsutil -m cp -J -r -n  {dir_name}/public/** gs://{os.environ["GOOGLE_BUCKET"]}/{obj_name}/public')
            copy_commands.append(f'gsutil -m cp -J -r -n  {dir_name}/protected/** gs://{os.environ["GOOGLE_BUCKET"]}/{obj_name}/protected')
            check_commands.append(f'gsutil ls gs://{os.environ["GOOGLE_BUCKET"]}/{obj_name}')
            uploaded_workspaces.append((consortium_name, mapping['name']))
    run_cmd('\n'.join(copy_commands))
    results = run_cmd('\n'.join(check_commands))
    for consortium_name, workspace_name in uploaded_workspaces:
        assert f"{consortium_name}/{workspace_name}/public" in results, ('missing.obj', f"{consortium_name}/{workspace_name}/public", results)
        assert f"{consortium_name}/{workspace_name}/protected" in results, ('missing.obj', f"{consortium_name}/{workspace_name}/protected", results)


@data_store_cli.command(name='load')
@click.pass_context
def load_data_stores(ctx):
    """Load data from bucket into data stores."""
    output_path = ctx.obj["output_path"]
    workplace_mapping = _extract_workspace_mapping(output_path)
    workspace_datastore_mapping = workplace_mapping['data_store']

    cmds = []
    for data_store, workspaces in workspace_datastore_mapping.items():
        for mapping in workspaces:
            object_path = f"fhir/{mapping['consortium']}/{mapping['name']}"
            for subdir in ['public', 'protected']:
                cmds.append(f"gcloud healthcare fhir-stores import gcs {data_store} --location=$GOOGLE_LOCATION --dataset=$GOOGLE_DATASET --content-structure=resource --async --gcs-uri=gs://$GOOGLE_BUCKET/{object_path}/{subdir}/*.ndjson")
    run_cmd("\n".join(cmds))
    _log_console_link(logger)


@data_store_cli.command(name='delete')
@click.pass_context
def delete(ctx):
    """Delete data stores."""
    # TODO - filter consortium, workspace, data-store
    output_path = ctx.obj["output_path"]
    workplace_mapping = _extract_workspace_mapping(output_path)
    list_cmd = f'gcloud beta healthcare fhir-stores list --dataset=$GOOGLE_DATASET  --location=$GOOGLE_LOCATION  --format="table[no-heading](ID)"'
    data_store_list = run_cmd(list_cmd)
    cmds = [f"gcloud beta healthcare fhir-stores delete {data_store} --dataset=$GOOGLE_DATASET --location=$GOOGLE_LOCATION --quiet" for data_store in workplace_mapping['data_store'].keys() if data_store in data_store_list]
    if len(cmds) > 0:
        run_cmd("\n".join(cmds))
    data_store_list = run_cmd(list_cmd)
    for data_store in workplace_mapping['data_store']:
        if data_store in data_store_list:
            logger.error(f"{data_store} delete failed, found in:\n{data_store_list}")
    logger.info("All data stores deleted.")


def _enable_ig(ctx, data_store, token):
    """Enable IG in datastores"""
    output_path = ctx.obj["output_path"]
    workspace_mapping = _extract_workspace_mapping(output_path)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    project = os.environ['GOOGLE_PROJECT']
    location = os.environ['GOOGLE_LOCATION']
    data_set = os.environ['GOOGLE_DATASET']
    url = f"https://healthcare.googleapis.com/v1beta1/projects/{project}/locations/{location}/datasets/{data_set}/fhirStores/{data_store}?updateMask=validationConfig"
    response = requests.patch(
        url=url,
        headers=headers,
        json={
            "validationConfig": {
                "enabledImplementationGuides": ["https://ncpi-fhir.github.io/ncpi-fhir-ig/ImplementationGuide/NCPI-FHIR-Implementation-Guide"]
            }
        }
    )
    if response.status_code == 200:
        logger.info(f"IG enabled on {data_store}")
    else:
        logger.info(("could.not.enable.ig", data_store, response.status_code, response.text))

import json
import os

import click
import logging

import requests

from anvil.etl.loaders.fhir import _extract_workspace_mapping, _log_console_link
from anvil.etl.loaders.fhir.implementation_guide import _enable_ig, _get_ig
from anvil.etl.transformers.fhir_writer import ensure_data_store_name
from anvil.etl.utilities import chunker
from anvil.etl.utilities.shell_helper import run_cmd

logger = logging.getLogger(__name__)


@click.group(name='data-store')
@click.pass_context
def data_store_cli(ctx):
    pass  # dummy


@data_store_cli.command(name='studies')
@click.option('--consortium', default=None, help='Filter, only this consortium.')
@click.option('--workspace', default=None, help='Filter, only this workspace.')
@click.pass_context
def _research_studies(ctx, consortium, workspace):
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
    cmds = [f"gcloud beta healthcare fhir-stores import gcs  {data_store}  --dataset=$GOOGLE_DATASET  --location=$GOOGLE_LOCATION --gcs-uri=gs://$GOOGLE_BUCKET/fhir/IG/*.json --content-structure=resource-pretty --async" for data_store in workspace_mapping['data_store'].keys() if data_store in data_store_list]
    if len(cmds) > 0:
        cmd_results = run_cmd("\n".join(cmds))
        while True:
            logger.info(f"Waiting for import to complete...")
            check_operations = json.loads(run_cmd('sleep 5 ; gcloud beta healthcare operations list --dataset $GOOGLE_DATASET --location $GOOGLE_LOCATION   --format \'json(name)\'   --filter="done:false"'))
            if len(check_operations) == 0:
                break

    logger.info("IG loaded on all. Enabling IG...")
    if not token:
        token = run_cmd("gcloud auth application-default print-access-token")
    for data_store in workspace_mapping['data_store'].keys():
        if data_store in data_store_list:
            _enable_ig(ctx, data_store, token)


@data_store_cli.command(name='upload')
@click.option('--consortium', default=None, help='Filter, only this consortium.', show_default=True)
@click.option('--workspace', default=None, help='Filter, only this workspace', show_default=True)
@click.option('--drop', default=False, help='Delete FHIR daa in bucket first', show_default=True, is_flag=True)
@click.option('--check', default=False, help='Delete FHIR daa in bucket first', show_default=True, is_flag=True)
@click.option('--go_fast', default=True, help='Copy entire fhir/ sdir in one go.', show_default=True, is_flag=True)
@click.pass_context
def upload(ctx, consortium, workspace, drop, check, go_fast):
    """Copy FHIR resources from local disk to bucket."""
    output_path = ctx.obj["output_path"]
    workspace_consortium_mapping = _extract_workspace_mapping(output_path)['consortium']
    uploaded_workspaces = []
    for consortium_name, workspaces in workspace_consortium_mapping.items():
        for mapping in workspaces:
            if consortium and consortium != consortium_name:
                continue
            if workspace and workspace != mapping['name']:
                continue
            data_store = mapping['data_store']
            dir_name = f"{output_path}/fhir/{data_store}/{consortium_name}/{mapping['name']}"
            assert os.path.isdir(dir_name), f"{dir_name} does not exist?"
            uploaded_workspaces.append(f"fhir/{data_store}/{consortium_name}/{mapping['name']}")

    # for group in chunker(copy_commands, 20):
    #     run_cmd('\n'.join(group))
    # for group in chunker(check_commands, 20):
    #     results = run_cmd('\n'.join(group))

    if drop:
        for dir_name in uploaded_workspaces:
            run_cmd(f"gsutil rm -r gs://$GOOGLE_BUCKET/{dir_name}")

    if go_fast:
        run_cmd(f"gsutil -m cp -J -r {output_path}/fhir/ gs://$GOOGLE_BUCKET")
    else:
        for group in chunker(uploaded_workspaces, 20):
            cmds = [f"gsutil -m cp -J -r {output_path}/{dir_name}/ gs://$GOOGLE_BUCKET" for dir_name in group]
            run_cmd('\n'.join(cmds))

    if check:
        results = run_cmd(f"gsutil ls -r gs://$GOOGLE_BUCKET/fhir")
        for dir_name in uploaded_workspaces:
            assert dir_name in results, ('missing.obj', dir_name, results)


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
            object_path = f"fhir/{mapping['data_store']}/{mapping['consortium']}/{mapping['name']}"
            cmds.append(
                f"gcloud healthcare fhir-stores import gcs {data_store} --location=$GOOGLE_LOCATION --dataset=$GOOGLE_DATASET --content-structure=resource --async --gcs-uri=gs://$GOOGLE_BUCKET/{object_path}/**.ndjson"
            )
            # for subdir in ['public', 'protected']:
            #     cmds.append(f"gcloud healthcare fhir-stores import gcs {data_store} --location=$GOOGLE_LOCATION --dataset=$GOOGLE_DATASET --content-structure=resource --async --gcs-uri=gs://$GOOGLE_BUCKET/{object_path}/{subdir}/*.ndjson"
    for group in chunker(cmds, 20):
        run_cmd('\n'.join(group))
    _log_console_link(logger)


@data_store_cli.command(name='load-public')
@click.pass_context
def load_public_data_stores(ctx):
    """Load public resources from bucket into 'public' data store."""
    run_cmd("gcloud healthcare fhir-stores import gcs public --location=$GOOGLE_LOCATION --dataset=$GOOGLE_DATASET --content-structure=resource --async --gcs-uri=gs://$GOOGLE_BUCKET/fhir/*/*/*/public/*.ndjson")
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


@data_store_cli.command(name='ls')
@click.option('--consortium', default=None, help='Filter, only this consortium.')
@click.option('--workspace', default=None, help='Filter, only this workspace.')
@click.option('--token',
              default=None,
              envvar='TOKEN',
              help='gcloud access token. If missing cmd  `gcloud auth application-default print-access-token`',
              show_default=True)
@click.pass_context
def _data_stores(ctx, consortium, workspace, token):
    """Print server's data-stores info to stdout."""

    if not token:
        token = run_cmd("gcloud auth application-default print-access-token")

    list_cmd = f'gcloud beta healthcare fhir-stores list --dataset=$GOOGLE_DATASET  --location=$GOOGLE_LOCATION  --format="table[no-heading](ID)"'
    data_store_list = run_cmd(list_cmd)

    for data_store in data_store_list.split('\n'):
        print(json.dumps(_get_ig(ctx, data_store, token)))


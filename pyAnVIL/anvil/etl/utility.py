import os

import click
import logging

import numpy as np
import pandas as pd
import yaml
import json

from tabulate import tabulate

from anvil.etl.extractors.gen3 import DRSReader
from anvil.etl.transform import _consortium_from_workspace
from anvil.etl.transformers import _recursive_default_dict
from anvil.etl.transformers.normalizer import ontologies, normalize, fetch_workspace_names
from anvil.etl.utilities.shell_helper import ensure_env_variables

logger = logging.getLogger(__name__)


@click.group(name='utility')
@click.pass_context
def utility(ctx):
    """Utility commands."""
    # Set log level in our & dependencies loggers
    logger.setLevel(ctx.obj['log_level'])


@utility.command(name='env')
@click.pass_context
def env(ctx):
    """Load env variables. source /dev/stdin <<< `anvil_etl utility env` ; env | grep GOOGLE """
    for k, v in ensure_env_variables()._asdict().items():
        if k == 'GOOGLE_DATASTORES':
            v = ','.join(v)
        print(f"export {k}={v}")


@utility.command(name='config')
@click.option('--format', 'format_', type=click.Choice(['json', 'yaml'],case_sensitive=False), default='json', show_default=True)
@click.pass_context
def _config(ctx, format_):
    """Print config to stdout."""
    if format_ == 'yaml':
        print(yaml.dump(ctx.obj['config']))
    else:
        print(json.dumps(ctx.obj['config']))


@utility.command(name='errors')
@click.option('--workspace', help='Filter, only this workspace.')
@click.pass_context
def errors(ctx, workspace):
    """Dump errors."""
    _consortium_name, workspace = next(iter(normalize(output_path=ctx.obj['output_path'], requested_consortium_name=None, workspace_name=workspace, validate_buckets=False, config=ctx.obj['config'])), None)
    print(json.dumps(workspace.errors))


@utility.command(name='qa')
@click.option('--consortium', default=None, help='Filter, only this consortium.')
@click.option('--workspace', default=None, help='Filter, only this workspace.')
@click.option('--details', default=False, help='Include error details.', show_default=True, is_flag=True)
@click.pass_context
def qa(ctx, consortium, workspace, details):
    """Report on analysis."""
    summaries = []
    with open(f"{ctx.obj['output_path']}/analysis.ndjson") as input_stream:
        for line in input_stream.readlines():
            summary = json.loads(line)
            o = {
                'consortium': summary['consortium'],
                'workspace': summary['workspace'],
            }
            for k in ['patients', 'specimens', 'tasks', 'documents']:
                o[k] = summary['nodes'][k]
            file_count = 0
            drs_count = 0
            for k in summary['files']:
                o[k] = summary['files'][k]['count']
                drs_count += summary['files'][k]['drs_count']
                file_count += summary['files'][k]['count']
            if o['specimens'] == 0:
                o['qa_grade'] = 0
            else:
                o['qa_grade'] = 100 - (summary['error_count'] / o['specimens'])

            if drs_count == 0:
                o['drs_grade'] = 0
            else:
                o['drs_grade'] = 100 - (drs_count / file_count)
            summaries.append(o)
    df = pd.DataFrame(summaries).replace({np.nan: None})
    print(tabulate(df, headers='keys', tablefmt='github'))


@utility.command(name='backup')
@click.option('--source',
              type=click.Choice(['FHIR', 'DB'], case_sensitive=False),
              help="Copy FHIR directories or working databases to bucket"
              )
@click.option('--bucket_name',
              help="Google bucket to write to."
              )
@click.pass_context
def _backup(ctx, source, bucket_name):
    """Backup files to terra workspace bucket."""
    logger.info(f"TODO: {ctx.command.name} copy from {ctx.obj['output_path']} to bucket")


@utility.group(name='data')
@click.pass_context
def data(ctx):
    """Data utility commands."""
    pass


@data.command(name='ontologies')
@click.option('--consortium', default=None, help='Filter, only this consortium.')
@click.option('--workspace', default=None, help='Filter, only this workspace.')
@click.option('--details', default=False, help='Include error details.', show_default=True, is_flag=True)
@click.pass_context
def _ontologies(ctx, consortium, workspace, details):
    """Discover ontologies fields write to stdout."""
    G = _recursive_default_dict()
    for ontology_fields in ontologies(ctx.obj['output_path'], consortium, workspace, details):
        for ontology_field in ontology_fields:
            G[ontology_field['consortium_name']][ontology_field['workspace_name']][ontology_field['entity_name']] = ontology_field['ontology_fields']
    print(json.dumps(G))


@data.command(name='sample')
@click.option('--consortium', default=None, help='Filter, only this consortium.')
@click.option('--workspace', default=None, help='Filter, only this workspace.')
@click.option('--depth', default=10, help='Number of patients.', show_default=True)
@click.pass_context
def _sample(ctx, consortium, workspace, depth):
    """Export source data for <depth> patients to file system."""
    if workspace:
        if not consortium:
            consortium = _consortium_from_workspace(ctx.obj['config'], workspace)
        workspace_names = [(consortium, workspace)]
    else:
        workspace_names = fetch_workspace_names(ctx.obj['output_path'], requested_consortium_name=consortium,
                                                workspace_name=workspace)

    dir_name = f"{ctx.obj['output_path']}/sample"

    drs_reader = DRSReader(ctx.obj['output_path'])

    def _generate_samples(_consortium_name, _workspace_name):
        for _consortium_name, _workspace in normalize(
                ctx.obj['output_path'],
                requested_consortium_name=_consortium_name,
                workspace_name=_workspace_name,
                validate_buckets=False,
                config=ctx.obj['config']
        ):
            yield {'entityType': 'schema', 'attributes':_workspace.children['schema'][0]}
            for index, patient_id in zip(range(depth), _workspace.patients):
                patient = _workspace.patients[patient_id]
                yield patient

                if 'family' in patient:
                    yield patient['family']

                for specimen in patient.get('specimens', []):
                    yield specimen
                    for task in specimen.get('tasks', []):
                        if task['entityType'] != 'Task':
                            #  don't yield _implied tasks
                            yield task
                        for source in task['outputs']:
                            for field in task['outputs'][source]:
                                # remove the drs_uri
                                if 'drs_uri' in task['outputs'][source][field]:
                                    del task['outputs'][source][field]['drs_uri']
                                yield {
                                    'entityType': 'blob',
                                    'attributes': task['outputs'][source][field]
                                }
                                drs = drs_reader.get(task['outputs'][source][field]['url'].split('/')[-1])
                                if drs:
                                    yield {
                                        'entityType': 'drs',
                                        'attributes': drs
                                    }

    for consortium_name, workspace_name in workspace_names:

        emitters = {}

        for entity in _generate_samples(consortium_name, workspace_name):
            subdir = f"{dir_name}/{consortium_name}/{workspace_name}"
            file_path = f"{subdir}/{entity['entityType']}.ndjson"

            emitter = emitters.get(file_path, None)
            if emitter is None:
                os.makedirs(subdir, exist_ok=True)
                emitter = open(file_path, "w")
                logger.info(f"Writing {file_path}")
                emitters[file_path] = emitter

            if entity['entityType'] not in ['blob', 'drs', 'schema']:
                json.dump({'attributes': entity['attributes'],
                           'name': entity['name'],
                           'entityType': entity['entityType']},
                          emitter,
                          separators=(',', ':'),
                          )
            else:
                json.dump(entity['attributes'],
                          emitter,
                          separators=(',', ':'),
                          )

            emitter.write('\n')

        for emitter in emitters.values():
            emitter.close()

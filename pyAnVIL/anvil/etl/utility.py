import click
import logging
import yaml
import json

from anvil.etl.transformers import _recursive_default_dict
from anvil.etl.transformers.normalizer import ontologies

logger = logging.getLogger(__name__)


@click.group(name='utility')
@click.pass_context
def utility(ctx):
    """Utility commands."""
    # Set log level in our & dependencies loggers
    logger.setLevel(ctx.obj['log_level'])


@utility.command(name='config')
@click.option('--format', type=click.Choice(['json', 'yaml'],case_sensitive=False), default='json', show_default=True)
@click.pass_context
def _config(ctx, format):
    """Print config to stdout."""
    if format == 'yaml':
        print(yaml.dump(ctx.obj['config']))
    else:
        print(json.dumps(ctx.obj['config']))


@utility.command(name='ontologies')
@click.option('--consortium', default=None, help='Filter, only this consortium.')
@click.option('--workspace', default=None, help='Filter, only this workspace.')
@click.option('--details', default=False, help='Include error details.', show_default=True, is_flag=True)
@click.pass_context
def _ontologies(ctx, consortium, workspace, details):
    """Find ontologies fields in workspace and write summary to stdout."""
    G = _recursive_default_dict()
    for ontology_fields in ontologies(ctx.obj['output_path'], consortium, workspace, details):
        for ontology_field in ontology_fields:
            G[ontology_field['consortium_name']][ontology_field['workspace_name']][ontology_field['entity_name']] = ontology_fields['ontology_fields']
    print(json.dumps(G))


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
            summaries.append(json.loads(line))
    logger.warning("TODO - write to dataframe.")


@utility.command(name='bucket')
@click.option('--source',
              type=click.Choice(['FHIR', 'DB'], case_sensitive=False),
              help="Copy FHIR directories or working databases to bucket"
              )
@click.option('--bucket_name',
              help="Google bucket to write to."
              )
@click.pass_context
def _bucket(ctx, source, bucket_name):
    """Backup files to bucket."""
    logger.info(f"TODO: {ctx.command.name} copy from {ctx.obj['output_path']} to bucket")

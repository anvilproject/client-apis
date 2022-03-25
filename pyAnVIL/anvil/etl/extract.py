import click
import logging


from anvil.etl import DEFAULT_GEN3_CREDENTIALS_PATH, DEFAULT_GOOGLE_PROJECT

from extractors.data_ingestion_tracker import write as write_projects
from extractors.gen3 import drs_extractor
from extractors.google import extract_buckets

logger = logging.getLogger(__name__)

@click.group(name='extract')
@click.pass_context
def extract(ctx):
    """ETL meta data from sources."""
    assert ctx.obj['output_path'], 'output_path not set.'

@extract.command('terra')
@click.pass_context
def _terra(ctx):
    """Extract terra workspaces, write to db."""
    logger.info(f"TODO: {ctx.command.name} write to {ctx.obj['output_path']}")


@extract.command('google')
@click.option('--user_project', default=DEFAULT_GOOGLE_PROJECT, help="AnVIL buckets use the `Requester Pays` feature. Please include a billing project. Defaults to GOOGLE_PROJECT")
@click.pass_context
def _google(ctx, user_project):
    """Extract bucket data, write to db."""
    extract_buckets(ctx.obj['output_path'], user_project)


@extract.command('spreadsheet')
@click.pass_context
def _spreadsheet(ctx):
    """Extract data_ingestion_tracker spreadsheet from anvil portal repo."""
    write_projects(ctx.obj['output_path'])


@extract.command('gen3')
@click.option('--gen3_credentials_path', default=DEFAULT_GEN3_CREDENTIALS_PATH, help=f'gen3 native credentials.', show_default=True )
@click.option('--use_terra_credentials', is_flag=True, default=True, help='Running in terra VM, use terra authenticator to access gen3.', show_default=True )
@click.option('--expected_row_count', default=175000, help="Minimum number of file records expected.", show_default=True )
@click.pass_context
def _gen3(ctx, gen3_credentials_path, use_terra_credentials, expected_row_count):
    """Extract meta data from gen3, including drs identifiers."""
    drs_extractor(output_path=ctx.obj['output_path'], expected_row_count=expected_row_count, gen3_credentials_path=gen3_credentials_path, use_terra_credentials=use_terra_credentials)


@extract.command('clean')
@click.pass_context
def _clean(ctx):
    """Drop all databases and downloads."""
    logger.info(f"TODO: {ctx.command.name} remove files from {ctx.obj['output_path']}")

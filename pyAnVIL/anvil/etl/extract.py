import click
import logging


from anvil.etl import DEFAULT_GEN3_CREDENTIALS_PATH, DEFAULT_GOOGLE_PROJECT

from anvil.etl.extractors.data_ingestion_tracker import write as write_projects
from anvil.etl.extractors.gen3 import drs_extractor
from anvil.etl.extractors.google import extract_buckets
from anvil.etl.extractors.terra import cli as terra_cli, logger as terra_logger


logger = logging.getLogger(__name__)


@click.group(name='extract')
@click.pass_context
def extract(ctx):
    """ETL meta data from sources."""
    logger.setLevel(ctx.obj['log_level'])
    terra_logger.setLevel(ctx.obj['log_level'])


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
@click.option('--gen3_credentials_path', default=DEFAULT_GEN3_CREDENTIALS_PATH, help='Gen3 credentials. https://anvilproject.org/learn/introduction/getting-started-with-gen3#profile-page---api-keys-and-project-access', show_default=True)
@click.option('--use_terra_credentials', is_flag=True, default=True, help='Running in terra VM, use terra authenticator to access gen3.', show_default=True)
@click.option('--expected_row_count', default=175000, help="Minimum number of file records expected.", show_default=True)
@click.pass_context
def _gen3(ctx, gen3_credentials_path, use_terra_credentials, expected_row_count):
    """Extract meta data from gen3, including drs identifiers."""
    drs_extractor(output_path=ctx.obj['output_path'], expected_row_count=expected_row_count, gen3_credentials_path=gen3_credentials_path, use_terra_credentials=use_terra_credentials)


@extract.command('clean')
@click.pass_context
def _clean(ctx):
    """Drop all databases and downloads."""
    logger.info(f"TODO: {ctx.command.name} remove files from {ctx.obj['output_path']}")


# TODO make terra a subgroup and incorporate commands from extractors.terra
@extract.group(name='terra')
@click.pass_context
def terra(ctx):
    """Commands to download and analyze terra workspaces."""
    pass


@terra.command('fetch')
@click.pass_context
def _terra_fetch(ctx):
    """Extract terra workspaces, write to db."""
    logger.info(f"TODO: {ctx.command.name} write to {ctx.obj['output_path']}")


for command in terra_cli.commands.values():
    terra.add_command(command)

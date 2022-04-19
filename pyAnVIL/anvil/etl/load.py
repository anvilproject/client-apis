import click
import logging


from anvil.etl.loaders.fhir.initializer import fhir as fhir_cli, logger as fhir_logger
from anvil.etl.loaders.fhir.dataset import data_set_cli, logger as data_set_logger
from anvil.etl.loaders.fhir.datastore import data_store_cli, logger as data_store_logger
from anvil.etl.loaders.fhir.implementation_guide import ig_cli, logger as ig_logger


logger = logging.getLogger(__name__)


@click.group(name='load')
@click.pass_context
def load(ctx):
    """Load commands."""
    logger.setLevel(ctx.obj['log_level'])
    fhir_logger.setLevel(ctx.obj['log_level'])
    data_set_logger.setLevel(ctx.obj['log_level'])
    data_store_logger.setLevel(ctx.obj['log_level'])
    ig_logger.setLevel(ctx.obj['log_level'])


@load.command(name='pfb')
@click.pass_context
def _pfb(ctx):
    """Write FHIR schema and data to PFB"""
    logger.info(f"TODO: {ctx.command.name} write files from {ctx.obj['output_path']} to PFB")


@load.group(name='fhir')
@click.pass_context
def fhir(ctx):
    """Commands to setup and load fhir server."""
    pass


@fhir.group(name='data-set')
@click.pass_context
def data_set(ctx):
    """Commands to create and delete data_set."""
    pass


@fhir.group(name='data-store')
@click.pass_context
def data_store(ctx):
    """Commands to create and delete data_store."""
    pass


@fhir.group(name='IG')
@click.pass_context
def ig(ctx):
    """Commands to create and delete implementation guide."""
    pass


for command in fhir_cli.commands.values():
    fhir.add_command(command)

for command in data_set_cli.commands.values():
    data_set.add_command(command)

for command in data_store_cli.commands.values():
    data_store.add_command(command)

for command in ig_cli.commands.values():
    ig.add_command(command)

import click
import logging

logger = logging.getLogger(__name__)


@click.group(name='load')
@click.pass_context
def load(ctx):
    """Load commands."""
    pass

@load.command(name='bucket')
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


@load.command(name='pfb')
@click.pass_context
def _pfb(ctx):
    """Write FHIR schema and data to PFB"""    
    logger.info(f"TODO: {ctx.command.name} write files from {ctx.obj['output_path']} to PFB")


@load.command(name='fhir')
@click.pass_context
def _fhir(ctx):
    """Load to FHIR server."""    
    logger.info(f"TODO: {ctx.command.name} load files from {ctx.obj['output_path']} to https://cloud.google.com/healthcare-api/docs/concepts/fhir")

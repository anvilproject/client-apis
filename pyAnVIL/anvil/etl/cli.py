#!/usr/bin/env python3
import click
import logging

from extract import extract
from transform import transform
from load import load
from anvil.etl import DEFAULT_OUTPUT_PATH

LOG_FORMAT = '%(asctime)s %(name)s %(levelname)-8s %(message)s'
logger = logging.getLogger('etl.cli')


@click.group()
@click.option('-v', '--verbose', count=True, default=2, help="Increase logging level 0-3.", show_default=True)
@click.option('--output_path', default=DEFAULT_OUTPUT_PATH, help=f'output path for working files and output.', show_default=True)
@click.pass_context
def cli(ctx, verbose, output_path):
    """ETL: extract from terra workspaces, google buckets and gen3; transform to FHIR; load to google  Healthcare API."""
    # ensure that ctx.obj exists and is a dict
    # set root logging
    if verbose == 0:
        logging.basicConfig(level=logging.ERROR, format=LOG_FORMAT)
    elif verbose == 1:
        logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT)
    elif verbose == 2:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    else:
        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    ctx.obj['output_path'] = output_path


if __name__ == '__main__':
    cli.add_command(extract)
    cli.add_command(transform)
    cli.add_command(load)
    cli()

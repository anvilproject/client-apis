#!/usr/bin/env python3

import click
import logging

from anvil.etl.extract import extract
from anvil.etl.transform import transform
from anvil.etl.utility import utility
from anvil.etl.load import load
from anvil.etl import DEFAULT_OUTPUT_PATH, NaturalOrderGroup, read_config
from click_loglevel import LogLevel
from importlib_metadata import distribution

LOG_FORMAT = '%(asctime)s %(name)s %(levelname)-8s %(message)s'
logger = logging.getLogger('etl-cli')


@click.group(cls=NaturalOrderGroup)
@click.option("-l", "--log-level", type=LogLevel(), default=logging.INFO)
@click.option('--output_path', default=DEFAULT_OUTPUT_PATH, help='output path for working files and output.', show_default=True)
@click.option('--config_path', default=None, help='path to config file override.', show_default=True)
@click.pass_context
def cli(ctx, log_level, output_path, config_path):
    """ETL: extract from terra workspaces, google buckets and gen3; transform to FHIR; load to google  Healthcare API."""
    # ensure that ctx.obj exists and is a dict
    # set root logging
    logging.basicConfig(level=log_level, format=LOG_FORMAT)
    ctx.ensure_object(dict)
    ctx.obj['log_level'] = log_level
    ctx.obj['output_path'] = output_path
    ctx.obj['config'] = read_config(config_path)


@cli.command()
def version():
    """Print the version."""
    dist = distribution('pyAnVIL')
    print(dist.version)


# load sub-commands
cli.add_command(extract)
cli.add_command(transform)
cli.add_command(load)
cli.add_command(utility)


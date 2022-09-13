import os
from collections import defaultdict

import click
import logging
import json

import requests

from anvil.etl.transform import _consortium_from_workspace
from anvil.etl.transformers.fhir_writer import ensure_data_store_name
from anvil.etl.transformers.normalizer import get_pickled_workspace, fetch_workspace_names
from anvil.etl.utilities.shell_helper import run_cmd

logger = logging.getLogger(__name__)


@click.group(name='fhir')
@click.pass_context
def fhir(ctx):
    pass  # dummy

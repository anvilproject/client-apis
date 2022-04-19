import os
import click
from anvil.etl.transformers.normalizer import ontologies, _recursive_default_dict, fetch_workspace_names, \
    write_workspace

from anvil.etl.transformers.normalizer import normalize, analyze
from anvil.etl.transformers.fhir_writer import write
import json
from multiprocessing import Pool

import logging 
logger = logging.getLogger(__name__)


@click.group(name='transform')
@click.pass_context
def transform(ctx):
    """Transform data commands."""
    # Set log level in our & dependencies loggers
    logger.setLevel(ctx.obj['log_level'])


@transform.command(name='normalize')
@click.option('--consortium', default=None, help='Filter, only this consortium.')
@click.option('--workspace', default=None, help='Filter, only this workspace.')
@click.option('--validate_buckets/--no-validate_buckets', default=True, help='Check gs:// urls against bucket contents.', show_default=True)
@click.pass_context
def _normalize(ctx, consortium, workspace, validate_buckets):
    """Normalize workspace and write summary to stdout."""
    if not consortium:
        consortium = _consortium_from_workspace(ctx.obj['config'], workspace)
        workspace_names = [(consortium, workspace)]
    else:
        workspace_names = fetch_workspace_names(ctx.obj['output_path'], requested_consortium_name=consortium, workspace_name=workspace)

    with Pool(maxtasksperchild=1) as pool:
        for consortium_name, workspace_name in workspace_names:
            pool.starmap(write_workspace, [(ctx.obj['output_path'], consortium, workspace_name, validate_buckets, ctx.obj['config'])])


def _print_analysis(output_path, consortium, workspace, details, validate_buckets, config, file_name):
    """Process worker."""
    analysis = analyze(output_path, consortium, workspace, details, validate_buckets, config)
    if analysis == {}:
        logger.error(('no.analysis', workspace))
        return
    logger.info(f"working on {workspace}")
    _json = json.dumps(analysis[consortium][workspace], separators=(',', ':'))
    with open(file_name, "a") as output_stream:
        output_stream.write(_json)    
        output_stream.write('\n')    


def _consortium_from_workspace(config, workspace_name):    
    """Extract consortium from config."""
    if not workspace_name:
        return None
    for k in config['consortiums']:
        if k.lower() in workspace_name.lower():
            return k
    assert False, f"no consortium for {workspace_name}"


@transform.command(name='analyze')
@click.option('--consortium', default=None, help='Filter, only this consortium.')
@click.option('--workspace', default=None, help='Filter, only this workspace.')
@click.option('--details', default=True, help='Include error details.', show_default=True, is_flag=True)
@click.option('--validate_buckets/--no-validate_buckets', default=True, help='Check gs:// urls against bucket contents.', show_default=True)
@click.pass_context
def _analyze(ctx, consortium, workspace, details, validate_buckets):
    """Retrieve workspace and write summary <data>/analysis.ndjson."""
    if workspace:
        if not consortium:
            consortium = _consortium_from_workspace(ctx.obj['config'], workspace)
        workspace_names = [(consortium, workspace)]
    else:
        workspace_names = fetch_workspace_names(ctx.obj['output_path'], requested_consortium_name=consortium, workspace_name=workspace)

    import os
    import contextlib
    with contextlib.suppress(FileNotFoundError):
        os.remove(f"{ctx.obj['output_path']}/analysis.ndjson")

    file_name = f"{ctx.obj['output_path']}/analysis.ndjson"
    logging.info(f"writing to {file_name}")
    with Pool(maxtasksperchild=1) as pool:
        for consortium_name, workspace_name in workspace_names:
            pool.starmap(_print_analysis, [(ctx.obj['output_path'], consortium_name, workspace_name, details, validate_buckets, ctx.obj['config'], file_name)])


def _fhir_transform(workspace_name, output_path, config, validate_buckets, details):
    for consortium_name, _workspace in normalize(
            output_path, workspace_name=workspace_name,
            requested_consortium_name=None,
            validate_buckets=validate_buckets, config=config):
        write(consortium_name, _workspace, output_path, details, config)


@transform.command(name='fhir')
@click.option('--consortium', default=None, help='Filter, only this consortium.')
@click.option('--workspace', default=None, help='Filter, only this workspace')
@click.option('--validate_buckets/--no-validate_buckets', default=True, help='Check gs:// urls against bucket contents.', show_default=True)
@click.option('--details', default=False, help='Include error details.', show_default=True, is_flag=True)
@click.pass_context
def _fhir(ctx, consortium, workspace, validate_buckets, details):
    """Normalize workspace and write as FHIR to file system (takes several minutes)."""
    if workspace:
        workspace_names = [workspace]
    else:
        workspace_names = fetch_workspace_names(ctx.obj['output_path'], requested_consortium_name=consortium, workspace_name=workspace)

    # with Pool(maxtasksperchild=1) as pool:
    #     for consortium_name, workspace_name in workspace_names:
    #         pool.starmap(_fhir_transform, [(workspace_name, ctx.obj['output_path'], ctx.obj['config'], validate_buckets, details,)])

    for consortium_name, workspace_name in workspace_names:
        _fhir_transform(workspace_name, ctx.obj['output_path'], ctx.obj['config'], validate_buckets, details,)

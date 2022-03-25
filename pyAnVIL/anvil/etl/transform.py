import click

from transformers.normalizer import normalize, analyze
from transformers.fhir_writer import write
import json

@click.group(name='transform')
@click.pass_context
def transform(ctx):
    """Transform data commands."""
    assert ctx.obj['output_path'], 'output_path not set.'

@transform.command(name='normalize')
@click.pass_context
def _normalize(ctx):
    """Normalize workspace and write summary to stdout."""    
    print(json.dumps(analyze(ctx.obj['output_path'])))


@transform.command(name='fhir')
@click.pass_context
def _fhir(ctx):
    """Normalize workspace and write as FHIR to file system."""    
    for consortium_name,  workspace in normalize(ctx.obj['output_path']):
        write(consortium_name,  workspace, ctx.obj['output_path'])





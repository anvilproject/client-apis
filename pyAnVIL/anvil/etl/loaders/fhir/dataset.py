import click
import logging

from anvil.etl.utilities.shell_helper import run_cmd

logger = logging.getLogger(__name__)


@click.group(name='data-set')
@click.pass_context
def data_set_cli(ctx):
    pass  # dummy


@data_set_cli.command(name='create')
@click.option('--data_set',  envvar='GOOGLE_DATASET', help='data set name', show_default=True)
@click.pass_context
def create_dataset(ctx, data_set):
    """Create dataset."""
    check_script = """
        gcloud healthcare datasets list --location=$GOOGLE_LOCATION | grep $GOOGLE_DATASET
    """
    check_results = run_cmd(check_script)
    if check_results and data_set in check_results:
        logger.info(f"{data_set} already exists")
        return
    create_script = """
        gcloud healthcare datasets create $GOOGLE_DATASET --location=$GOOGLE_LOCATION
    """
    print(run_cmd(create_script))


@data_set_cli.command(name='delete')
@click.option('--data_set',  envvar='GOOGLE_DATASET', help='data set name', show_default=True)
@click.pass_context
def delete_dataset(ctx, data_set):
    """Delete dataset."""
    rm_script = """
        gcloud healthcare datasets delete $GOOGLE_DATASET --location=$GOOGLE_LOCATION  --quiet
    """
    print(run_cmd(rm_script))

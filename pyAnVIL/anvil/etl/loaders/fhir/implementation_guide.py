import json
import os

import requests

from anvil.etl.utilities.shell_helper import run_cmd

import click
import logging

from anvil.etl.utilities.shell_helper import run_cmd

logger = logging.getLogger(__name__)


@click.group(name='ig')
@click.pass_context
def ig_cli(ctx):
    pass  # dummy


@ig_cli.command(name='create')
@click.pass_context
def create(ctx):
    """Download and setup IG on local file system, copy to bucket."""

    download_script = """
    mkdir -p  $OUTPUT_PATH/fhir/IG
    # clean up old
    rm -f $OUTPUT_PATH/fhir/IG/*.json || true 2> /dev/null
    rm -f $OUTPUT_PATH/fhir/IG/*.internals || true 2> /dev/null
    rm -f $OUTPUT_PATH/definitions.json.zip || true 2> /dev/null
    rm -f $OUTPUT_PATH/expansions.json.zip || true 2> /dev/null
    # copy from IG build
    curl https://nih-ncpi.github.io/ncpi-fhir-ig/definitions.json.zip --output $OUTPUT_PATH/definitions.json.zip 2> /dev/null 
    curl https://nih-ncpi.github.io/ncpi-fhir-ig/expansions.json.zip --output $OUTPUT_PATH/expansions.json.zip 2> /dev/null

    unzip $OUTPUT_PATH/definitions.json.zip -d $OUTPUT_PATH/fhir/IG
    unzip $OUTPUT_PATH/expansions.json.zip  -d $OUTPUT_PATH/fhir/IG

    # delete extraneous
    rm $OUTPUT_PATH/fhir/IG/*.internals

    """
    # https://cloud.google.com/healthcare/docs/how-tos/fhir-profiles#configure_your_implementation_guide
    run_cmd(download_script)

    # adjust for google
    output_path = ctx.obj["output_path"]
    ig_path = f'{output_path}/fhir/IG/ImplementationGuide-NCPI-FHIR-Implementation-Guide.json'
    ig = json.load(open(ig_path, 'r'))
    # items to add to global
    structure_definitions = [r['reference']['reference'] for r in ig['definition']['resource'] if
                             'StructureDefinition' in r['reference']['reference']]

    ig_global = []
    for _id in structure_definitions:
        _id = _id.replace('/', '-')
        sd_path = f'{output_path}/fhir/IG/{_id}.json'
        sd = json.load(open(sd_path, 'r'))
        if sd['kind'] != 'resource':
            continue
        ig_global.append({'type': sd['type'], 'profile': sd['url']})

    ig['global'] = ig_global
    # logger.info(f"added to 'global' {[g['type'] for g in ig['global']]}")
    json.dump(ig, open(ig_path, 'w'), separators=(',', ':'))

    move_ig_to_bucket = """
    gsutil -m cp -J -r $OUTPUT_PATH/fhir/IG    gs://$GOOGLE_BUCKET/fhir
    # also need to include all dependencies
    curl -s http://hl7.org/fhir/us/core/STU3.1.1/ImplementationGuide-hl7.fhir.us.core.json | gsutil cp - gs://$GOOGLE_BUCKET/IG/ImplementationGuide-hl7.fhir.us.core.json

    curl -s https://www.hl7.org/fhir/definitions.json.zip -o /tmp/definitions.json.zip
    unzip -p /tmp/definitions.json.zip valuesets.json > /tmp/valuesets.json
    cat /tmp/valuesets.json | gsutil cp - gs://$GOOGLE_BUCKET/fhir/IG/valuesets/valuesets.json
    rm /tmp/definitions.json.zip
    rm /tmp/valuesets.json        
    """
    run_cmd(move_ig_to_bucket)

    logger.debug(run_cmd("gsutil ls gs://$GOOGLE_BUCKET/fhir/IG"))

    logger.info(f"IG setup complete and copied to gs://{os.environ['GOOGLE_BUCKET']}/fhir/IG.")


@ig_cli.command(name='delete')
@click.pass_context
def delete(ctx):
    """Remove IG from local file system and bucket."""
    delete_script = """
    rm -r  $OUTPUT_PATH/fhir/IG  || true > /dev/null
    gsutil -m rm -r  gs://$GOOGLE_BUCKET/fhir/IG 
    """
    run_cmd(delete_script)


def _enable_ig(ctx, data_store, token):
    """Enable IG in datastores"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    project = os.environ['GOOGLE_PROJECT']
    location = os.environ['GOOGLE_LOCATION']
    data_set = os.environ['GOOGLE_DATASET']
    url = f"https://healthcare.googleapis.com/v1beta1/projects/{project}/locations/{location}/datasets/{data_set}/fhirStores/{data_store}?updateMask=validationConfig"
    response = requests.patch(
        url=url,
        headers=headers,
        json={
            "validationConfig": {
                "enabledImplementationGuides": ["https://ncpi-fhir.github.io/ncpi-fhir-ig/ImplementationGuide/NCPI-FHIR-Implementation-Guide"]
            }
        }
    )
    if response.status_code == 200:
        logger.info(f"IG enabled on {data_store}")
    else:
        logger.info(("could.not.enable.ig", data_store, response.status_code, response.text))


def _get_ig(ctx, data_store, token):
    """Get IG info"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    project = os.environ['GOOGLE_PROJECT']
    location = os.environ['GOOGLE_LOCATION']
    data_set = os.environ['GOOGLE_DATASET']
    url = f"https://healthcare.googleapis.com/v1beta1/projects/{project}/locations/{location}/datasets/{data_set}/fhirStores/{data_store}"
    response = requests.get(
        url=url,
        headers=headers,
    )
    response.raise_for_status()
    return response.json()

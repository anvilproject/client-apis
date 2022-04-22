#!/usr/bin/env python3

"""Send query to multiple FHIR endpoints, consume all pages, write results to stdout."""

import multiprocessing
import queue
import os
import json
import sys
import traceback
import click
import requests
import logging

from anvil.etl.utilities.shell_helper import run_cmd

TOKEN = None
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(threadName)s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)


def _worker(input_queue, stop_event):
    """Consume input queue, query FHIR service, write to stdout."""
    global TOKEN

    while not stop_event.is_set():
        try:
            # Check if any URL has arrived in the input queue. If not,
            # loop back and try again.
            url = input_queue.get(True, 1)
            input_queue.task_done()
        except queue.Empty:
            continue

        def fetch(url):
            headers = {"Authorization": f"Bearer {_get_token()}"}
            logger.debug((url, headers))
            response = requests.get(url, headers=headers)
            try:
                response.raise_for_status()
                _json = response.json()
                _next = None
                if 'link' in _json:
                    _links = {lnk['relation']: lnk['url'] for lnk in _json['link']}
                    if 'next' in _links:
                        _next = _links['next']
                return _json, _next
            except Exception:
                logger.error(response.content.decode("utf-8"))
                traceback.print_tb(sys.exc_info()[2])
                raise

        _next = url
        while _next:
            (_json, _next) = fetch(_next)
            print(json.dumps(_json, separators=(',', ':')))


def _dispatch(project, location, dataset, all_data_stores, requested_data_store, path, token):
    """Manufacture URLs, dispatch to threads."""
    assert path.startswith('/'), "Should start with /"
    all_data_stores = all_data_stores.split(',')
    if requested_data_store.lower() == 'dispatch':
        data_stores = all_data_stores
    else:
        # assert requested_data_store in all_data_stores, f"{requested_data_store} not in {all_data_stores}"
        data_stores = [requested_data_store]

    base_urls = [f"https://healthcare.googleapis.com/v1beta1/projects/{project}/locations/{location}/datasets/{dataset}/fhirStores/{ds.strip()}/fhir" for ds in data_stores if len(ds.strip()) > 0]
    urls = [f"{bu}{path}"for bu in base_urls]
    input_queue = multiprocessing.JoinableQueue()
    stop_event = multiprocessing.Event()

    global TOKEN
    TOKEN = token

    workers = []

    # Create workers.
    for i in range(16):  # range(len(base_urls)):
        p = multiprocessing.Process(target=_worker,
                                    args=(input_queue, stop_event))
        workers.append(p)
        p.start()

    # Distribute work.
    for url in urls:
        input_queue.put(url)

    # Wait for the queue to be consumed.
    input_queue.join()

    # Ask the workers to quit.
    stop_event.set()

    # Wait for workers to quit.
    for w in workers:
        w.join()


def _get_datastores():
    return os.environ.get('GOOGLE_DATASTORES', None)


def _get_datastores_help():
    datastores = _get_datastores()
    if datastores:
        return ""
    return """export GOOGLE_DATASTORES=$(gcloud beta healthcare fhir-stores list --dataset=$GOOGLE_DATASET --location=$GOOGLE_LOCATION --format="table[no-heading](ID)" | tr '\n' ',')"""
    # export GOOGLE_DATASTORES=$(gcloud beta healthcare fhir-stores list --dataset=$GOOGLE_DATASET --location=$GOOGLE_LOCATION | awk '(NR>1){print $1}' | sed -z 's/\n/,/g')

def _get_token():
    return os.environ.get('TOKEN', None)


def _get_token_help():
    token = _get_token()
    if token:
        return f"{token[0:10]}..."
    return """export TOKEN=$(gcloud auth application-default print-access-token)"""


@click.command()
@click.option('--project', default=os.environ.get('GOOGLE_PROJECT', None), show_default=True, help='env var. GOOGLE_PROJECT')
@click.option('--location', default=os.environ.get('GOOGLE_LOCATION', None), show_default=True, help='env var. GOOGLE_LOCATION')
@click.option('--dataset', default=os.environ.get('GOOGLE_DATASET', None), show_default=True, help='env var. GOOGLE_DATASET')
@click.option('--token', default=_get_token(), help=f'env var. TOKEN [default: {_get_token_help()}]')
@click.option('--data_stores', default=_get_datastores(), show_default=True, help=f'env var. GOOGLE_DATASTORES. [default: {_get_datastores_help()}]')
@click.option('--data_store', default='dispatch', show_default=True, help="Name of target data store, 'dispatch' will send to all data stores.")
@click.argument('path')
def cli(project, location, dataset, token, data_stores, data_store, path):
    """Utility to manufacture FHIR URLs, dispatch path via threads."""
    if not token:
        logger.info("Creating token")
        token = run_cmd("gcloud auth application-default print-access-token")
    if not token:
        token = run_cmd("gcloud auth application-default print-access-token")
    _dispatch(project, location, dataset, data_stores, data_store, path, token)


if __name__ == '__main__':
    cli()

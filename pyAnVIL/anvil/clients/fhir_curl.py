#!/usr/bin/env python3

"""Send query to multiple FHIR endpoints, consume all pages, write results to stdout."""

import json
import logging
import os
import urllib.parse as urlparse

import click
from click_loglevel import LogLevel
from fhirclient.server import FHIRJSONMimeType

from anvil.clients.fhir_client import DispatchingFHIRClient
from anvil.clients.smart_auth import GoogleFHIRAuth
from anvil.etl.anvil_etl import LOG_FORMAT

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(threadName)s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)


def worker(server, path=None, no_sign=False, headers={}):
    """Callback handler"""

    def fetch(url, _headers):
        """Low level GET, follow next links.
         see https://github.com/smart-on-fhir/client-py/blob/master/fhirclient/server.py#L174."""
        header_defaults = {
            'Accept': FHIRJSONMimeType,
            'Accept-Charset': 'UTF-8',
        }
        # merge in user headers with defaults
        header_defaults.update(_headers)
        _headers = header_defaults
        if not no_sign and server.auth is not None and server.auth.can_sign_headers():
            _headers = server.auth.signed_headers(_headers)
        # perform the request but intercept 401 responses, raising our own Exception
        res = server.session.get(url, headers=_headers)
        server.raise_for_status(res)
        __json = res.json()
        __next = None
        if 'link' in __json:
            _links = {lnk['relation']: lnk['url'] for lnk in __json['link']}
            if 'next' in _links:
                __next = _links['next']
        return __json, __next

    # main
    assert path, "caller MUST pass path"
    if path.startswith('/'):
        path = path[:1]
    initial_url = urlparse.urljoin(server.base_uri, path)
    _url = initial_url
    while _url:
        (_json, _url) = fetch(_url, _headers=headers)
        print(json.dumps(_json, separators=(',', ':')), flush=True)


def _dispatch(project, location, dataset, all_data_stores, requested_data_store, path, token):
    """Manufacture URLs, dispatch to thread."""

    all_data_stores = all_data_stores.split(',')
    if requested_data_store.lower() == 'dispatch':
        data_stores = all_data_stores
    else:
        # assert requested_data_store in all_data_stores, f"{requested_data_store} not in {all_data_stores}"
        data_stores = [requested_data_store]

    api_bases = [f"https://healthcare.googleapis.com/v1beta1/projects/{project}/locations/{location}/datasets/{dataset}/fhirStores/{ds.strip()}/fhir" for ds in data_stores if len(ds.strip()) > 0]

    settings = {
        'app_id': 'my_web_app',
        'api_bases': api_bases
    }
    client = DispatchingFHIRClient(settings=settings, auth=GoogleFHIRAuth(access_token=token))
    client.prepare()
    assert client.ready, "server should be ready"
    assert project in client.server.base_uri
    client.dispatch(worker, path=path)


def _get_data_stores():
    return os.environ.get('GOOGLE_DATASTORES', None)


def _get_data_stores_help():
    data_stores = _get_data_stores()
    if data_stores:
        return ""
    return """source /dev/stdin <<< `anvil_etl utility env`  """


def _get_token():
    return os.environ.get('TOKEN', None)


def _get_token_help():
    token = _get_token()
    if token:
        return f"{token[0:10]}..."
    return """export TOKEN=$(gcloud auth application-default print-access-token)"""


class DefaultCommand(click.Command):
    def parse_args(self, ctx, args):
        if len(args) == 1:
            args.insert(0, 'dispatch')
        super(DefaultCommand, self).parse_args(ctx, args)


@click.command(cls=DefaultCommand)
@click.option('--project', default=os.environ.get('GOOGLE_PROJECT', None), show_default=True, help='env var. GOOGLE_PROJECT')
@click.option('--location', default=os.environ.get('GOOGLE_LOCATION', None), show_default=True, help='env var. GOOGLE_LOCATION')
@click.option('--dataset', default=os.environ.get('GOOGLE_DATASET', None), show_default=True, help='env var. GOOGLE_DATASET')
@click.option('--token', default=_get_token(), help=f'env var. TOKEN [default: {_get_token_help()}]')
@click.option('--data_stores', default=_get_data_stores(), show_default=True, help=f'env var. GOOGLE_DATASTORES. [default: {_get_data_stores_help()}]')
@click.option("-l", "--log-level", type=LogLevel(), default=logging.INFO)
@click.argument('data_store', required=False, default='dispatch')
@click.argument('path', required=True)
def cli(project, location, dataset, token, data_stores, data_store, path, log_level):
    """Utility to manufacture FHIR URLs, dispatch path via threads.

        data_store: FHIR data store name.  'dispatch' will send to all data stores.
        path: FHIR compliant path i.e. http{s}://server{/path} see https://www.hl7.org/fhir/http.html#general
    """
    logging.basicConfig(level=log_level, format=LOG_FORMAT)
    _dispatch(project, location, dataset, data_stores, data_store, path, token)


if __name__ == '__main__':
    cli()

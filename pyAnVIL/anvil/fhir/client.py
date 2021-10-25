"""Instances of this class handle authorizing and talking to Google Healthcare API FHIR Service."""


import logging
import threading
from urllib.parse import urlparse
from fhirclient import client
from fhirclient.models.meta import Meta
from fhirclient.models.bundle import Bundle

from anvil.fhir.smart_auth import GoogleFHIRAuth

logger = logging.getLogger(__name__)


class FHIRClient(client.FHIRClient):
    """Instances of this class handle authorizing and talking to Google Healthcare API FHIR Service.

    Parameters:
        See https://github.com/smart-on-fhir/client-py/blob/master/fhirclient/client.py#L19

    Returns:
        Instance of client, with injected authorization method

    Examples: ::
        from anvil.fhir.client import FHIRClient
        settings = {
            'app_id': 'my_web_app',
            'api_base': 'https://healthcare.googleapis.com/v1/projects/gcp-testing-308520/locations/us-east4/datasets/testset/fhirStores/fhirstore/fhir'
        }
        smart = FHIRClient(settings=settings)
        assert smart.ready, "server should be ready"
        # search for all ResearchStudy
        import fhirclient.models.researchstudy as rs
        [s.title for s in rs.ResearchStudy.where(struct={}).perform_resources(smart.server)]
        >>>
        ['1000g-high-coverage-2019', 'my NCPI research study example']


    """

    def __init__(self, *args, **kwargs):
        """Pass args to super, adds GoogleFHIRAuth authenticator, prepares connection."""
        super(FHIRClient, self).__init__(*args, **kwargs)
        client_major_version = int(client.__version__.split('.')[0])
        assert client_major_version >= 4, f"requires version >= 4.0.0 current version {client.__version__} `pip install -e git+https://github.com/smart-on-fhir/client-py#egg=fhirclient`"
        self.server.auth = GoogleFHIRAuth()
        self.server.session.hooks['response'].append(self.server.auth.handle_401)
        self.prepare()
        assert self.ready, "server should be ready"


class DispatchingFHIRClient(client.FHIRClient):
    """Instances of this class handle authorizing and talking to Google Healthcare API FHIR Service.

    Parameters:
        See https://github.com/smart-on-fhir/client-py/blob/master/fhirclient/client.py#L19

    :param settings.api_bases: The servers against which to perform the search **settings.api_base ignored**
    :param access_token: Optional access token, if none provided `gcloud auth print-access-token` is used

    Returns:
        Instance of client, with injected authorization method

    Examples: ::
        from anvil.fhir.client import DispatchingFHIRClient
        from fhirclient.models.researchstudy import ResearchStudy
        from collections import defaultdict
        from pprint import pprint

        settings = {
            'app_id': 'my_web_app',
            'api_bases': [
                'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-11-329119/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir',
                'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-11-329119/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir',
            ]
        }
        smart = DispatchingFHIRClient(settings=settings)

        # search for all ResearchStudy, index by source
        studies = defaultdict(list)
        for s in ResearchStudy.where(struct={'_count':'1000'}).perform_resources(smart.server):
            studies[s.meta.source].append(s)

        pprint({k: len(v) for k,v in studies.items()})
        >>>
        {'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-11-329119/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir/': 259,
        'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-11-329119/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir/': 393}

    """

    def __init__(self, *args, **kwargs):
        """Pass args to super, patches `perform` to our dispatching version."""
        # use the first entry as 'our' server
        _settings = dict(kwargs['settings'])
        api_base = _settings['api_bases'].pop()
        _settings['api_base'] = api_base
        kwargs['settings'] = _settings

        # grab a token if passed
        access_token = None
        if 'access_token' in kwargs:
            access_token = kwargs['access_token']
            del kwargs['access_token']

        # normal setup with our authenticator
        super(DispatchingFHIRClient, self).__init__(*args, **kwargs)
        client_major_version = int(client.__version__.split('.')[0])
        assert client_major_version >= 4, f"requires version >= 4.0.0 current version {client.__version__} `pip install -e git+https://github.com/smart-on-fhir/client-py#egg=fhirclient`"
        self.server.auth = GoogleFHIRAuth(access_token=access_token)
        self.server.session.hooks['response'].append(self.server.auth.handle_401)
        self.prepare()
        assert self.ready, "server should be ready"

        # set up an array of FHIRClients, including this instance, in self._clients
        # re-use authenticator
        self._clients = [self]
        self._api_bases = _settings['api_bases']
        for api_base in self._api_bases:
            __settings = dict(_settings)
            __settings['api_base'] = api_base
            _client = client.FHIRClient(settings=__settings)
            _client.server.auth = self.server.auth
            _client.server.session.hooks['response'].append(self.server.auth.handle_401)
            _client.prepare()
            self._clients.append(_client)

        # monkey patch search perform if we haven't already
        from fhirclient.models.fhirsearch import FHIRSearch
        if not hasattr(FHIRSearch, '_anvil_patch'):
            FHIRSearch._anvil_patch = True
            logger.debug("******** Needs patching ********")
            original_perform = FHIRSearch.perform
            me = self

            def _perform(self, server):
                """Dispatch query to api_bases."""

                def _worker(self, server, _results):
                    """Dispatches request to underlying class, return an entry indexed by base uri.

                    Sets bundle.meta.source
                    See https://www.hl7.org/fhir/resource-definitions.html#Meta.source

                    :param server: The server against which to perform the search
                    :_results: Result of operation added to this array
                    """
                    logger.debug(f"worker starting {server.base_uri}")
                    result = original_perform(self, server)
                    logger.debug(f"worker got {result}")
                    while result:

                        # add source to meta if it doesn't already exist
                        if not result.meta:
                            result.meta = Meta()
                        if not result.meta.source:
                            result.meta.source = server.base_uri
                        _results.append(result)

                        # follow `next` link for pagination
                        if hasattr(result, 'link'):
                            _next = next((lnk.as_json() for lnk in result.link if lnk.relation == 'next'), None)
                            result = None
                            if _next:
                                logger.debug(f"has next {_next}")
                                # request_json takes a full path & query (not host)
                                parts = urlparse(_next['url'])
                                assert len(parts.query) > 0, parts
                                path = f"{parts.path}?{parts.query}"
                                logger.debug(f"attempting next {path}")
                                res = server.request_json(path)
                                result = Bundle(res)
                                result.origin_server = server
                        else:
                            result = None
                    logger.debug(f"worker done {result}")

                logger.debug("starting threads")
                workers = []
                results = []
                for _client in me._clients:
                    workers.append(
                        threading.Thread(target=_worker, args=(self, _client.server, results, ))
                    )
                # Start workers.
                for w in workers:
                    w.start()

                # Wait for workers to quit.
                logger.debug("waiting for results.")
                for w in workers:
                    w.join()
                logger.debug(f"all workers done. {len(results)}")
                return results
            # monkey patch
            FHIRSearch.perform = _perform

            # since perform returns an array, patch _perform_resources as well.
            def _perform_resources(self, server):
                """Perform the search by calling `perform`, then extracts all Bundle entries and returns a list of Resource instances.

                Sets resource.meta.source
                See https://www.hl7.org/fhir/resource-definitions.html#Meta.source

                :param server: The server against which to perform the search
                :returns: A list of Resource instances
                """
                # flatten into an array of resources
                bundles = self.perform(server)
                resources = []
                if bundles is not None:
                    if not isinstance(bundles, list):
                        bundles = [bundles]
                    for bundle in bundles:
                        if bundle.entry:
                            for entry in bundle.entry:
                                if not entry.resource.meta:
                                    entry.resource.meta = Meta()
                                if not entry.resource.meta.source:
                                    entry.resource.meta.source = bundle.meta.source
                                resources.append(entry.resource)
                logger.debug("_perform_resources done.")
                return resources
            FHIRSearch.perform_resources = _perform_resources

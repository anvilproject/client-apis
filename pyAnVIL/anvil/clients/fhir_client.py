"""Instances of this class handle authorizing and talking to Google Healthcare API FHIR Service."""


import logging
import concurrent.futures
from urllib.parse import urlparse
from fhirclient import client, server as fhirclient_server
from fhirclient.models.meta import Meta
from fhirclient.models.bundle import Bundle
import resource

from fhirclient.server import FHIRPermissionDeniedException

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
        """Pass args to super, add authenticator, prepares connection."""
        # grab auth if passed
        auth = None
        if 'auth' in kwargs:
            auth = kwargs['auth']
            del kwargs['auth']
        super(FHIRClient, self).__init__(*args, **kwargs)
        client_major_version = int(client.__version__.split('.')[0])
        assert client_major_version >= 4, f"requires version >= 4.0.0 current version {client.__version__} `pip install -e git+https://github.com/smart-on-fhir/client-py#egg=fhirclient`"
        print(f"auth {auth}")
        if auth:
            logger.debug("Setting auth")
            print("setting auth")
            self.server.auth = auth
            self.server.session.hooks['response'].append(self.server.auth.handle_401)
        self.prepare()
        assert self.ready, "server should be ready"


class DispatchingFHIRClient(client.FHIRClient):
    """Instances of this class handle authorizing and talking to Google Healthcare API FHIR Service.

    Parameters:
        See https://github.com/smart-on-fhir/client-py/blob/master/fhirclient/client.py#L19

    :param settings.api_bases: The servers against which to perform the search **settings.api_base ignored**
    :param auth: An instance of FHIRAuth that will authenticate each request.
    :param max_workers: Number of thread workers, if null will be set to the maximum of open files per process
    or the number of api bases, whichever is less.

    Returns:
        Instance of client, with injected authorization method

    Examples: ::
        from anvil.clients.fhir_client import DispatchingFHIRClient
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
        smart = DispatchingFHIRClient(settings=settings, auth=GoogleFHIRAuth())

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

        # grab retrieve_all if passed
        self._retrieve_all = False
        if 'retrieve_all' in kwargs['settings']:
            self._retrieve_all = kwargs['settings']['retrieve_all']
            del kwargs['settings']['retrieve_all']

        # grab auth if passed
        auth = None
        if 'auth' in kwargs:
            auth = kwargs['auth']
            del kwargs['auth']

        max_workers = None

        if 'max_workers' in kwargs:
            max_workers = kwargs['max_workers']
            del kwargs['max_workers']

        if not max_workers:
            max_workers = int(resource.getrlimit(resource.RLIMIT_NOFILE)[0]/3)
            if max_workers > len(_settings['api_bases']):
                max_workers = max(len(_settings['api_bases']), 1)
            logger.debug(f"Setting number of threads to {max_workers}")

        # normal setup with our authenticator
        super(DispatchingFHIRClient, self).__init__(*args, **kwargs)
        client_major_version = int(client.__version__.split('.')[0])
        assert client_major_version >= 4, f"requires version >= 4.0.0 current version {client.__version__} `pip install -e git+https://github.com/smart-on-fhir/client-py#egg=fhirclient`"

        if auth:
            self.server.auth = auth
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
            original_perform = FHIRSearch.perform
            me = self

            def _perform(self, server):
                """Dispatch query to api_bases."""
                # FHIRSearch can be used by multiple classes, don't dispatch unless one of ours
                if not server.client.__class__.__name__ == 'DispatchingFHIRClient':
                    logger.debug(f"* * * * * * * original_perform {server.client.__class__.__name__}")
                    return original_perform(self, server)

                def _worker(self, server):
                    """Dispatches request to underlying class, return an entry indexed by base uri.

                    Sets bundle.meta.source
                    See https://www.hl7.org/fhir/resource-definitions.html#Meta.source

                    :param server: The server against which to perform the search
                    :_results: Result of operation added to this array
                    """
                    logger.debug(f"worker starting {server.base_uri}")
                    _worker_results = []
                    _result = original_perform(self, server)
                    logger.debug(f"worker got {_result}")

                    while _result:

                        # add source to meta if it doesn't already exist
                        if not _result.meta:
                            _result.meta = Meta()
                        if not _result.meta.source:
                            _result.meta.source = server.base_uri
                        _worker_results.append(_result)
                        if not me._retrieve_all:
                            break

                        # follow `next` link for pagination
                        if hasattr(_result, 'link'):
                            _next = next((lnk.as_json() for lnk in _result.link if lnk.relation == 'next'), None)
                            _result = None
                            if _next:
                                logger.debug(f"has next {_next}")
                                # request_json takes a full path & query (not host)
                                parts = urlparse(_next['url'])
                                assert len(parts.query) > 0, parts
                                path = f"{parts.path}?{parts.query}"
                                logger.debug(f"attempting next {path}")
                                res = server.request_json(path)
                                _result = Bundle(res)
                                _result.origin_server = server
                        else:
                            _result = None
                    logger.debug(f"worker done {len(_worker_results)}")
                    return _worker_results

                logger.debug("starting threads")
                results = []
                # We can use a with statement to ensure threads are cleaned up promptly
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Start the load operations and mark each future with its URL
                    future_result = {executor.submit(_worker, self, __client.server, ): __client for __client
                                     in server.client._clients}
                    for future in concurrent.futures.as_completed(future_result):
                        try:
                            worker_results = future.result()
                            results.extend(worker_results)
                        except Exception as exc:
                            if 'FHIRPermissionDeniedException' in exc.__class__.__name__:
                                # requests.response embedded in exception
                                logger.error(f"{str(exc)} {exc.args[0].url}")
                            raise exc
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

                                # add tag for fullURL, allows caller
                                # to disambiguate resources returned from different base URL
                                # TODO Expecting property "tag" on <class 'fhirclient.models.meta.Meta'> to be <class 'fhirclient.models.coding.Coding'>, but is <class 'dict'>
                                # if not entry.resource.meta.tag:
                                #     entry.resource.meta.tag = []
                                # entry.resource.meta.tag.append({
                                #     "system" : "https://nih-ncpi.github.io/ncpi-fhir-ig/#fullUrl",
                                #     "code" : entry.fullUrl
                                # })
                                resources.append(entry.resource)
                return resources
            FHIRSearch.perform_resources = _perform_resources
            logger.debug("Patched FHIRSearch")

    @property
    def clients(self):
        """Expose our list of clients for caller to add to."""
        return self._clients

"""Instances of this class handle authorizing and talking to Google Healthcare API FHIR Service."""

from fhirclient import client
from anvil.fhir.smart_auth import GoogleFHIRAuth


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

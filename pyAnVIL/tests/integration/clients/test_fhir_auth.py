import os
from anvil.clients.fhir_client import FHIRClient
from anvil.clients.smart_auth import GoogleFHIRAuth
import fhirclient.models.researchstudy as rs


def test_query():
    settings = {
        'app_id': 'my_web_app',
        'api_base': 'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir'
    }
    smart = FHIRClient(settings=settings, auth=GoogleFHIRAuth())
    smart.prepare()
    assert smart.ready, "server should be ready"
    # search for all ResearchStudy
    ids = [s.id for s in rs.ResearchStudy.where(struct={}).perform_resources(smart.server)]
    print(ids)
    assert len(ids) > 0, "Should have at least one ResearchStudy"

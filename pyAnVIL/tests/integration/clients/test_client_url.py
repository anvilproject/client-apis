from anvil.clients.fhir_client import FHIRClient, DispatchingFHIRClient
from anvil.clients.smart_auth import GoogleFHIRAuth
import fhirclient.models.researchstudy as rs



def test_query_simple():
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


def test_query_request_jsonl():
    settings = {
        'app_id': 'my_web_app',
        'api_base': 'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir'
    }
    client = FHIRClient(settings=settings, auth=GoogleFHIRAuth())
    client.prepare()
    assert client.ready, "server should be ready"
    response = client.server.request_json(path=f'{client.server.base_uri}ResearchStudy?_elements=id&_count=1000')
    assert 'entry' in response, response


def test_dispatching_query_get():
    settings = {
        'app_id': 'my_web_app',
        'api_bases': [
            'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir',
            'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir',
        ]
    }
    client = DispatchingFHIRClient(settings=settings, auth=GoogleFHIRAuth())
    client.prepare()
    assert client.ready, "server should be ready"
    assert 'fhir-test-16-342800' in client.server.base_uri
    response = client.server._get(path='ResearchStudy?_elements=id&_count=1000')
    assert response.json()


def test_dispatching_worker():
    settings = {
        'app_id': 'my_web_app',
        'api_bases': [
            'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir',
            'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir',
        ]
    }
    client = DispatchingFHIRClient(settings=settings, auth=GoogleFHIRAuth())
    client.prepare()
    assert client.ready, "server should be ready"
    assert 'fhir-test-16-342800' in client.server.base_uri

    def worker(server):
        """Callback handler"""
        response = server._get(path='ResearchStudy?_elements=id&_count=1000')
        assert response.json()
        return response.json()

    results = client.dispatch(worker)
    for r in results:
        assert 'entry' in r



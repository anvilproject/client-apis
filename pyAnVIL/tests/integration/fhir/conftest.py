"""Provide test fixtures."""

import pytest
import os
from anvil.clients.fhir_client import DispatchingFHIRClient
from anvil.clients.smart_auth import KidsFirstFHIRAuth, GoogleFHIRAuth
from fhirclient.models.researchstudy import ResearchStudy
from fhirclient.models.observation import Observation

@pytest.fixture
def token():
    """Optional access token, if none provided GoogleFHIRAuth will perform `gcloud auth print-access-token` is used."""
    return os.environ.get('TOKEN', None)


@pytest.fixture
def kids_first_cookie():
    """AWSELBAuthSessionCookie cookie captured from https://kf-api-fhir-service.kidsfirstdrc.org browser"""
    assert 'KIDS_FIRST_COOKIE' in os.environ, "AWSELBAuthSessionCookie cookie captured from https://kf-api-fhir-service.kidsfirstdrc.org browser"
    return os.environ['KIDS_FIRST_COOKIE']


@pytest.fixture
def anvil_client(token):
    """Create FHIRClient for AnVIL server."""
    settings = {
        'app_id': __name__,
        'api_bases': [
            'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir',
        ]
    }
    return DispatchingFHIRClient(settings=settings, auth=GoogleFHIRAuth(access_token=token))


@pytest.fixture
def kids_first_client(kids_first_cookie):
    """Create FHIRClient for Kids First server. 
    :param kids_first_cookie: AWSELBAuthSessionCookie cookie captured from browser after login to https://kf-api-fhir-service.kidsfirstdrc.org"""

    settings = {
        'app_id': __name__,
        'api_bases': ['https://kf-api-fhir-service.kidsfirstdrc.org'],
        'retrieve_all': True
    }
    return DispatchingFHIRClient(settings=settings, auth=KidsFirstFHIRAuth(cookie=kids_first_cookie))


@pytest.fixture
def dbgap_client():
    """Create FHIRClient for dbgap (no auth)."""
    settings = {
        'app_id': __name__,
        'api_bases': ['https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1']
    }
    return DispatchingFHIRClient(settings=settings)


@pytest.fixture
def aggregated_client(token, kids_first_server):
    """Create FHIRClient for AnVIL and Kids First servers."""
    settings = {
        'app_id': __name__,
        'api_bases': [
            'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-11-329119/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir',
        ]
    }
    dispatching_client = DispatchingFHIRClient(settings=settings, auth=GoogleFHIRAuth(access_token=token))
    dispatching_client.clients.append(kids_first_server)
    return dispatching_client


@pytest.fixture
def search_research_studies_with_observations():
    """Search clause for all ResearchStudy with Observation"""
    return ResearchStudy.where(struct={}).include('focus', Observation, reverse=True)


@pytest.fixture
def dbgap_research_studies_with_observations(dbgap_client, search_research_studies_with_observations):
    """
    Perform search on dbgap_server.
    :return: List of resources
    """
    return search_research_studies_with_observations.perform_resources(dbgap_client.server)


@pytest.fixture
def kids_first_research_studies_with_observations(caplog, kids_first_client, search_research_studies_with_observations):
    """
    Perform search on kids_first_server.
    :return: List of resources
    """
    # caplog.set_level(logging.DEBUG)
    return search_research_studies_with_observations.perform_resources(kids_first_client.server)


@pytest.fixture
def anvil_research_studies_with_observations(anvil_client, search_research_studies_with_observations):
    """
    Perform search on anvil_server.
    :return: List of resources
    """
    return search_research_studies_with_observations.perform_resources(anvil_client.server)

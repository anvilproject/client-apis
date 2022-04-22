"""
These tests request a single record from NCPI servers.
"""

from collections import defaultdict
import os
import pytest

from anvil.clients.fhir_client import DispatchingFHIRClient
from anvil.clients.smart_auth import KidsFirstFHIRAuth, GoogleFHIRAuth

from fhirclient.models.researchstudy import ResearchStudy
from fhirclient.models.observation import Observation
from fhirclient.models.researchsubject import ResearchSubject
from fhirclient.models.bundle import Bundle
from urllib.parse import urlparse

import logging


@pytest.fixture
def token():
    """Optional access token, if none provided GoogleFHIRAuth will perform `gcloud auth print-access-token` is used."""
    return os.environ.get('TOKEN', None)


@pytest.fixture
def kids_first_cookie():
    """AWSELBAuthSessionCookie cookie captured from https://kf-api-fhir-service.kidsfirstdrc.org browser"""
    assert 'KIDS_FIRST_COOKIE' in os.environ
    assert os.environ['KIDS_FIRST_COOKIE'].startswith('AWSELBAuthSessionCookie')
    return os.environ['KIDS_FIRST_COOKIE']


@pytest.fixture
def anvil_server(token):
    """Create FHIRClient for AnVIL server."""
    settings = {
        'app_id': __name__,
        'api_bases': [
            'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir',
        ]
    }
    return DispatchingFHIRClient(settings=settings, auth=GoogleFHIRAuth(access_token=token))


@pytest.fixture
def kids_first_server(kids_first_cookie):
    """Create FHIRClient for Kids First server. 
    :param kids_first_cookie: AWSELBAuthSessionCookie cookie captured from browser after login to https://kf-api-fhir-service.kidsfirstdrc.org"""

    settings = {
        'app_id': __name__,
        'api_bases': ['https://kf-api-fhir-service.kidsfirstdrc.org']
    }
    return DispatchingFHIRClient(settings=settings, auth=KidsFirstFHIRAuth(cookie=kids_first_cookie))


@pytest.fixture
def dbgap_server():
    """Create FHIRClient for dbgap (no auth)."""
    settings = {
        'app_id': __name__,
        'api_bases': ['https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1']
    }
    return DispatchingFHIRClient(settings=settings)


@pytest.fixture
def query_one():
    """Retrieve a single ResearchStudy."""
    return ResearchStudy.where(struct={'_count': '1'})


def test_simple_kf(kids_first_server, query_one):
    """Return a single resource."""
    resources = query_one.perform_resources(kids_first_server.server)
    assert len(resources) == 1


def test_simple_anvil(anvil_server, query_one):
    """Return a single resource."""
    resources = query_one.perform_resources(anvil_server.server)
    assert len(resources) == 1


def test_simple_dbgap(dbgap_server, query_one):
    """Return a single resource."""
    resources = query_one.perform_resources(dbgap_server.server)
    assert len(resources) == 1

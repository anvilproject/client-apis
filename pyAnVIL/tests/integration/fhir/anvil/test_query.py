import os
import pytest

from anvil.clients.fhir_client import DispatchingFHIRClient
from anvil.clients.smart_auth import GoogleFHIRAuth
from anvil.etl.utilities.shell_helper import run_cmd
from fhirclient.models.researchstudy import ResearchStudy
import logging


# print("LOG LEVEL:", logging.getLogger().getEffectiveLevel())
# assert logging.getLogger().getEffectiveLevel() == logging.DEBUG, logging.getLogger().getEffectiveLevel()

logger = logging.getLogger(__name__)


@pytest.fixture
def token():
    """Optional access token, if none provided GoogleFHIRAuth will perform `gcloud auth print-access-token` is used."""
    return os.environ.get('TOKEN', None)


@pytest.fixture
def aggregated_server(token):
    """Create FHIRClient for AnVIL and Kids First servers."""
    settings = {
        'app_id': __name__,
        'api_bases': None
    }
    if not token:
        token = run_cmd("gcloud auth application-default print-access-token")
    list_cmd = f'gcloud beta healthcare fhir-stores list --dataset=$GOOGLE_DATASET  --location=$GOOGLE_LOCATION  --format="table[no-heading](ID)"'
    data_stores = [data_store for data_store in  run_cmd(list_cmd).split('\n')]
    settings['api_bases'] = [f'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/{data_store}/fhir' for data_store in data_stores]
    logger.info(f"Query dispatching to {len(settings['api_bases'])} data-stores")
    return DispatchingFHIRClient(settings=settings, auth=GoogleFHIRAuth(access_token=token))


@pytest.fixture
def query_all_research_studies():
    """Retrieve a single ResearchStudy."""
    return ResearchStudy.where(struct={'_count': '1000', '_elements': 'id,status'})


@pytest.fixture
def minimum_expected_study_count():
    """How many research studies are there?"""
    return 440


def test_dispatching_query(aggregated_server, query_all_research_studies, minimum_expected_study_count):
    """Retrieve all studies from all data stores."""
    all_studies = query_all_research_studies.perform_resources(aggregated_server.server)
    assert len(all_studies) > minimum_expected_study_count, f"Expected over {minimum_expected_study_count} studies, got {len(all_studies)}"

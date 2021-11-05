import os
import pytest

from anvil.clients.fhir_client import DispatchingFHIRClient
from anvil.clients.smart_auth import KidsFirstFHIRAuth, GoogleFHIRAuth

from fhirclient.models.researchstudy import ResearchStudy
from fhirclient.models.observation import Observation
from fhirclient.models.practitioner  import Practitioner
from fhirclient.models.researchsubject  import ResearchSubject
from fhirclient.models.patient  import Patient
from fhirclient.models.bundle  import Bundle

import logging

@pytest.fixture
def token():
    """Optional access token, if none provided GoogleFHIRAuth will perform `gcloud auth print-access-token` is used."""
    return os.environ.get('TOKEN', None)


@pytest.fixture
def kids_first_cookie():
    """AWSELBAuthSessionCookie cookie captured from https://kf-api-fhir-service.kidsfirstdrc.org browser"""
    assert 'KIDS_FIRST_COOKIE' in os.environ
    return os.environ['KIDS_FIRST_COOKIE']


@pytest.fixture
def anvil_server(token):
    """Create FHIRClient for AnVIL server."""
    settings = {
        'app_id': __name__,
        'api_bases': [
            'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-11-329119/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir',
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
def aggregated_server(token, kids_first_server):
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
def anvil_research_studies_with_observations(anvil_server, search_research_studies_with_observations):
    """
    Perform search on anvil_server.
    :return: List of resources
    """
    return search_research_studies_with_observations.perform_resources(anvil_server.server)


@pytest.fixture
def kids_first_research_studies_with_observations(kids_first_server, search_research_studies_with_observations):
    """
    Perform search on kids_first_server.
    :return: List of resources
    """
    return search_research_studies_with_observations.perform_resources(kids_first_server.server)

@pytest.fixture
def dbgap_research_studies_with_observations(dbgap_server, search_research_studies_with_observations):
    """
    Perform search on dbgap_server.
    :return: List of resources
    """
    return search_research_studies_with_observations.perform_resources(dbgap_server.server)


@pytest.fixture
def all_research_studies_with_observations(aggregated_server, search_research_studies_with_observations):
    """
    Perform search on aggregated_server.
    :return: List of resources
    """
    return search_research_studies_with_observations.perform_resources(aggregated_server.server)


def test_aggregated_server(all_research_studies_with_observations,
    kids_first_research_studies_with_observations,
    anvil_research_studies_with_observations):
    """Aggregation should contain resources from both servers"""
    # print([r.relativePath() for r in anvil_research_studies_with_observations])
    assert len(all_research_studies_with_observations) == len(kids_first_research_studies_with_observations) + len(anvil_research_studies_with_observations), "Sum of studies should be the same."


def test_ResearchStudy_Observations(all_research_studies_with_observations):
    """All ResearchStudies should have a corresponding Observation"""
    all_observations = {r.relativePath():r for r in all_research_studies_with_observations if r.__class__.__name__ == 'Observation'}
    all_research_studies = {r.relativePath():r for r in all_research_studies_with_observations if r.__class__.__name__ == 'ResearchStudy'}
    research_studies_missing_observations = dict(**all_research_studies)
    for observation in all_observations.values():
        del research_studies_missing_observations[observation.focus[0].reference]
    assert len(research_studies_missing_observations) == 0, f"ResearchStudies missing Observations {[r.id for r in research_studies_missing_observations.values()]}"


def _validate_ResearchStudy_Condition(research_studies_with_observations):
    """All ResearchStudies should have a coded condition"""
    research_studies_with_observations = {r.relativePath():r for r in research_studies_with_observations if r.__class__.__name__ == 'ResearchStudy'}
    research_studies_missing_condition = dict(**research_studies_with_observations)
    for research_study in research_studies_with_observations.values():
        if research_study.condition and len(research_study.condition) > 0:
            # print(f"{research_study.relativePath()} has condition {research_study.condition[0].as_json()}")
            del research_studies_missing_condition[research_study.relativePath()]
    # for k in research_studies_missing_condition:
    #     from pprint import pprint
    #     pprint(research_studies_missing_condition[k].as_json())
    #     break
    assert len(research_studies_missing_condition) == 0, f"{len(research_studies_missing_condition)} of {len(research_studies_with_observations)} ResearchStudies missing Condition codeable concept\n{research_studies_missing_condition.keys()}"


def _validate_ResearchStudy_Condition(research_studies_with_observations):
    """All ResearchStudies should have a coded condition"""
    research_studies = {r.relativePath():r for r in research_studies_with_observations if r.__class__.__name__ == 'ResearchStudy'}
    research_studies_missing_condition = dict(**research_studies)
    for research_study in research_studies.values():
        if research_study.condition and len(research_study.condition) > 0:
            # print(f"{research_study.relativePath()} has condition {research_study.condition[0].as_json()}")
            del research_studies_missing_condition[research_study.relativePath()]
    # for k in research_studies_missing_condition:
    #     from pprint import pprint
    #     pprint(research_studies_missing_condition[k].as_json())
    #     break
    assert len(research_studies_missing_condition) == 0, f"{len(research_studies_missing_condition)} of {len(research_studies)} ResearchStudies missing Condition codeable concept\n{research_studies_missing_condition.keys()}"


def test_AggregatedResearchStudy_Condition(all_research_studies_with_observations):
    """All ResearchStudies should have a coded condition"""
    _validate_ResearchStudy_Condition(all_research_studies_with_observations)
    

def test_AnvilResearchStudy_Condition(anvil_research_studies_with_observations):
    """All ResearchStudies should have a coded condition"""
    _validate_ResearchStudy_Condition(anvil_research_studies_with_observations)


def test_KidsFirstResearchStudy_Condition(kids_first_research_studies_with_observations):
    """All ResearchStudies should have a coded condition"""
    _validate_ResearchStudy_Condition(kids_first_research_studies_with_observations)


def test_dbGapResearchStudy_Condition(dbgap_research_studies_with_observations):
    """All ResearchStudies should have a coded condition"""
    _validate_ResearchStudy_Condition(dbgap_research_studies_with_observations)


def _validate_ResearchStudy_Focus(research_studies_with_observations):
    """All ResearchStudies should have a focus"""
    research_studies = {r.relativePath():r for r in research_studies_with_observations if r.__class__.__name__ == 'ResearchStudy'}
    research_studies_missing_focus = dict(**research_studies)
    for research_study in research_studies.values():
        if research_study.focus and len(research_study.focus) > 0:
            # print(f"{research_study.relativePath()} has condition {research_study.condition[0].as_json()}")
            del research_studies_missing_focus[research_study.relativePath()]
    # for k in research_studies_missing_condition:
    #     from pprint import pprint
    #     pprint(research_studies_missing_condition[k].as_json())
    #     break
    assert len(research_studies_missing_focus) == 0, f"{len(research_studies_missing_focus)} of {len(research_studies)} ResearchStudies missing Focus codeable concepts"  # \n{research_studies_missing_focus.keys()}


def test_AggregatedResearchStudy_Focus(all_research_studies_with_observations):
    """All ResearchStudies should have a coded Focus"""
    _validate_ResearchStudy_Focus(all_research_studies_with_observations)
    

def test_AnvilResearchStudy_Focus(anvil_research_studies_with_observations):
    """All ResearchStudies should have a coded Focus"""
    _validate_ResearchStudy_Focus(anvil_research_studies_with_observations)


def test_KidsFirstResearchStudy_Focus(kids_first_research_studies_with_observations):
    """All ResearchStudies should have a coded Focus"""
    _validate_ResearchStudy_Focus(kids_first_research_studies_with_observations)


def test_dbGapResearchStudy_Focus(dbgap_research_studies_with_observations):
    """All ResearchStudies should have a coded Focus"""
    _validate_ResearchStudy_Focus(dbgap_research_studies_with_observations)


def _validateResearchStudy_Source(research_studies_with_observations):
    research_studies = {r.relativePath():r for r in research_studies_with_observations if r.__class__.__name__ == 'ResearchStudy'}
    for research_study in research_studies.values():
        print(research_study.id, research_study.meta.source)
    assert False


def test_dbGapResearchStudy_Source(dbgap_research_studies_with_observations):
    """All ResearchStudies should have a valid source"""
    _validateResearchStudy_Source(dbgap_research_studies_with_observations)


def test_AnvilResearchStudy_Source(anvil_research_studies_with_observations):
    """All ResearchStudies should have a valid source"""
    _validateResearchStudy_Source(anvil_research_studies_with_observations)


def search_research_study_with_subjects(_id):
    """Search clause for all ResearchStudy with Observation"""
    return ResearchStudy.where(struct={'_id': _id}).include('study', ResearchSubject, reverse=True)


def test_kids_first_research_study_with_subjects(kids_first_server, kids_first_research_studies_with_observations):
    """
    Perform search on aggregated_server.
    :return: List of resources
    """
    # pick the first ResearchStudy
    research_studies = {r.relativePath():r for r in kids_first_research_studies_with_observations if r.__class__.__name__ == 'ResearchStudy'}
    research_study = list(research_studies.values())[0]
    # get the list of subjects, extract Patient ids
    study_with_subjects = search_research_study_with_subjects(research_study.id).perform_resources(kids_first_server.server)
    subjects = {r.relativePath():r for r in study_with_subjects if r.__class__.__name__ == 'ResearchSubject'}
    patient_ids = [subjects[s].individual.reference.split('/')[-1] for s in subjects]
    patients = []
    for patient_id in patient_ids:
        patients.append(Bundle.read_from(f"Patient/{patient_id}/$everything", kids_first_server.server))
    print(patients[0].as_json())
    assert False


# 'https://kf-api-fhir-service.kidsfirstdrc.org/ResearchStudy?_id=76758&_revinclude=ResearchSubject:study&_summary=text'
# 'https://kf-api-fhir-service.kidsfirstdrc.org/Patient/21953/$everything?_summary=text'


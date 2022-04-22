"""
These tests explore data in dbGAP, AnVIL and Kids First
- they are designed to find corner cases in the way the data was encoded.
- as such they will fail
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
def aggregated_server(token, kids_first_server):
    """Create FHIRClient for AnVIL and Kids First servers."""
    settings = {
        'app_id': __name__,
        'api_bases': [
            'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir',
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
def kids_first_research_studies_with_observations(caplog, kids_first_server, search_research_studies_with_observations):
    """
    Perform search on kids_first_server.
    :return: List of resources
    """
    # caplog.set_level(logging.DEBUG)
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
    assert len(all_research_studies_with_observations) == len(kids_first_research_studies_with_observations) + len(
        anvil_research_studies_with_observations), "Sum of studies should be the same."


def test_ResearchStudy_Observations(all_research_studies_with_observations):
    """All ResearchStudies should have a corresponding Observation"""
    all_observations = {r.relativePath(): r for r in all_research_studies_with_observations if
                        r.__class__.__name__ == 'Observation'}
    all_research_studies = {r.relativePath(): r for r in all_research_studies_with_observations if
                            r.__class__.__name__ == 'ResearchStudy'}
    research_studies_missing_observations = dict(**all_research_studies)
    for observation in all_observations.values():
        del research_studies_missing_observations[observation.focus[0].reference]
    assert len(
        research_studies_missing_observations) == 0, f"ResearchStudies missing Observations {[r.id for r in research_studies_missing_observations.values()]}"


def _validate_ResearchStudy_Condition(research_studies_with_observations):
    """All ResearchStudies should have a coded condition"""
    research_studies_with_observations = {r.relativePath(): r for r in research_studies_with_observations if
                                          r.__class__.__name__ == 'ResearchStudy'}
    research_studies_missing_condition = dict(**research_studies_with_observations)
    for research_study in research_studies_with_observations.values():
        if research_study.condition and len(research_study.condition) > 0:
            # print(f"{research_study.relativePath()} has condition {research_study.condition[0].as_json()}")
            del research_studies_missing_condition[research_study.relativePath()]
    # for k in research_studies_missing_condition:
    #     from pprint import pprint
    #     pprint(research_studies_missing_condition[k].as_json())
    #     break
    assert len(
        research_studies_missing_condition) == 0, f"{len(research_studies_missing_condition)} of {len(research_studies_with_observations)} ResearchStudies missing Condition codeable concept\n{research_studies_missing_condition.keys()}"


def _validate_ResearchStudy_Condition(research_studies_with_observations):
    """All ResearchStudies should have a coded condition"""
    research_studies = {r.relativePath(): r for r in research_studies_with_observations if
                        r.__class__.__name__ == 'ResearchStudy'}
    research_studies_missing_condition = dict(**research_studies)
    for research_study in research_studies.values():
        if research_study.condition and len(research_study.condition) > 0:
            # print(f"{research_study.relativePath()} has condition {research_study.condition[0].as_json()}")
            del research_studies_missing_condition[research_study.relativePath()]
    # for k in research_studies_missing_condition:
    #     from pprint import pprint
    #     pprint(research_studies_missing_condition[k].as_json())
    #     break```
    assert len(
        research_studies_missing_condition) == 0, f"{len(research_studies_missing_condition)} of {len(research_studies)} ResearchStudies missing Condition codeable concept\n{research_studies_missing_condition.keys()}"


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
    research_studies = {r.relativePath(): r for r in research_studies_with_observations if
                        r.__class__.__name__ == 'ResearchStudy'}
    research_studies_missing_focus = dict(**research_studies)
    for research_study in research_studies.values():
        if research_study.focus and len(research_study.focus) > 0:
            # print(f"{research_study.relativePath()} has condition {research_study.condition[0].as_json()}")
            del research_studies_missing_focus[research_study.relativePath()]
    # for k in research_studies_missing_condition:
    #     from pprint import pprint
    #     pprint(research_studies_missing_condition[k].as_json())
    #     break
    assert len(
        research_studies_missing_focus) == 0, f"{len(research_studies_missing_focus)} of {len(research_studies)} ResearchStudies missing Focus codeable concepts"  # \n{research_studies_missing_focus.keys()}


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


def search_research_study_with_subjects(_id):
    """Search clause for all ResearchStudy with Observation"""
    return ResearchStudy.where(struct={'_id': _id}).include('study', ResearchSubject, reverse=True)


def _validate_patient_everything(caplog, fhir_client, research_studies_with_observations):
    assert research_studies_with_observations, 'Should be non-null'
    assert len(research_studies_with_observations) > 0, 'Should have at least one ResearchStudy'
    # pick the first ResearchStudy
    research_studies = {r.relativePath(): r for r in research_studies_with_observations if
                        r.__class__.__name__ == 'ResearchStudy'}
    research_study = list(research_studies.values())[0]
    # get the list of subjects, extract Patient ids
    study_with_subjects = search_research_study_with_subjects(research_study.id).perform_resources(fhir_client.server)
    subjects = {r.relativePath(): r for r in study_with_subjects if r.__class__.__name__ == 'ResearchSubject'}
    patient_ids = [subjects[s].individual.reference.split('/')[-1] for s in subjects]
    patient_bundles = []
    caplog.set_level(logging.DEBUG)
    desired_types = ['ResearchStudy', 'ResearchSubject', 'Patient', 'Specimen', 'Task', 'DocumentReference']
    # KF: ['Patient', 'DocumentReference', 'Specimen', 'ResearchSubject', 'ResearchStudy', 'Practitioner', 'Organization', 'PractitionerRole']
    # Anvil: ['Patient', 'DocumentReference', 'Specimen', 'ResearchSubject']

    for patient_id in patient_ids:
        print(patient_id)
        patient_bundles.append(
            Bundle.read_from(f"Patient/{patient_id}/$everything?_type={','.join(desired_types)}", fhir_client.server))
        break
    resource_counts = {}
    for patient_bundle in patient_bundles:
        resource_counts = defaultdict(int)
        for entry in patient_bundle.entry:
            resource_counts[entry.resource.resource_type] += 1
    for resource_type in desired_types:
        assert resource_type in resource_counts, f"{resource_type} should be in $everything {resource_counts}"
    return resource_counts


def test_kids_first_patient_everything(caplog, kids_first_server, kids_first_research_studies_with_observations):
    """
    Test that a Patient/XXX/$everything returns a set of resources.
    """
    resource_counts = _validate_patient_everything(caplog, kids_first_server,
                                                   kids_first_research_studies_with_observations)
    assert False, resource_counts


def test_anvil_first_patient_everything(caplog, anvil_server, anvil_research_studies_with_observations):
    """
    Test that a Patient/XXX/$everything returns a set of resources.
    """
    research_studies = {r.relativePath(): r for r in anvil_research_studies_with_observations if
                        r.__class__.__name__ == 'ResearchStudy'}
    resource_counts = _validate_patient_everything(caplog, anvil_server,
                                                   [research_studies['ResearchStudy/1000G-high-coverage-2019']])
    assert False, resource_counts


def search_research_subjects_with_patients(study_id):
    """Search clause for all ResearchStudy with Observation"""
    return ResearchSubject.where(struct={'study': f'ResearchStudy/{study_id}'}).include('individual').include('study')


def validate_research_subjects_with_patients(caplog, search, fhir_client):
    """
    Test search on client.server
    """
    from collections import defaultdict
    # caplog.set_level(logging.DEBUG)
    research_subjects_with_patients = search.perform_resources(fhir_client.server)
    print(research_subjects_with_patients)
    resource_types = defaultdict(int)
    for r in research_subjects_with_patients:
        resource_types[r.resource_type] += 1
    assert resource_types['ResearchSubject'] > 1, f"Should have at least one ResearchSubject {resource_types}"
    assert resource_types['ResearchSubject'] == resource_types[
        'Patient'], f"Should have equal number of patients and subjects f{resource_types}"
    for r in research_subjects_with_patients:
        if r.resource_type == 'ResearchStudy':
            print(r.id)
    assert resource_types['ResearchStudy'] > 0, f"Should have at least one ResearchStudy {resource_types}"


def test_dbgap_research_subjects_with_patients(caplog, dbgap_server):
    """
    Test search on dbgap_server.
    """
    validate_research_subjects_with_patients(caplog, search_research_subjects_with_patients('phs002409'), dbgap_server)


def test_kf_research_subjects_with_patients(caplog, kids_first_server):
    """
    Test search on dbgap_server.
    """
    validate_research_subjects_with_patients(caplog, search_research_subjects_with_patients('281300'),
                                             kids_first_server)


def test_anvil_research_subjects_with_patients(caplog, anvil_server):
    """
    Test ResearchStudy search on anvil
    """
    validate_research_subjects_with_patients(caplog, search_research_subjects_with_patients('1000G-high-coverage-2019'),
                                             anvil_server)


def _validateResearchStudy_tag_fullUrl(research_studies_with_observations):
    """Validate .meta.tag has a fullUrl code."""
    research_studies = {r.relativePath(): r for r in research_studies_with_observations if
                        r.__class__.__name__ == 'ResearchStudy'}
    for research_study in research_studies.values():
        has_fullUrl_tag = False
        for tag in research_study.meta.tag:
            if tag['system'] != "https://nih-ncpi.github.io/ncpi-fhir-ig/#fullUrl":
                continue
            url_parts = urlparse(tag['code'])
            assert all([url_parts.scheme, url_parts.netloc]), f"{tag} code should be a valid url"
            has_fullUrl_tag = True
        assert has_fullUrl_tag, f"Should contain fullURL tag {research_study.meta.tags}"


def test_dbGapResearchStudy_tag_fullUrl(dbgap_research_studies_with_observations):
    """All ResearchStudies should have a valid source"""
    _validateResearchStudy_tag_fullUrl(dbgap_research_studies_with_observations)


def test_AnvilResearchStudy_tag_fullUrl(anvil_research_studies_with_observations):
    """All ResearchStudies should have a valid source"""
    _validateResearchStudy_tag_fullUrl(anvil_research_studies_with_observations)


def test_KidsFirstResearchStudy_tag_fullUrl(kids_first_research_studies_with_observations):
    """All ResearchStudies should have a valid source"""
    _validateResearchStudy_tag_fullUrl(kids_first_research_studies_with_observations)


def test_AllResearchStudy_tag_fullUrl(all_research_studies_with_observations):
    """All ResearchStudies should have a url source, used to disambiguate combined results."""
    research_studies = [r for r in all_research_studies_with_observations if r.__class__.__name__ == 'ResearchStudy']
    sources = defaultdict(int)
    for research_study in research_studies:
        url_parts = urlparse(research_study.meta.source)
        assert all([url_parts.scheme, url_parts.netloc]), f"{research_study.meta.source} should be a valid url"
        sources[url_parts.netloc] += 1
    assert len(sources) > 1, f"Should have more than one source in a combined result {sources}"


def test_simple_kf(kids_first_server):
    studies = ResearchStudy.where(struct={'_count': '1'}).perform_resources(kids_first_server.server)
    assert len(studies) == 1


def test_simple_anvil(anvil_server):
    studies = ResearchStudy.where(struct={'_count': '1'}).perform_resources(anvil_server.server)
    assert len(studies) == 1


def test_simple_dbgap(dbgap_server):
    studies = ResearchStudy.where(struct={'_count': '1'}).perform_resources(dbgap_server.server)
    assert len(studies) == 1

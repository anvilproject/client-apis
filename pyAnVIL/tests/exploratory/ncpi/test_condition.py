
from fhirclient.models.condition import Condition
from fhirclient.models.researchstudy import ResearchStudy
from collections import defaultdict
import pytest
import json
from os.path import exists

EXPECTED_CONDITION_CODE_SYSTEMS = ['http://purl.obolibrary.org/obo/hp.owl', 'http://purl.obolibrary.org/obo/mondo.owl', 'http://snomed.info/sct', 'http://purl.obolibrary.org/obo/ncit.owl', 'https://www.who.int/classifications/classification-of-diseases']

def _conditions(client):
    """
    Perform search on client's server .
    :return: List of Conditions
    """
    return Condition.where(struct={'_count':'1000'}).perform_resources(client.server)


@pytest.fixture
def kids_first_conditions(kids_first_client):
    """
    Perform search on kids_first .
    :return: List of Conditions
    """
    if exists("kids_first_conditions.json"):
        with open("kids_first_conditions.json", "r") as input:
            conditions = []
            for line in input:
                conditions.append(Condition(json.loads(line)))
            return conditions
    conditions = _conditions(kids_first_client)
    with open("kids_first_conditions.json", "w") as output:
        for condition in kids_first_conditions:
            json.dump(condition.as_json(), output, separators=(',', ':'))
            output.write("\n")
    return conditions


def _validate_kidsfirst_conditions(kids_first_conditions):
    """There should be many Conditions."""
    assert kids_first_conditions, "Should return conditions"
    assert len(kids_first_conditions) > 0, "Should return conditions"


# def test_kidsfirst_conditions(kids_first_conditions):
#     """There should be many Conditions."""
#     _validate_kidsfirst_conditions(kids_first_conditions)


# def test_kidsfirst_condition_missing_coding(kids_first_conditions):
#     """All conditions should have a coding."""
#     _validate_kidsfirst_conditions(kids_first_conditions)
#     missing_coding = defaultdict(int)
#     for condition in kids_first_conditions:
#         if not condition.code.coding:
#             missing_coding[f"Condition/{condition.id}"] += 1
#     assert len(missing_coding) == 0, f"All conditions should have a coding\n{missing_coding}"


# def test_kidsfirst_condition_systems(kids_first_conditions):
#     """There should be many a limited set of systems."""
#     _validate_kidsfirst_conditions(kids_first_conditions)
#     systems = defaultdict(int) 
#     for condition in kids_first_conditions:
#         if not condition.code.coding:
#             continue
#         for coding in condition.code.coding:
#             systems[coding.system] += 1
#         assert coding.system in EXPECTED_CONDITION_CODE_SYSTEMS, f"{coding.system} is an unexpected code system" 


# def test_kidsfirst_condition_systems(kids_first_conditions):
#     """There should be many a limited set of systems."""
#     _validate_kidsfirst_conditions(kids_first_conditions)
#     systems = defaultdict(int) 

#     systems = defaultdict(int) 
#     codes = defaultdict(int)
#     prefixes = defaultdict(int)
#     missing_coding = defaultdict(int)
#     for condition in kids_first_conditions:
#         if not condition.code.coding:
#             missing_coding[f"Condition/{condition.id}"] += 1
#             continue
#         for coding in condition.code.coding:
#             codes[coding.code] += 1
#             systems[coding.system] += 1
#             prefixes[coding.code.split(':')[0]] += 1
#     print(systems)
#     print(codes)
#     print(prefixes)
#     print(missing_coding)


@pytest.fixture
def dbgap_conditions(dbgap_client):
    """
    Perform search on kids_first .
    :return: List of Conditions
    """
    if exists("dbgap_conditions.json"):
        with open("dbgap_conditions.json", "r") as input:
            conditions = []
            for line in input:
                conditions.append(Condition(json.loads(line)))
            return conditions
    conditions = _conditions(dbgap_client)
    with open("dbgap_conditions.json", "w") as output:
        for condition in conditions:
            json.dump(condition.as_json(), output, separators=(',', ':'))
            output.write("\n")
    return conditions


def _validate_dbgap_conditions(dbgap_conditions):
    """There should be many Conditions."""
    assert dbgap_conditions, "Should return conditions"
    assert len(dbgap_conditions) > 0, "Should return conditions"


# def test_dbgap_conditions(dbgap_conditions):
#     """There should be many Conditions."""
#     _validate_dbgap_conditions(dbgap_conditions)


def _study_condition_systems(studies):
    systems = defaultdict(set)
    for research_study in studies:
        if research_study.resource_type is not "ResearchStudy":
            continue
        research_study_id = f"ResearchStudy/{research_study.id}"
        if not research_study.condition:
             systems[None].add(research_study_id)
             continue
        for condition in research_study.condition:
            if not condition.coding:
                systems[None].add(research_study_id)
                continue
            for coding in condition.coding:
                systems[coding.system].add(research_study_id)
    return systems


@pytest.fixture
def dbgap_study_condition_systems(dbgap_research_studies_with_observations):
    """There should be many a limited set of systems."""
    return _study_condition_systems(dbgap_research_studies_with_observations)


@pytest.fixture
def kidsfirst_study_condition_systems(kids_first_research_studies_with_observations):
    return _study_condition_systems(kids_first_research_studies_with_observations)


@pytest.fixture
def anvil_study_condition_systems(anvil_research_studies_with_observations):
    return _study_condition_systems(anvil_research_studies_with_observations)

# def test_condition_coding(kidsfirst_study_condition_systems, dbgap_study_condition_systems, anvil_study_condition_systems):
#     """We should use the same ontologies.
#     While all three systems use ResearchStudy.condition to indicate the condition that is the focus of the study. 
#     All three have a significant number of studies without a populated `condition`.
#     In addition, all three systems use different ontologies:
#     * kidsfirst ['http://snomed.info/sct', None]
#     * dbgap ['urn:oid:2.16.840.1.113883.6.177', 'https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/NamingSystem/MeshEntryTerm', 'https://uts.nlm.nih.gov/metathesaurus.html', None]
#     * anvil ['http://purl.obolibrary.org/obo/doid.owl', None]
#     """

#     from pprint import pprint
#     pprint({'kidsfirst': kidsfirst_study_condition_systems.keys()})
#     pprint({'dbgap': dbgap_study_condition_systems.keys()})
#     pprint({'anvil': anvil_study_condition_systems.keys()})

#     # only show the length of studies that match
#     for s in [kidsfirst_study_condition_systems, dbgap_study_condition_systems, anvil_study_condition_systems]:
#         for k in s:
#             s[k] = len(s[k])

#     pprint({'kidsfirst': kidsfirst_study_condition_systems})
#     pprint({'dbgap': dbgap_study_condition_systems})
#     pprint({'anvil': anvil_study_condition_systems})

#     kidsfirst = set(kidsfirst_study_condition_systems.keys())
#     dbgap = set(dbgap_study_condition_systems.keys())
#     anvil = set(anvil_study_condition_systems.keys())
#     assert kidsfirst.issubset(dbgap) and kidsfirst.issubset(anvil), f"{kidsfirst} is not a subset of {dbgap} {anvil}"

def test_anvil_condition(anvil_research_studies_with_observations):
    from pprint import pprint
    pprint(anvil_research_studies_with_observations)
    assert False
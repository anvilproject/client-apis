import json

import requests
import os
import pytest

from anvil.etl.utilities.shell_helper import run_cmd


@pytest.fixture(scope="session")
def token():
    return os.environ.get('TOKEN', run_cmd("gcloud auth application-default print-access-token"))


@pytest.fixture
def base_api():
    default = 'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/dev/fhir'
    return os.environ.get('BASE_API', default)


def test_implementation_guide(token, base_api):

    url = f'{base_api}/ImplementationGuide'

    headers = {
        "Content-Type": "application/fhir+json;charset=utf-8",
        "Authorization": f"Bearer {token}",
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    print(json.dumps(response.json()))
    assert response.json()['entry'][0]['resource']['id'] == 'NCPI-FHIR-Implementation-Guide'


def test_document_reference_invalid_body_no_subject(token, base_api):
    validate_url = f'{base_api}/DocumentReference/$validate'

    headers = {
        "Content-Type": "application/fhir+json;charset=utf-8",
        "Authorization": f"Bearer {token}",
    }

    # missing subject
    invalid_body_no_subject = {
        "resourceType": "DocumentReference",
        "id": "44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f",
        "meta": {
            "profile": [
                "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/ncpi-research-document-reference"
            ]
        },
        "identifier": [
            {
                "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/1000G-high-coverage-2019",
                "value": "gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_14151/Project_CCDG_14151_B01_GRM_WGS.cram.2020-02-12/Sample_HG00405/analysis/HG00405.final.cram"
            },
            {
                "system": "urn:ncpi:unique-string",
                "value": "44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f"
            }
        ],
        "status": "current",
        "custodian": {
            "reference": "Organization/1000G-high-coverage-2019"
        },
        "content": [
            {
                "attachment": {
                    "url": "foo://dg.ANV0/a9a4489b-de29-4589-9409-4767860e8e85"
                },
                "format": {
                    "display": "cram"
                }
            }
        ],
        "context": {
            "related": [
                {
                    "reference": "Task/d0767fea-d6f6-5482-be12-9260e6901c4f"
                }
            ]
        }
    }
    response = requests.post(validate_url, json=invalid_body_no_subject, headers=headers)
    response.raise_for_status()
    issues = response.json()['issue']
    errors = [issue['expression'][0] for issue in issues if issue['severity'] == 'error']
    assert 'DocumentReference.subject' in errors, f"Should have raised a 'invalid number of elements, min is 1, got 0' {issues}. To see the expected errors try `java -jar validator_cli.jar  /tmp/invalid_body_no_subject.json -ig ~/client-apis/pyAnVIL/DATA/fhir/IG/` "
    print("invalid_body_no_subject OK")


def test_document_reference_missing_task(token, base_api):
    validate_url = f'{base_api}/DocumentReference/$validate'

    headers = {
        "Content-Type": "application/fhir+json;charset=utf-8",
        "Authorization": f"Bearer {token}",
    }

    # missing task
    invalid_body_no_task = {"resourceType": "DocumentReference", "id": "44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f", "meta": {
        "profile": ["http://fhir.ncpi-project-forge.io/StructureDefinition/ncpi-drs-document-reference"]},
                            "identifier": [{
                                               "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/1000G-high-coverage-2019",
                                               "value": "gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_14151/Project_CCDG_14151_B01_GRM_WGS.cram.2020-02-12/Sample_HG00405/analysis/HG00405.final.cram"},
                                           {"system": "urn:ncpi:unique-string",
                                            "value": "44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f"}], "status": "current",
                            "custodian": {"reference": "Organization/1000G-high-coverage-2019"},
                            "subject": {"reference": "Patient/e87f0936-319b-5b86-bc27-3e3fdde77c29"}, "content": [
            {"attachment": {"url": "drs://dg.ANV0/a9a4489b-de29-4589-9409-4767860e8e85"},
             "format": {"display": "cram"}}]}
    response = requests.post(validate_url, json=invalid_body_no_task, headers=headers)
    response.raise_for_status()
    issues = response.json()['issue']
    errors = [issue['expression'][0] for issue in issues if issue['severity'] == 'error']
    assert 'DocumentReference.subject' in errors, f"Should have raised a 'invalid number of elements, min is 1, got 0' {issues}. To see the expected errors try `java -jar validator_cli.jar  /tmp/invalid_body_no_subject.json -ig ~/client-apis/pyAnVIL/DATA/fhir/IG/` "
    print("invalid_body_no_task OK")


def test_document_reference_invalid_body_no_custodian(token, base_api):
    validate_url = f'{base_api}/DocumentReference/$validate'

    headers = {
        "Content-Type": "application/fhir+json;charset=utf-8",
        "Authorization": f"Bearer {token}",
    }
    # missing custodian
    invalid_body_no_custodian = {"resourceType": "DocumentReference", "id": "44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f",
                                 "meta": {"profile": [
                                     "http://fhir.ncpi-project-forge.io/StructureDefinition/ncpi-drs-document-reference"]},
                                 "identifier": [{
                                                    "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/1000G-high-coverage-2019",
                                                    "value": "gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_14151/Project_CCDG_14151_B01_GRM_WGS.cram.2020-02-12/Sample_HG00405/analysis/HG00405.final.cram"},
                                                {"system": "urn:ncpi:unique-string",
                                                 "value": "44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f"}], "status": "current",
                                 "subject": {"reference": "Patient/e87f0936-319b-5b86-bc27-3e3fdde77c29"}, "content": [
            {"attachment": {"url": "foo://dg.ANV0/a9a4489b-de29-4589-9409-4767860e8e85"},
             "format": {"display": "cram"}}],
                                 "context": {"related": [{"reference": "Task/d0767fea-d6f6-5482-be12-9260e6901c4f"}]}}
    response = requests.post(validate_url, json=invalid_body_no_custodian, headers=headers)
    response.raise_for_status()
    issues = response.json()['issue']
    errors = [issue['expression'][0] for issue in issues if issue['severity'] == 'error']
    assert 'DocumentReference.custodian' in errors, f"Should have raised a 'invalid number of elements, min is 1, got 0' {issues}"


def test_document_reference_valid_body(token, base_api):
    validate_url = f'{base_api}/DocumentReference/$validate'

    headers = {
        "Content-Type": "application/fhir+json;charset=utf-8",
        "Authorization": f"Bearer {token}",
    }

    # all OK
    valid_body = {"resourceType": "DocumentReference", "id": "44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f", "meta": {
        "profile": ["http://fhir.ncpi-project-forge.io/StructureDefinition/ncpi-drs-document-reference"]},
                  "identifier": [
                      {"system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/1000G-high-coverage-2019",
                       "value": "gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_14151/Project_CCDG_14151_B01_GRM_WGS.cram.2020-02-12/Sample_HG00405/analysis/HG00405.final.cram"},
                      {"system": "urn:ncpi:unique-string", "value": "44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f"}],
                  "status": "current", "custodian": {"reference": "Organization/1000G-high-coverage-2019"},
                  "subject": {"reference": "Patient/e87f0936-319b-5b86-bc27-3e3fdde77c29"}, "content": [
            {"attachment": {"url": "drs://dg.ANV0/a9a4489b-de29-4589-9409-4767860e8e85"},
             "format": {"display": "cram"}}],
                  "context": {"related": [{"reference": "Task/d0767fea-d6f6-5482-be12-9260e6901c4f"}]}}
    response = requests.post(validate_url, json=valid_body, headers=headers)
    response.raise_for_status()
    issues = response.json()['issue']
    errors = [issue for issue in issues if issue['severity'] == 'error']
    assert len(errors) == 0, f"Should not have any errors {errors}"
    # https://github.com/FirelyTeam/firely-net-sdk/issues/1055
    warnings = [issue for issue in issues if issue['severity'] == 'warning' and issue['code'] != 'informational']
    assert len(warnings) == 0, f"Should not have any warnings {warnings}"


def test_document_reference_attachment(token, base_api):
    validate_url = f'{base_api}/DocumentReference/$validate'
    invalid_body_bad_attachment = {"resourceType": "DocumentReference", "id": "44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f",
                                   "meta": {"profile": [
                                       "http://fhir.ncpi-project-forge.io/StructureDefinition/ncpi-drs-document-reference"]},
                                   "identifier": [{
                                                      "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/1000G-high-coverage-2019",
                                                      "value": "gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_14151/Project_CCDG_14151_B01_GRM_WGS.cram.2020-02-12/Sample_HG00405/analysis/HG00405.final.cram"},
                                                  {"system": "urn:ncpi:unique-string",
                                                   "value": "44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f"}],
                                   "status": "current",
                                   "custodian": {"reference": "Organization/1000G-high-coverage-2019"},
                                   "subject": {"reference": "Patient/e87f0936-319b-5b86-bc27-3e3fdde77c29"},
                                   "content": [
                                       {"attachment": {"url": "foo://dg.ANV0/a9a4489b-de29-4589-9409-4767860e8e85"},
                                        "format": {"display": "cram"}}],
                                   "context": {"related": [{"reference": "Task/d0767fea-d6f6-5482-be12-9260e6901c4f"}]}}

    headers = {
        "Content-Type": "application/fhir+json;charset=utf-8",
        "Authorization": f"Bearer {token}",
    }

    # bad attachment
    response = requests.post(validate_url, json=invalid_body_bad_attachment, headers=headers)
    response.raise_for_status()
    issues = response.json()['issue']
    errors = [issue['expression'][0] for issue in issues if issue['severity'] == 'error']
    assert 'DocumentReference.attachment' in errors, f"Should have raised an invalid url {issues}\nExpected error for Google FHIR service.  see https://groups.google.com/g/gcp-healthcare-discuss/c/dOKuFXqPlXo for more"


def test_patient(token, base_api):
    validate_url = f'{base_api}/Patient/$validate'

    valid_body = {"resourceType": "Patient", "id": "e87f0936-319b-5b86-bc27-3e3fdde77c29",
                  "meta": {"profile": ["http://hl7.org/fhir/StructureDefinition/Patient"]}, "identifier": [
            {"system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/1000G-high-coverage-2019",
             "value": "ERS4367795"},
            {"system": "urn:ncpi:unique-string", "value": "1000G-high-coverage-2019/Patient/ERS4367795"}],
                  "managingOrganization": {"reference": "Organization/1000G-high-coverage-2019"}}

    headers = {
        "Content-Type": "application/fhir+json;charset=utf-8",
        "Authorization": f"Bearer {token}",
    }

    # all OK
    response = requests.post(validate_url, json=valid_body, headers=headers)
    response.raise_for_status()
    issues = response.json()['issue']
    errors = [issue for issue in issues if issue['severity'] == 'error']
    assert len(errors) == 0, f"Should not have any errors {errors}"
    warnings = [issue for issue in issues if issue['severity'] == 'warning']
    assert len(warnings) == 0, f"Should not have any warnings {warnings}"

    # TODO - if we implement it in schema
    # # missing managingOrganization
    # valid_body_managingOrganization = {"resourceType":"Patient","id":"e87f0936-319b-5b86-bc27-3e3fdde77c29","meta":{"profile":["http://hl7.org/fhir/StructureDefinition/Patient"]},"identifier":[{"system":"https://anvil.terra.bio/#workspaces/anvil-datastorage/1000G-high-coverage-2019","value":"ERS4367795"},{"system":"urn:ncpi:unique-string","value":"1000G-high-coverage-2019/Patient/ERS4367795"}]}
    # response = requests.post(validate_url, json=valid_body_managingOrganization, headers=headers)
    # response.raise_for_status()
    # issues = response.json()['issue']
    # errors = [issue for issue in issues if issue['severity'] == 'error']
    # assert len(errors) > 0, f"Should have at least 1 error {errors}"
    # warnings = [issue for issue in issues if issue['severity'] == 'warning']
    # assert len(warnings) == 0, f"Should not have any warnings {warnings}"


def test_task(token, base_api):
    validate_url = f'{base_api}/Task/$validate'

    headers = {
        "Content-Type": "application/fhir+json;charset=utf-8",
        "Authorization": f"Bearer {token}",
    }

    # all OK
    valid_body = {"resourceType": "Task", "id": "d0767fea-d6f6-5482-be12-9260e6901c4f", "meta": {
        "profile": ["https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/ncpi-specimen-task"]}, "identifier": [
        {"system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/1000G-high-coverage-2019",
         "value": "ERS4367795/Task/AnVILInjest"}, {"system": "urn:ncpi:unique-string",
                                                   "value": "1000G-high-coverage-2019/Patient/ERS4367795/Specimen/ERS4367795/Task/AnVILInjest"}],
                  "status": "accepted", "intent": "unknown", "input": [{"type": {"coding": [{"code": "Specimen"}]},
                                                                        "valueReference": {
                                                                            "reference": "Specimen/e87f0936-319b-5b86-bc27-3e3fdde77c29"}}],
                  "output": [{"type": {"coding": [{"code": "DocumentReference"}]}, "valueReference": {
                      "reference": "DocumentReference/44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f"}},
                             {"type": {"coding": [{"code": "DocumentReference"}]}, "valueReference": {
                                 "reference": "DocumentReference/ed123e4e-f8f2-565c-a7f6-c492c9c4ab60"}},
                             {"type": {"coding": [{"code": "DocumentReference"}]}, "valueReference": {
                                 "reference": "DocumentReference/ccb094a2-567b-52dd-92e3-5f16b3b9f7da"}}],
                  "focus": {"reference": "Specimen/e87f0936-319b-5b86-bc27-3e3fdde77c29"},
                  "for": {"reference": "Patient/e87f0936-319b-5b86-bc27-3e3fdde77c29"},
                  "owner": {"reference": "Organization/1000G-high-coverage-2019"}}
    response = requests.post(validate_url, json=valid_body, headers=headers)
    response.raise_for_status()
    issues = response.json()['issue']
    errors = [issue for issue in issues if issue['severity'] == 'error']
    assert len(errors) == 0, f"Should not have any errors {errors}"
    warnings = [issue for issue in issues if issue['severity'] == 'warning']
    assert len(warnings) == 0, f"Should not have any warnings {warnings}"

    # no focus
    invalid_body_no_focus = {"resourceType": "Task", "id": "d0767fea-d6f6-5482-be12-9260e6901c4f", "meta": {
        "profile": ["https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/ncpi-specimen-task"]}, "identifier": [
        {"system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/1000G-high-coverage-2019",
         "value": "ERS4367795/Task/AnVILInjest"}, {"system": "urn:ncpi:unique-string",
                                                   "value": "1000G-high-coverage-2019/Patient/ERS4367795/Specimen/ERS4367795/Task/AnVILInjest"}],
                             "status": "accepted", "intent": "unknown", "input": [
            {"type": {"coding": [{"code": "Specimen"}]},
             "valueReference": {"reference": "Specimen/e87f0936-319b-5b86-bc27-3e3fdde77c29"}}], "output": [
            {"type": {"coding": [{"code": "DocumentReference"}]},
             "valueReference": {"reference": "DocumentReference/44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f"}},
            {"type": {"coding": [{"code": "DocumentReference"}]},
             "valueReference": {"reference": "DocumentReference/ed123e4e-f8f2-565c-a7f6-c492c9c4ab60"}},
            {"type": {"coding": [{"code": "DocumentReference"}]},
             "valueReference": {"reference": "DocumentReference/ccb094a2-567b-52dd-92e3-5f16b3b9f7da"}}],
                             "for": {"reference": "Patient/e87f0936-319b-5b86-bc27-3e3fdde77c29"},
                             "owner": {"reference": "Organization/1000G-high-coverage-2019"}}
    response = requests.post(validate_url, json=invalid_body_no_focus, headers=headers)
    response.raise_for_status()
    issues = response.json()['issue']
    errors = [issue['expression'][0] for issue in issues if issue['severity'] == 'error']
    assert 'Task.focus' in errors, f"Should have raised missing focus {issues}"
    warnings = [issue for issue in issues if issue['severity'] == 'warning']
    assert len(warnings) == 0, f"Should not have any warnings {warnings}"

    # no for
    invalid_body_no_for = {"resourceType": "Task", "id": "d0767fea-d6f6-5482-be12-9260e6901c4f", "meta": {
        "profile": ["https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/ncpi-specimen-task"]}, "identifier": [
        {"system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/1000G-high-coverage-2019",
         "value": "ERS4367795/Task/AnVILInjest"}, {"system": "urn:ncpi:unique-string",
                                                   "value": "1000G-high-coverage-2019/Patient/ERS4367795/Specimen/ERS4367795/Task/AnVILInjest"}],
                           "status": "accepted", "intent": "unknown", "input": [
            {"type": {"coding": [{"code": "Specimen"}]},
             "valueReference": {"reference": "Specimen/e87f0936-319b-5b86-bc27-3e3fdde77c29"}}], "output": [
            {"type": {"coding": [{"code": "DocumentReference"}]},
             "valueReference": {"reference": "DocumentReference/44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f"}},
            {"type": {"coding": [{"code": "DocumentReference"}]},
             "valueReference": {"reference": "DocumentReference/ed123e4e-f8f2-565c-a7f6-c492c9c4ab60"}},
            {"type": {"coding": [{"code": "DocumentReference"}]},
             "valueReference": {"reference": "DocumentReference/ccb094a2-567b-52dd-92e3-5f16b3b9f7da"}}],
                           "focus": {"reference": "Specimen/e87f0936-319b-5b86-bc27-3e3fdde77c29"},
                           "owner": {"reference": "Organization/1000G-high-coverage-2019"}}
    response = requests.post(validate_url, json=invalid_body_no_for, headers=headers)
    response.raise_for_status()
    issues = response.json()['issue']
    errors = [issue['expression'][0] for issue in issues if issue['severity'] == 'error']
    assert 'Task.for' in errors, f"Should have raised missing focus {issues}"
    warnings = [issue for issue in issues if issue['severity'] == 'warning']
    assert len(warnings) == 0, f"Should not have any warnings {warnings}"

    # no owner
    invalid_body_no_owner = {"resourceType": "Task", "id": "d0767fea-d6f6-5482-be12-9260e6901c4f", "meta": {
        "profile": ["https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/ncpi-specimen-task"]}, "identifier": [
        {"system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/1000G-high-coverage-2019",
         "value": "ERS4367795/Task/AnVILInjest"}, {"system": "urn:ncpi:unique-string",
                                                   "value": "1000G-high-coverage-2019/Patient/ERS4367795/Specimen/ERS4367795/Task/AnVILInjest"}],
                             "status": "accepted", "intent": "unknown", "input": [
            {"type": {"coding": [{"code": "Specimen"}]},
             "valueReference": {"reference": "Specimen/e87f0936-319b-5b86-bc27-3e3fdde77c29"}}], "output": [
            {"type": {"coding": [{"code": "DocumentReference"}]},
             "valueReference": {"reference": "DocumentReference/44a58180-e4ff-5a8c-9a1f-db4a76ce6f1f"}},
            {"type": {"coding": [{"code": "DocumentReference"}]},
             "valueReference": {"reference": "DocumentReference/ed123e4e-f8f2-565c-a7f6-c492c9c4ab60"}},
            {"type": {"coding": [{"code": "DocumentReference"}]},
             "valueReference": {"reference": "DocumentReference/ccb094a2-567b-52dd-92e3-5f16b3b9f7da"}}],
                             "focus": {"reference": "Specimen/e87f0936-319b-5b86-bc27-3e3fdde77c29"},
                             "for": {"reference": "Patient/e87f0936-319b-5b86-bc27-3e3fdde77c29"}}
    response = requests.post(validate_url, json=invalid_body_no_owner, headers=headers)
    response.raise_for_status()
    issues = response.json()['issue']
    errors = [issue['expression'][0] for issue in issues if issue['severity'] == 'error']
    assert 'Task.owner' in errors, f"Should have raised missing focus {issues}"
    warnings = [issue for issue in issues if issue['severity'] == 'warning']
    assert len(warnings) == 0, f"Should not have any warnings {warnings}"

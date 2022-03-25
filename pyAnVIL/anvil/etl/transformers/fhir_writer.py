import json
import os
import uuid
import logging
import re

import fhirclient.models.patient as FHIRPatient
import fhirclient.models.organization as FHIROrganization
import fhirclient.models.researchstudy as FHIRResearchStudy
import fhirclient.models.researchsubject as FHIRResearchSubject
import fhirclient.models.specimen as FHIRSpecimen
import fhirclient.models.task as FHIRTask
import fhirclient.models.documentreference as FHIRDocumentReference
import fhirclient.models.observation as FHIRObservation
import fhirclient.models.fhirreference as FHIRReference
import fhirclient.models.identifier as FHIRIdentifier
import fhirclient.models.identifier as FHIRIdentifier
import fhirclient.models.practitioner as FHIRPractitioner
import fhirclient.models.practitionerrole as FHIRPractitionerRole

logger = logging.getLogger(__name__)

def _ref(fhir_resource):
    """Create a local reference for a fhir resource."""
    return FHIRReference.FHIRReference({'reference': f"{fhir_resource.resource_type}/{fhir_resource.id}"})

def _id(*args):
    """Create legal fhir id, a reproducible SHA-1 hash of a workspace_name and resource_id."""
    name = '-'.join([x for x in args])
    namespace = bytes(bytearray("pyAnVIL".ljust(16), 'utf-8'))
    namespace_uuid = uuid.UUID(bytes=namespace)
    return str(uuid.uuid5(namespace_uuid, name))

def _identifier(workspace, terra_entity):
    """Create FHIR compliant identifier from terra entity"""
    js = {
        "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{workspace.workspace.name}",
        "value": f"{terra_entity['entityType']}/{terra_entity['name']}",
    }
    _identifier = FHIRIdentifier.Identifier(js)
    return _identifier

def _document_reference_identifier(workspace, output_source, output_property, specimen):
    """Create FHIR compliant identifier for document reference, add the blob origination entity and property."""
    js = {
        "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{workspace.workspace.name}",
        "value": f"{specimen['entityType']}/{specimen['name']}#{output_source}/{output_property}",
    }
    _identifier = FHIRIdentifier.Identifier(js)
    return _identifier

def _fhir_id(*args):
    """Create legal fhir id."""
    return re.sub(r"[^A-Za-z0-9\-\.]", "-", ".".join(str(a) for a in args))[-64:]


def generate_fhir(workspace, consortium_name):
    """Generate FHIR resources from normalized workspace."""
    workspace_name = workspace.workspace.name
    anvil_org = FHIROrganization.Organization({'id': 'anvil'})
    consortium_org = FHIROrganization.Organization({'id': consortium_name})
    consortium_org.partOf = _ref(anvil_org)
    practitioner = FHIRPractitioner.Practitioner({'id': _fhir_id(workspace.practitioner)})
    workspace_org = FHIROrganization.Organization({'id': _id('Organization', workspace_name)})
    workspace_org.partOf = _ref(consortium_org)
    workspace_org.identifier = [FHIRIdentifier.Identifier({
        "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/",
        "value": f"{workspace_name}",
    })]
    practitioner_role = FHIRPractitionerRole.PractitionerRole(
        {'id': _id('PractitionerRole', workspace_name, workspace.practitioner)}
    )
    practitioner_role.practitioner = _ref(practitioner)
    practitioner_role.organization = _ref(workspace_org)
    practitioner_role.identifier = [FHIRIdentifier.Identifier({
        "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/",
        "value": f"{workspace_name}",
    })]

    research_study = FHIRResearchStudy.ResearchStudy({'id': workspace_name, 'status': 'completed', })
    research_study.sponsor = _ref(workspace_org)

    yield anvil_org
    yield consortium_org
    yield practitioner
    yield workspace_org
    yield practitioner_role
    yield research_study

    for patient in workspace.patients.values():
        fhir_patient = FHIRPatient.Patient(
            {
                'id': _id(workspace_name, 'Patient', patient['name']),
                'managingOrganization': _ref(workspace_org).as_json()
            }
        )
        fhir_patient.identifier = [_identifier(workspace, patient)]
        yield fhir_patient
        research_subject = FHIRResearchSubject.ResearchSubject(
            {
                'id': _id(workspace_name, 'ResearchSubject', patient['name']),
                'status': 'on-study',
                'individual': _ref(fhir_patient).as_json(),
                'study': _ref(research_study).as_json()
            }
        )
        research_subject.identifier = [_identifier(workspace, patient)]
        yield research_subject
        for specimen in patient['specimens']:
            fhir_specimen = FHIRSpecimen.Specimen({'id': _id(workspace_name, 'Specimen', specimen['name']), 'subject':_ref(fhir_patient).as_json()})
            fhir_specimen.identifier = [_identifier(workspace, specimen)]
            yield fhir_specimen
            for task in specimen['tasks']:
                # create unique task id
                _task_id_keys = [input['name'] for input in task['inputs']]
                for output_source, output in task['outputs'].items():
                    for output_property, blob in output.items():
                        _task_id_keys.append(blob['url'])
                task_id = _id(*_task_id_keys)
                fhir_task = FHIRTask.Task({'id': task_id, 'input':[], 'output': [], 'status': 'accepted', 'intent': 'unknown'})
                fhir_task.identifier = [_identifier(workspace, task)]
                for input in task['inputs']:
                    assert 'fhir_entity' in input, 'missing.fhir_entity.in.task'                    
                    fhir_task.input.append(FHIRTask.TaskInput(
                        {
                            'type': {'coding': [{'code': 'Reference'}]},                                    
                            'valueReference': {'reference': f"{input['fhir_entity']}/{_id(workspace_name, input['fhir_entity'], input['name'])}"}
                        }
                    ))
                seen_already = set()
                for output_source, output in task['outputs'].items():
                    for output_property, blob in output.items():
                        assert 'url' in blob, ('missing.url.in.blob', output_source, output_property)
                        document_reference = FHIRDocumentReference.DocumentReference(
                            {
                                'id': _id(blob['url']),
                                'status': 'current',
                                'content': [{'attachment': {'url': blob['url']} }],
                                'subject': _ref(fhir_patient).as_json()
                            }
                        )
                        document_reference.identifier = [_document_reference_identifier(workspace, output_source, output_property, task['inputs'][0])]

                        # FHIRDocumentReference.DocumentReferenceContent({'attachment': {'url': blob['url']} })
                        fhir_task.output.append(
                            FHIRTask.TaskOutput(
                                {'type': {'coding': [{'code': 'Reference'}]}, 'valueReference':  _ref(document_reference).as_json() }
                            )
                        )
                        # multiple tasks can refer to same document, so de-duplicate
                        if document_reference.id not in seen_already:
                            yield document_reference
                        seen_already.add(document_reference.id)
                yield fhir_task



def write(consortium_name, workspace, output_path):
    """Write normalized workspace to disk as FHIR."""
    emitters = {}
    for fhir_resource in generate_fhir(workspace, consortium_name):
        # print(fhir_resource.relativePath())
        resourceType = fhir_resource.resource_type
        dir_path = f"{output_path}/{consortium_name}/{workspace.workspace.name}"
        public_protected = 'protected'
        if resourceType in ['ResearchStudy', 'Organization', 'Practitioner', 'PractitionerRole']:
            public_protected = 'public'
        file_path = None
        if resourceType == 'Observation' and fhir_resource.focus:
            print(fhir_resource.focus)
            exit()
            if resourceType == 'Observation' and 'ResearchStudy' in focus_reference:
                file_path = f"{dir_path}/public/ResearchStudyObservation.json"
            else:
                file_path = f"{dir_path}/protected/Observation.json"
        if not file_path:
            file_path = f"{dir_path}/{public_protected}/ResearchStudyObservation.json"

        file_path = f"{dir_path}/protected/{fhir_resource.relativeBase()}.ndjson"
        emitter = emitters.get(file_path, None)
        if emitter is None:
            os.makedirs(f"{dir_path}/public", exist_ok=True)
            os.makedirs(f"{dir_path}/protected", exist_ok=True)
            emitter = open(file_path, "w")
            logger.info(f"Writing {file_path}")
            emitters[file_path] = emitter
        json.dump(fhir_resource.as_json(), emitter, separators=(',', ':'))
        emitter.write('\n')
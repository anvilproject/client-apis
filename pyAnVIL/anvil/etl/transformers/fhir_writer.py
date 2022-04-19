from collections import defaultdict
import json
import os
import uuid
import logging
import re
import itertools

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
import fhirclient.models.practitioner as FHIRPractitioner
import fhirclient.models.practitionerrole as FHIRPractitionerRole

from anvil.etl.utilities.disease_normalizer import ontology_text, disease_system, text_ontology

logger = logging.getLogger(__name__)

# note coordinate with StructureDefintions /fhir/config.yaml::canonical
CANONICAL = "https://nih-ncpi.github.io/ncpi-fhir-ig"


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
    return FHIRIdentifier.Identifier(js)


def _document_reference_identifier(workspace, output_source, output_property, specimen):
    """Create FHIR compliant identifier for document reference, add the blob origination entity and property."""
    js = {
        "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{workspace.workspace.name}",
        "value": f"{specimen['entityType']}/{specimen['name']}#{output_source}/{output_property}",
    }
    return FHIRIdentifier.Identifier(js)


def _family_identifier(workspace, family_id):
    """Create FHIR compliant identifier for family_id."""
    js = {
        "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{workspace.workspace.name}#family_id",
        "value": family_id
    }
    return FHIRIdentifier.Identifier(js)


def _patient_identifier(workspace, id, label):
    """Create FHIR compliant identifier for patient id."""
    js = {
        "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{workspace.workspace.name}#{label}",
        "value": id
    }
    return FHIRIdentifier.Identifier(js)


def _fhir_id(*args):
    """Create legal fhir id."""
    return re.sub(r'[^A-Za-z0-9\-\.]', "-", ".".join(str(a) for a in args))[-64:]


def ensure_data_store_name(workspace):
    """Create data store name from phsId and dataUseRestriction."""
    data_store_name = 'pending'
    if workspace.consortium_name == 'Public':
        data_store_name = 'public'
    if 'tracker' in workspace and workspace['tracker'] and 'phsId' in workspace['tracker'] and workspace['tracker']['phsId']:
        data_store_name = workspace['tracker']['phsId']
        if not data_store_name or not data_store_name.startswith('phs'):
            data_store_name = 'pending'
        else:
            if "library:dataUseRestriction" in workspace['tracker'] and 'tbd' != workspace['tracker']['library:dataUseRestriction'].lower():
                data_store_name = f"{data_store_name}-{workspace['tracker']['library:dataUseRestriction']}"
    return data_store_name.replace(' ', '_')


def _create_administrative_entities(workspace, consortium_name):
    """Generate FHIR Organization, ResearchStudy, etc."""
    workspace_name = workspace.workspace.name

    anvil_org = FHIROrganization.Organization({'id': 'anvil'})
    anvil_org.identifier = [
        FHIRIdentifier.Identifier({
            "system": "https://anvil.terra.bio/program",
            "value": 'anvil',
        })
    ]

    consortium_org = FHIROrganization.Organization({'id': _fhir_id(consortium_name)})
    consortium_org.partOf = _ref(anvil_org)
    consortium_org.identifier = [
        FHIRIdentifier.Identifier({
            "system": "https://anvil.terra.bio/consortium",
            "value": consortium_name,
        })
    ]

    data_store_name = ensure_data_store_name(workspace)    
    data_store_org = FHIROrganization.Organization({'id': _fhir_id(data_store_name)})
    data_store_org.partOf = _ref(consortium_org)
    data_store_org.identifier = [
        FHIRIdentifier.Identifier({
            "system": "https://anvil.terra.bio/#FHIR/data-store",
            "value": data_store_name,
        })
    ]

    practitioner = FHIRPractitioner.Practitioner({'id': _fhir_id(workspace.practitioner)})
    workspace_org = FHIROrganization.Organization({'id': _id('Organization', workspace_name)})
    workspace_org.partOf = _ref(data_store_org)
    workspace_org.identifier = [FHIRIdentifier.Identifier({
        "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/",
        "value": f"{workspace_name}",
    })]
    practitioner_role = FHIRPractitionerRole.PractitionerRole(
        {'id': _id('PractitionerRole', workspace_name, workspace.practitioner)}
    )
    practitioner_role.practitioner = _ref(practitioner)
    practitioner_role.organization = _ref(workspace_org)
    practitioner_role.identifier = [FHIRIdentifier.Identifier({
        "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/",
        "value": f"{workspace_name}",
    })]

    research_study = FHIRResearchStudy.ResearchStudy({'id': _fhir_id(workspace_name), 'status': 'completed', })
    research_study.sponsor = _ref(workspace_org)
    research_study.identifier = [
        FHIRIdentifier.Identifier({
            "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/",
            "value": f"{workspace_name}",
        }),
        FHIRIdentifier.Identifier({
            "system": "https://anvil.terra.bio/#FHIR/data-store",
            "value": f"{data_store_name}",
        }),
        FHIRIdentifier.Identifier({
            "system": "https://anvil.terra.bio/#consortium",
            "value": consortium_name,
        }),
    ]

    return (
        anvil_org,
        consortium_org,
        practitioner,
        data_store_org,
        workspace_org,
        practitioner_role,
        research_study
    )


def _make_fhir_task_NEW(fhir_patient, task, workspace, workspace_name):
    """Generate Task, etc."""
    for output_source, output in task['outputs'].items():
        for output_property, blob in output.items():
            _task_id_keys.append(blob['url'])
    task_id = _id(*_task_id_keys)
    identifiers = [_identifier(workspace, task)]
    inputs = []
    for _input in task['inputs']:
        assert 'fhir_entity' in _input, 'missing.fhir_entity.in.task'
        inputs.append(
            {
                'type': {'coding': [{'code': 'Reference'}]},
                'valueReference': {
                    'reference': f"{_input['fhir_entity']}/{_id(workspace_name, _input['fhir_entity'], _input['name'])}"}
            }
        )

    task = {
      "id": task_id,
      "identifier": identifiers,
      "input": inputs,
      "intent": "unknown",
      "output": [
        {
          "type": {
            "coding": [
              {
                "code": "Reference"
              }
            ]
          },
          "valueReference": {
            "reference": "DocumentReference/c07e3b64-f677-5ef7-9bb0-8a2a188592c2"
          }
        },
        {
          "type": {
            "coding": [
              {
                "code": "Reference"
              }
            ]
          },
          "valueReference": {
            "reference": "DocumentReference/f21602c9-f5ad-5863-9fa3-cef957311a5f"
          }
        },
        {
          "type": {
            "coding": [
              {
                "code": "Reference"
              }
            ]
          },
          "valueReference": {
            "reference": "DocumentReference/19162a3a-346a-54de-8587-a4aa5d7b1a5b"
          }
        },
        {
          "type": {
            "coding": [
              {
                "code": "Reference"
              }
            ]
          },
          "valueReference": {
            "reference": "DocumentReference/c07e3b64-f677-5ef7-9bb0-8a2a188592c2"
          }
        },
        {
          "type": {
            "coding": [
              {
                "code": "Reference"
              }
            ]
          },
          "valueReference": {
            "reference": "DocumentReference/f21602c9-f5ad-5863-9fa3-cef957311a5f"
          }
        }
      ],
      "status": "accepted",
      "resourceType": "Task"
    }


def _generate_specimen_descendants(workspace, patient, fhir_patient, details):
    """Generate FHIR Specimen, Task and DocumentReference."""
    workspace_name = workspace.workspace.name

    if 'specimens' in patient:
        for specimen in patient['specimens']:
            fhir_specimen = FHIRSpecimen.Specimen({'id': _id(workspace_name, 'Specimen', specimen['name']), 'subject': _ref(fhir_patient).as_json()})
            fhir_specimen.identifier = [_identifier(workspace, specimen)]
            yield fhir_specimen
            if details:
                yield _terra_observation(workspace, specimen, fhir_specimen)
            if 'tasks' in specimen:
                for task in specimen['tasks']:
                    fhir_task = yield from _make_fhir_task(fhir_patient, task, workspace, workspace_name)
                    yield fhir_task
                    if details:
                        yield _terra_observation(workspace, task, fhir_task)


def _make_fhir_task(fhir_patient, task, workspace, workspace_name):
    """Create task and its descendants."""
    # create unique task id
    _task_id_keys = [input['name'] for input in task['inputs']]
    for output_source, output in task['outputs'].items():
        for output_property, blob in output.items():
            _task_id_keys.append(blob['url'])
    task_id = _id(*_task_id_keys)
    fhir_task = FHIRTask.Task({'id': task_id, 'input': [], 'output': [], 'status': 'accepted', 'intent': 'unknown'})
    fhir_task.identifier = [_identifier(workspace, task)]
    for input in task['inputs']:
        assert 'fhir_entity' in input, 'missing.fhir_entity.in.task'
        fhir_task.input.append(FHIRTask.TaskInput(
            {
                'type': {'coding': [{'code': 'Reference'}]},
                'valueReference': {
                    'reference': f"{input['fhir_entity']}/{_id(workspace_name, input['fhir_entity'], input['name'])}"}
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
                    'content': [{'attachment': {'url': blob['url']}}],
                    'subject': _ref(fhir_patient).as_json()
                }
            )
            document_reference.identifier = [
                _document_reference_identifier(workspace, output_source, output_property, task['inputs'][0])]

            # FHIRDocumentReference.DocumentReferenceContent({'attachment': {'url': blob['url']} })
            fhir_task.output.append(
                FHIRTask.TaskOutput(
                    {'type': {'coding': [{'code': 'Reference'}]}, 'valueReference': _ref(document_reference).as_json()}
                )
            )
            # multiple tasks can refer to same document, so de-duplicate
            if document_reference.id not in seen_already:
                yield document_reference
            seen_already.add(document_reference.id)
    yield fhir_task
    return fhir_task


def _create_individual(workspace, patient, workspace_org, research_study):
    """Create FHIR resources from Patient, ResearchSubject."""
    workspace_name = workspace.workspace.name

    fhir_patient = FHIRPatient.Patient(
        {
            'id': _id(workspace_name, 'Patient', patient['name']),
            'managingOrganization': _ref(workspace_org).as_json()
        }
    )
    if 'gender' in patient:
        fhir_patient.gender = patient['gender']
    fhir_patient.identifier = [_identifier(workspace, patient)]

    research_subject = FHIRResearchSubject.ResearchSubject(
        {
            'id': _id(workspace_name, 'ResearchSubject', patient['name']),
            'status': 'on-study',
            'individual': _ref(fhir_patient).as_json(),
            'study': _ref(research_study).as_json()
        }
    )
    research_subject.identifier = [_identifier(workspace, patient)]
    return fhir_patient, research_subject


def _disease_coding(disease):
    """Given a term, lookup the disease ontology match."""
    curie = text_ontology.get(disease, None)
    assert curie, f"could not find curie for {disease}"
    text = ontology_text.get(curie)
    prefix, code = curie.split(':')
    system = disease_system.get(prefix)
    return {
        "system": system,
        "code": code,
        "display": text
    }


def _research_study_observation(workspace, research_study):
    """Generate Observation summarizing research_study."""
    workspace_name = workspace.workspace.name
    sample_count = 0
    patient_count = 0
    blob_size_sum = 0
    missing_specimens_count = 0
    for p in workspace.patients.values():
        patient_count += 1
        if 'specimens' not in p:
            if missing_specimens_count == 0:
                logger.warning(('patient.specimens.missing', workspace_name, p['entityType'], p['name']))
            missing_specimens_count += 1
            continue
        for s in p['specimens']:
            sample_count += 1
            if 'tasks' not in s:
                logger.warning(('sample.tasks.missing', workspace_name, s['entityType'], s['name']))
                continue
            for t in s['tasks']:
                for property_blobs in t['outputs'].values():
                    for blob in property_blobs.values():
                        if 'size' not in blob:
                            logger.warning(('blob.attributes.missing', workspace_name, blob.keys(), blob))
                        blob_size_sum += int(blob.get('size', '0'))
    if missing_specimens_count > 0:
        logger.warning(('total.patient.specimens.missing', workspace_name, missing_specimens_count))

    consent_codes = "pending"
    primary_disease = "NA"
    study_design = "NA"
    data_type = "NA"

    if 'tracker' in workspace and workspace['tracker']:
        if "library:dataUseRestriction" in workspace['tracker']:
            consent_codes = workspace['tracker']['library:dataUseRestriction']

        assert 'library:indication' in workspace['tracker'], workspace['tracker']
        primary_disease = workspace['tracker']['library:indication']

        assert 'library:studyDesign' in workspace['tracker'], workspace['tracker']
        study_design = workspace['tracker']['library:studyDesign']

        assert 'library:datatype' in workspace['tracker'], workspace['tracker']
        data_type = workspace['tracker']['library:datatype']
    else:
        logger.error(('workspace.tracker.missing', workspace_name))

    observation = FHIRObservation.Observation(
        {
            'id': _id(workspace_name, 'Observation', research_study.id),
            "status": "final",
            'code': {'coding': [{'code': 'Summary', "display": "Variable Summary", "system": CANONICAL}]},
            'focus': [_ref(research_study).as_json()],
            'component': [
                {
                    'code': {'coding': [{'code': 'SampleCount', 'display': 'Number of Samples', "system": CANONICAL}]},
                    'valueInteger': sample_count
                },
                {
                    'code': {'coding': [{'code': 'Participant', 'display': 'Number of Participants', "system": CANONICAL}]},
                    'valueInteger': patient_count
                },
                {
                    'code': {'coding': [{'code': 'StorageSize', 'display': 'Size on Disk', "system": CANONICAL}]},
                    "valueQuantity": {
                        "value": blob_size_sum,
                        "system": "http://unitsofmeasure.org",
                        "code": "L"
                    }
                },
                {
                    'code': {'coding': [{'code': 'TerraWorkspace', 'display': 'Terra Workspace Name', "system": CANONICAL}]},
                    "valueString": workspace_name
                },
                {
                    'code': {'coding': [{'code': 'PrimaryDisease', 'display': 'Primary Disease', "system": CANONICAL}]},
                    "valueString": primary_disease
                },
                {
                    'code': {'coding': [{'code': 'StudyDesign', 'display': 'Study Design', "system": CANONICAL}]},
                    "valueString": study_design
                },
                {
                    'code': {'coding': [{'code': 'DataType', 'display': 'Data Type', "system": CANONICAL}]},
                    "valueString": data_type
                },
                {
                    'code': {'coding': [{'code': 'ConsentCodes', 'display': 'Consent Codes', "system": CANONICAL}]},
                    "valueString": consent_codes
                },

            ]
        }
    )

    if primary_disease and primary_disease != "NA":
        observation.component.append(
            {
                'code': {'coding': [{'code': 'PrimaryDiseaseOntology', 'display': 'Primary Disease Ontology', "system": CANONICAL}]},
                "valueCodeableConcept": {
                    "coding": [
                        _disease_coding(primary_disease)
                    ],
                    "text": primary_disease
                }
            }
        )

    return observation


def _terra_observation(workspace, entity, fhir_resource):
    """Create an observation with all the fields from the terra.workspace.patient"""
    assert 'attributes' in entity, ('no attributes on entity', entity)
    workspace_name = workspace.workspace.name

    def _str(val):
        """Return simple string or json dict."""
        if isinstance(val, dict):
            return json.dumps(val)
        return str(val)

    observation = FHIRObservation.Observation(
        {
            'id': _id(workspace_name, 'Observation', fhir_resource.id),
            "status": "final",
            'code': {'coding': [{'code': 'Detail', "display": f"{entity['entityType']} Detail", "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/"}]},
            'focus': [_ref(fhir_resource).as_json()],
            'component': [
                {
                    'code': {'coding': [{'code': 'Keys', "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{workspace_name}/{entity['entityType']}"}]},
                    'valueString': ','.join([key for key in entity['attributes']])
                },
                {
                    'code': {'coding': [{'code': 'Values', "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{workspace_name}/{entity['entityType']}/{entity['name']}"}]},
                    'valueString': ','.join([_str(val) for val in entity['attributes'].values()])
                },
            ]
        }
    )

    return observation


def _patient_reference(id):
    """Create a family member reference."""
    return FHIRReference.FHIRReference({'reference': f"Patient/{id}"})


def _generate_family_relationship_observations(workspace, family_id, family, config):
    """Create an observation of this patient's relationship to proband."""
    
    workspace_name = workspace.workspace.name
    assert list(config['mapping']['family_relationships'].keys()) == ['terms', 'intra_family']

    # local functions
    def _map_relationship(relationship):
        """Map term to standard concept."""
        assert list(config['mapping']['family_relationships']['terms'].keys()) == ['exact_match', 'occurrence'], config['mapping']['family_relationships']['terms'].keys()
        occurrences = config['mapping']['family_relationships']['terms']['occurrence']
        exact_matches = config['mapping']['family_relationships']['terms']['exact_match']
        # everything done in lower case
        relationship = relationship.lower().replace('_', ' ')
        # map variations of relationships to more standard term
        for occurrence in occurrences:
            if occurrence in relationship:
                return occurrences[occurrence]
        
        # look up term in mapping and return role code
        if relationship not in exact_matches:
            # logger.warning(('no.family.relationship.mapping', workspace_name, relationship, family))
            logger.warning(('no.family.relationship.mapping', relationship ))
            relationship = '_other_'
        return exact_matches[relationship]


    def _family_membership(mapped_family):
        """Yield family membership records based on relationship to proband."""
        assert list(config['mapping']['family_relationships']['intra_family'].keys()) == ['pairs', 'rules'], config['mapping']['family_relationships']['intra_family'].keys()
        flattened = []
        # family role is the RoleCode
        for family_role, _details in mapped_family.items():
            for detail in _details:
                detail['family_role'] = family_role
                flattened.append(detail)
        for focus, subject in itertools.permutations(flattened, 2):
            relationship = None
            roles = [focus['family_role'], subject['family_role']]
            pairs_results = {}
            pairs = config['mapping']['family_relationships']['intra_family']['pairs']
            for relationship in pairs:  
                pairs_results[relationship] = [pair == roles for pair in pairs[relationship] if pair == roles]
            relationship = next(iter([k  for k,v in pairs_results.items() if v]), None)
            if not relationship:
                rule_results = {}
                rules = config['mapping']['family_relationships']['intra_family']['rules']
                
                def _logging_eval(rule, locals):
                    """Log any exception."""
                    try:
                        return eval(rule, {'focus': focus, 'subject': subject})
                    except Exception as e:
                        logger.error((rule, e))
                        raise e

                for relationship in rules:
                    rule_results[relationship] = True in [_logging_eval(rule, {'focus': focus, 'subject': subject}) for rule in rules[relationship]]
                relationship = next(iter([k  for k,v in rule_results.items() if v]), None)
            if not relationship:
                logger.warning(('no.relationship.between.family.members', workspace_name, family_id, focus, subject))
            else:
                observation = {
                    'id': _id(workspace_name, 'Observation', 'family-relationship', family_id, relationship, detail['id']),
                    "meta": {
                        "profile": [
                            "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/family-relationship"
                        ]
                    },
                    'identifier': [
                        _family_identifier(workspace, family_id).as_json(),
                        _patient_identifier(workspace, subject['name'], 'subject').as_json(),
                        _patient_identifier(workspace, focus['name'], 'focus').as_json()
                    ],
                    "status": "final",
                    'code': {'coding': [{'code': 'FAMMEMB', "display": "family member", "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode"}]},
                    'subject': _patient_reference(subject['id']).as_json(),  # this patient
                    'focus': [_patient_reference(focus['id']).as_json()],  # the proband
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                                "code": relationship
                            }
                        ]
                    }
                }

                observation = FHIRObservation.Observation(observation)
                yield observation        
        # if no relationships
        yield from []



    def _map_relationship_OLD(relationship):
        """Map term to standard concept."""
        relationship = relationship.lower().replace('_', ' ')
        # https://terminology.hl7.org/3.1.0/CodeSystem-v3-RoleCode.html
        relationship_mapping = {
            'proband': 'ONESELF',
            'sister': 'SIS',
            'mother': 'MTH',
            'sibling': 'SIB',
            'affected father': 'FTH',
            'father': 'FTH',
            'brother': 'BRO',
            'paternal uncle': 'PUNCLE',
            'paternal aunt': 'PAUNT',
            'paternal grandfather': 'PGRFTH',
            'wife': 'WIFE',
            'husband': 'HUSB',
            'daughter': 'DAU',
            'son': 'SON',
            'parent': 'PRN',
            'child': 'CHILD',
            'uncle': 'UNCLE',
            'aunt': 'AUNT',
            'nephew or niece': 'NIENEPH',
            'brother or sister': 'SIB',
            'son or daughter': 'CHILD',
            'cousin': 'COUSN',
            'spouse': 'SPS',
            'brother or sister-in-law': 'INLAW',
            'grandfather': 'GRFTH',
            'grandmother': 'GRMTH',
            'inlaw': 'INLAW',
            'twin brother': 'TWINBRO',
            'twin sister': 'TWINSIS',
            'twin': 'TWIN',
            'maternal grandparent': 'MGRPRN',
            'paternal grandparent': 'PGRPRN',
            'half-sib': 'HSIB',
            'other': 'EXT',
            '_other_': 'EXT',
            '_not_related_': 'NOTRELATED'
        }
        if '#' in relationship and 'sibling' in relationship:
            relationship = 'sibling'
        if 'affected' == relationship:
            relationship = 'proband'
        if 'affected1' == relationship:
            relationship = 'proband'
        if 'affected2' == relationship:
            relationship = 'proband'
        if 'proband (twin)' == relationship:
            relationship = 'proband'
        if 'other' in relationship:
            relationship = 'other'
        if 'in law' in relationship:
            relationship = 'inlaw'
        if 'cousin' in relationship:
            relationship = 'cousin'
        if 'husband or wife' in relationship:
            relationship = 'spouse'
        if 'son or daughter' in relationship:
            relationship = 'sibling'
        if 'aunt or uncle' in relationship:
            relationship = 'other'
        if 'identical twin of proband' in relationship:
            relationship = 'twin'
        if 'father1' in relationship:
            relationship = 'father'
        if 'father2' in relationship:
            relationship = 'father'
        if 'sister2' in relationship:
            relationship = 'sister'
        if 'unaffected sibling' in relationship:
            relationship = 'sibling'
        if 'not related' in relationship:
            relationship = '_not_related_'
        if relationship not in relationship_mapping:
            logger.warning(('no.family.relationship.mapping', workspace_name, relationship, family))
            relationship = '_other_'
        return relationship_mapping[relationship]

    def _family_membership_OLD(mapped_family):
        """Yield family membership records based on relationship to proband."""
        flattened = []
        # family role is the RoleCode
        for family_role, _details in mapped_family.items():
            for detail in _details:
                detail['family_role'] = family_role
                flattened.append(detail)
        for focus, subject in itertools.permutations(flattened, 2):
            sorted_roles = sorted([focus['family_role'], subject['family_role']])
            relationship = None
            if sorted_roles == ['FTH', 'MTH']:
                relationship = 'SPS'  # spouse
            elif sorted_roles == ['SIB', 'SIB'] or sorted_roles == ['CHILD', 'CHILD']:
                relationship = 'SIB'  # sibling
            elif sorted_roles == ['CHILD', 'NIENEPH'] or sorted_roles == ['NIENEPH', 'SIB'] or sorted_roles == ['COUSN', 'SIB']:
                relationship = 'COUSN'  # cousin
            elif sorted_roles == ['NIENEPH', 'NIENEPH']:
                relationship = 'EXT'
            elif sorted_roles == ['MGRPRN', 'MGRPRN']:
                relationship = 'SPS'
            elif sorted_roles == ['PGRPRN', 'PGRPRN']:
                relationship = 'SPS'
            elif sorted_roles == ['MGRPRN', 'PGRPRN']:
                relationship = 'EXT'
            elif sorted_roles == ['COUSN', 'COUSN']:
                relationship = 'EXT'
            elif focus['family_role'] in ['FTH', 'MTH', 'PRN', 'SPS'] and subject['family_role'] in ['SIB', 'CHILD']:
                relationship = 'CHILD'
            elif subject['family_role'] in ['FTH', 'MTH', 'PRN', 'SPS'] and focus['family_role'] in ['SIB', 'CHILD']:
                relationship = 'PRN'  # parent
            elif 'EXT' in subject['family_role'] or 'EXT' in focus['family_role']:
                relationship = 'EXT'  # extended
            elif 'NIENEPH' in sorted_roles:
                relationship = 'EXT'
            elif 'MGRPRN' in sorted_roles:
                relationship = 'EXT'
            elif 'PGRPRN' in sorted_roles:
                relationship = 'EXT'
            elif 'COUSN' in sorted_roles:
                relationship = 'EXT'
            elif 'NOTRELATED' in sorted_roles:
                relationship = 'NOTRELATED'
            if not relationship:
                logger.warning(('no.relationship.between.family.members', workspace_name, family_id, focus, subject))
        yield from []

    def _proband_relationships(mapped_family):
        """Yield family membership records based on membership in a family."""
        for relationship, _details in mapped_family.items():
            for detail in _details:
                observation = {
                    'id': _id(workspace_name, 'Observation', 'family-relationship', family_id, relationship, detail['id']),
                    "meta": {
                        "profile": [
                            "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/family-relationship"
                        ]
                    },
                    'identifier': [
                        _family_identifier(workspace, family_id).as_json(),
                        _patient_identifier(workspace, detail['name'], 'subject').as_json(),
                        _patient_identifier(workspace, mapped_family['ONESELF'][0]['name'], 'focus').as_json()
                    ],
                    "status": "final",
                    'code': {'coding': [{'code': 'FAMMEMB', "display": "family member", "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode"}]},
                    'subject': _patient_reference(detail['id']).as_json(),  # this patient
                    'focus': [_patient_reference(mapped_family['ONESELF'][0]['id']).as_json()],  # the proband
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                                "code": relationship
                            }
                        ]
                    }
                }

                observation = FHIRObservation.Observation(observation)
                yield observation

    def _dispatcher():
        """Dispatch based on a) relationship to proband, or b) membership in family."""
        mapped_family = defaultdict(list)
        for relationship, details in family.items():
            mapped_family[_map_relationship(relationship)].extend(details)
        if 'ONESELF' not in mapped_family or len(mapped_family['ONESELF']) == 0:
            for obj in _family_membership(mapped_family):
                yield obj
        else:
            for obj in _proband_relationships(mapped_family):
                yield obj

    # main
    for _obj in _dispatcher():
        yield _obj


def _discover_families(workspace, patient, fhir_patient, families):
    """Populate families with relationships found in patients."""
    assert 'family_relationship_values' in patient
    for family_relationship in patient['family_relationship_values']:
        if family_relationship == {}:
            continue
        assert 'family_id' in family_relationship, ('no.id.in.family', workspace.workspace.name, family_relationship, patient)
        relationship = family_relationship.get('proband_relationship', family_relationship.get('family_relationship', None))

        # assert relationship, ('no.find.relationship', family_relationship, patient)
        if not relationship:
            relationship = 'proband'

        if relationship not in families[family_relationship['family_id']]:
            families[family_relationship['family_id']][relationship] = []

        family_member = {'id': fhir_patient.id, 'name': patient['name'], 'entityType': patient['entityType']}
        extra_fields = {k: v for k, v in family_relationship.items() if k not in ['family_id', 'family_relationship']}
        family_member.update(extra_fields)
        families[family_relationship['family_id']][relationship].append(family_member)


def generate_fhir(workspace, consortium_name, details, config):
    """Generate FHIR resources from normalized workspace."""

    (
        anvil_org,
        consortium_org,
        practitioner,
        data_store_org,
        workspace_org,
        practitioner_role,
        research_study
    ) = _create_administrative_entities(workspace, consortium_name)

    for resource in (
        anvil_org,
        consortium_org,
        practitioner,
        data_store_org,
        workspace_org,
        practitioner_role,
        research_study
    ):
        yield resource

    families = defaultdict(dict)
    for patient in workspace.patients.values():
        (fhir_patient, research_subject) = _create_individual(workspace, patient, workspace_org, research_study)
        yield fhir_patient
        yield research_subject

        for resource in _generate_specimen_descendants(workspace, patient, fhir_patient, details):
            yield resource

        if details:
            yield _terra_observation(workspace, patient, fhir_patient)

        _discover_families(workspace, patient, fhir_patient, families)

    yield _research_study_observation(workspace, research_study)

    for family_id, family in families.items():
        for family_relationship in _generate_family_relationship_observations(workspace, family_id, family, config):
            if family_relationship:
                yield family_relationship


def write(consortium_name, workspace, output_path, details, config):
    """Write normalized workspace to disk as FHIR."""
    emitters = {}
    import cProfile, pstats
    profiler = cProfile.Profile()
    profiler.enable()
    for fhir_resource in generate_fhir(workspace, consortium_name, details, config):
        resource_type = fhir_resource.resource_type
        data_store_name = ensure_data_store_name(workspace)
        dir_path = f"{output_path}/fhir/{data_store_name}/{consortium_name}/{workspace.workspace.name}"
        public_protected = 'protected'
        if resource_type in ['ResearchStudy', 'Organization', 'Practitioner', 'PractitionerRole']:
            public_protected = 'public'
        file_path = None

        resource_reference = None
        if hasattr(fhir_resource, 'focus') and fhir_resource.focus:
            resource_reference = [foci.reference for foci in fhir_resource.focus]
            if len(resource_reference) == 0:
                resource_reference = None
            else:
                resource_reference = resource_reference[0].split('/')[0]
            assert resource_reference

        resource_code = None
        if hasattr(fhir_resource, 'code') and fhir_resource.code:
            resource_code = [coding.code for coding in fhir_resource.code.coding]
            if len(resource_code) == 0:
                resource_code = None
            else:
                resource_code = resource_code[0]

        if resource_type == 'Observation':
            if resource_code == 'Summary':
                file_path = f"{dir_path}/public/{resource_reference}Observation{resource_code}.ndjson"
            else:
                file_path = f"{dir_path}/protected/{resource_reference}Observation{resource_code}.ndjson"

        if not file_path:
            file_path = f"{dir_path}/{public_protected}/{fhir_resource.relativeBase()}.ndjson"

        emitter = emitters.get(file_path, None)
        if emitter is None:
            os.makedirs(f"{dir_path}/public", exist_ok=True)
            os.makedirs(f"{dir_path}/protected", exist_ok=True)
            emitter = open(file_path, "w")
            logger.info(f"Writing {file_path}")
            emitters[file_path] = emitter
        json.dump(fhir_resource.as_json(), emitter, separators=(',', ':'))
        emitter.write('\n')
    profiler.disable()
    # Export profiler output to file
    stats = pstats.Stats(profiler)
    stats.dump_stats(f'{output_path}/program.prof')
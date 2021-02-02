"""Represent fhir entity."""

from anvil.transformers.fhir import CANONICAL, join, make_identifier
from anvil.transformers.fhir.practitioner import Practitioner
from anvil.transformers.fhir.organization import Organization

import logging

from anvil.transformers.fhir.disease_normalizer import disease_text, disease_system


class ResearchStudy:
    """Create fhir entity."""

    class_name = "research_study"
    resource_type = "ResearchStudy"

    @staticmethod
    def build_entity(workspace):
        """Create fhir entity."""
        study_id = workspace.id
        investigator_name = workspace.investigator
        if not investigator_name:
            logging.getLogger(__name__).warning(f'{study_id} missing investigator')
        diseaseOntologyId = workspace.diseaseOntologyId

        diseaseOntologyIdText = 'Missing'
        if diseaseOntologyId and diseaseOntologyId not in disease_text:
            logging.getLogger(__name__).error(f'{study_id} missing {diseaseOntologyId}')
        else:
            diseaseOntologyIdText = disease_text.get(diseaseOntologyId, 'Missing')

        workspace = workspace.attributes.workspace.attributes
        institution = workspace.get('institute', None)
        if not institution:
            logging.getLogger(__name__).warning(f'{study_id} missing institute')
        study_name = study_id
        attribution = study_id
        short_name = study_id
        key = study_id
        condition = None
        if diseaseOntologyId:
            prefix = diseaseOntologyId.split(':')[0]
            condition = [
                {
                    "coding": [
                        {
                            "system": disease_system[prefix],
                            "code": diseaseOntologyId,
                            "display": diseaseOntologyIdText,
                        }
                    ]
                }
            ]

        entity = {
            "resourceType": ResearchStudy.resource_type,
            "id": make_identifier(study_id),
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/ResearchStudy"
                ]
            },
            "identifier": [
                {
                    "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/",
                    "value": study_id,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": join(ResearchStudy.resource_type, key),
                },
            ],
            "extension": [
                {
                    "url": f"{CANONICAL}/StructureDefinition/related-organization",
                    "extension": [
                        {
                            "url": "organization",
                            "valueReference": {
                                "reference": f"Organization/{make_identifier(Organization.resource_type, institution)}"
                            },
                        }
                    ],
                }
            ],
            "title": study_name,
            "status": "completed",
        }

        if investigator_name:
            entity["principalInvestigator"] = {
                "reference": f"Practitioner/{make_identifier(Practitioner.resource_type, investigator_name)}"
            }

        if attribution:
            entity["identifier"].append({"value": attribution})

        if short_name:
            entity["extension"].append(
                {
                    "url": f"{CANONICAL}/StructureDefinition/display-name",
                    "valueString": short_name,
                }
            )
        if condition:
            entity['condition'] = condition

        return entity

"""Represent fhir entity."""

from anvil.transformers.fhir import CANONICAL, join, make_identifier
from anvil.transformers.fhir.practitioner import Practitioner
from anvil.transformers.fhir.organization import Organization

import logging


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
            logging.getLogger(__name__).warning(f'workspace {study_id} missing investigator')

        workspace = workspace.attributes.workspace.attributes
        institution = workspace.get('institute', None)
        if not institution:
            logging.getLogger(__name__).warning(f'workspace {study_id} missing institute')
        study_name = study_id
        attribution = study_id
        short_name = study_id
        key = study_id

        entity = {
            "resourceType": ResearchStudy.resource_type,
            "id": make_identifier(study_id),
            "meta": {
                "profile": [
                    f"{CANONICAL}/StructureDefinition/anvil-research-study"
                ]
            },
            "identifier": [
                {
                    "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/",
                    "value": study_id,
                },
                {
                    "system": "urn:anvil:unique-string",
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

        return entity

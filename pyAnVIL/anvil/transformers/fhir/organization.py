"""Represent fhir entity."""


import logging
from anvil.transformers.fhir import make_identifier


class Organization:
    """Create fhir entity."""

    class_name = "organization"
    resource_type = "Organization"

    @staticmethod
    def build_entity(workspace):
        """Create fhir entity."""
        study_id = workspace.id
        institution = workspace.get('institute', None)
        if not institution:
            logging.getLogger(__name__).warning(f'workspace {study_id} missing institute')

        workspace = workspace.attributes.workspace.attributes

        entity = {
            "resourceType": Organization.resource_type,
            "id": f"Organization/{make_identifier(Organization.resource_type, institution)}",
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/Practitioner"
                ]
            },
            "identifier": [
                {
                    "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/",
                    "value": study_id,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": f"Organization/{make_identifier(Organization.resource_type, institution)}",
                },
            ],
            "name": [{"text": institution}],
        }

        return entity

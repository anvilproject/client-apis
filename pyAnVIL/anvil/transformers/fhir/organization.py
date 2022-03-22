"""Represent fhir entity."""

from os import stat
from anvil.transformers.fhir import make_workspace_id, make_identifier
import logging


class Organization:
    """Create fhir entity."""

    class_name = "organization"
    resource_type = "Organization"

    @staticmethod
    def slug(resource):
        """Make id."""
        return make_workspace_id(resource)

    @staticmethod
    def build_entity(workspace):
        """Create fhir entity."""
        study_id = workspace.id
        id = Organization.slug(workspace)
        institute = workspace.institute
        if not institute:
            logging.getLogger(__name__).warning(f'workspace {workspace.id} missing institute')

        parent = f'Organization/{workspace.attributes.reconciler_name.lower()}'

        entity = {
            "resourceType": Organization.resource_type,
            "id": f"{id}",
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/Organization"
                ]
            },
            "identifier": [
                {
                    "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/",
                    "value": study_id,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": f"{id}",
                },
                {
                    "system": "anvil:consortium",
                    "value": f"{workspace.attributes.reconciler_name.lower()}",
                }
            ],
            "name": study_id,
        }

        if institute:
            entity['identifier'].append(
                {
                    "system": "anvil:institute",
                    "value": f"{workspace.institute.lower()}",
                }
            )

        entity['partOf'] = {
            "reference": parent
        }

        return entity


    @staticmethod
    def build_practitioner_org(workspace):
        """Create fhir entity."""
        institute = workspace.institute or 'Unknown'
        if not institute:
            logging.getLogger(__name__).warning(f'workspace {workspace.id} missing institute')
        id = make_identifier(institute)
        entity = {
            "resourceType": Organization.resource_type,
            "id": f"{id}",
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/Organization"
                ]
            },
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio",
                    "value": id,
                }
            ],
            "name": institute,
            'partOf': {
                "reference": 'Organization/anvil'
            }
        }
        return entity

    @staticmethod
    def build_consortium_org(workspace):
        """Create fhir entity."""
        id = workspace.attributes.reconciler_name.lower()
        entity = {
            "resourceType": Organization.resource_type,
            "id": f"{id}",
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/Organization"
                ]
            },
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio",
                    "value": id,
                }
            ],
            "name": workspace.attributes.reconciler_name,
            'partOf': {
                "reference": 'Organization/anvil'
            }            
        }
        return entity

    @staticmethod
    def build_anvil_org():
        """Create fhir entity."""
        id = 'anvil'
       
        entity = {
            "resourceType": Organization.resource_type,
            "id": f"{id}",
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/Organization"
                ]
            },
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio",
                    "value": id,
                }
            ],
            "name": 'AnVIL',
        }
        return entity

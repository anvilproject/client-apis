"""Represent fhir entity."""


from anvil.transformers.fhir import join, make_identifier
import logging

from anvil.transformers.fhir.organization import Organization

PRACTITIONERS = []

class Practitioner:
    """Create fhir entity."""

    class_name = "practitioner"
    resource_type = "Practitioner"

    @staticmethod
    def slug(workspace):
        """Make id."""
        investigator_name = "Unknown"
        if hasattr(workspace, 'investigator'):
            if workspace.investigator:
                investigator_name = workspace.investigator
        return make_identifier(investigator_name)

    @staticmethod
    def build_entity(workspace):
        """Create fhir entity."""
        practitioner_id = Practitioner.slug(workspace)
        investigator_name = workspace.investigator
        institute = workspace.institute
        if practitioner_id in PRACTITIONERS:
            return None
        PRACTITIONERS.append(practitioner_id)
        if not institute:
            logging.getLogger(__name__).warning(f'workspace {workspace.id} missing institute')
        entity = {
            "resourceType": Practitioner.resource_type,
            "id": practitioner_id,
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/Practitioner"
                ]
            },
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{workspace.attributes.workspace.name}/data",
                    "value": investigator_name,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": join(Practitioner.resource_type, practitioner_id),
                }
            ],
            "name": [{"text": investigator_name}],
        }

        return entity

    @staticmethod
    def build_practitioner_role(workspace):
        """Create fhir entity."""
        practitioner_id = Practitioner.slug(workspace)
        organization_id = Organization.slug(workspace)
        id = f"{organization_id}-{practitioner_id}"
        
        if id in PRACTITIONERS:
            return None

        PRACTITIONERS.append(id)            

        entity = {
            "resourceType": 'PractitionerRole',
            "id": f"{id}",
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/PractitionerRole"
                ]
            },
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio",
                    "value": id,
                }
            ],
            "practitioner": {"reference": f"Practitioner/{practitioner_id}"},
            "organization": {"reference": f"Organization/{organization_id}"}
        }
        return entity        
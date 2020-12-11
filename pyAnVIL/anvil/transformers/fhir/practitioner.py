"""Represent fhir entity."""


from anvil.transformers.fhir import join, make_identifier


class Practitioner:
    """Create fhir entity."""

    class_name = "practitioner"
    resource_type = "Practitioner"

    @staticmethod
    def build_entity(workspace):
        """Create fhir entity."""
        study_id = workspace.id
        investigator_name = workspace.investigator
        if not investigator_name:
            return None

        workspace = workspace.attributes.workspace.attributes
        entity = {
            "resourceType": Practitioner.resource_type,
            "id": make_identifier(Practitioner.resource_type, investigator_name),
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
                    "value": join(Practitioner.resource_type, investigator_name),
                },
            ],
            "name": [{"text": investigator_name}],
        }

        return entity

"""Represent fhir entity."""


from anvil.transformers.fhir import join, make_identifier


class Practitioner:
    """Create fhir entity."""

    class_name = "practitioner"
    resource_type = "Practitioner"

    @staticmethod
    def slug(workspace):
        """Make id."""
        investigator_name = "Unknown"
        if hasattr(workspace, 'investigator'):
            investigator_name = workspace.investigator
        return make_identifier(investigator_name)

    @staticmethod
    def build_entity(workspace):
        """Create fhir entity."""
        practitioner_id = Practitioner.slug(workspace)
        investigator_name = workspace.investigator

        workspace = workspace.attributes.workspace.attributes
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
                    "system": "https://anvil.terra.bio/#workspaces/anvil-datastorage/",
                    "value": investigator_name,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": join(Practitioner.resource_type, investigator_name),
                },
            ],
            "name": [{"text": investigator_name}],
        }

        return entity

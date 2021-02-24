"""Represent fhir entity."""


class Organization:
    """Create fhir entity."""

    class_name = "organization"
    resource_type = "Organization"

    @staticmethod
    def build_entity(workspace, parent=None):
        """Create fhir entity."""
        study_id = workspace.id
        id = study_id.lower()  # make_identifier(Organization.resource_type, study_id)
        # institution = workspace.institute
        # if not institution:
        #     logging.getLogger(__name__).warning(f'workspace {study_id} missing institute')
        #     institution = f"{study_id}-missing-institution"

        # workspace = workspace.attributes.workspace.attributes

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
            ],
            "name": study_id,
        }

        if parent:
            entity['partOf'] = {
                "reference": parent
            }
        elif 'attributes' in workspace:
            entity['partOf'] = {
                "reference": f"Organization/{workspace.attributes.reconciler_name.lower()}"
            }

        return entity

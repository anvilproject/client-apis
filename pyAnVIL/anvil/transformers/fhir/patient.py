"""Represent fhir entity."""

from anvil.transformers.fhir import make_id
from anvil.transformers.fhir.research_study import ResearchStudy
from anvil.terra.subject import Subject


class Patient:
    """Create fhir entity."""

    class_name = "patient"
    resource_type = "Patient"

    @staticmethod
    def slug(subject):
        """Make id."""
        return make_id(subject.workspace_name, subject.id)

    @staticmethod
    def build_entity(subject):
        """Create fhir entity."""
        assert issubclass(subject.__class__, Subject), f"{subject}"

        subject_id_slug = Patient.slug(subject)

        # ethnicity = None
        # race = None
        # species = None
        # gender = None

        entity = {
            "resourceType": Patient.resource_type,
            "id": subject_id_slug,
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/Patient"
                ]
            },
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{subject.workspace_name}",
                    "value": subject.id,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": f"{subject.workspace_name}/Patient/{subject.id}",
                },
            ],
            "managingOrganization": {
                "reference": f"Organization/{ResearchStudy.slug(subject)}"
            }
        }

        # ethnicity QUESTION: http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity
        # if ethnicity:
        #     if omb_ethnicity_category.get(ethnicity):
        #         entity.setdefault("extension", []).append(
        #             {
        #                 "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
        #                 "extension": [
        #                     omb_ethnicity_category[ethnicity],
        #                     {"url": "text", "valueString": ethnicity},
        #                 ],
        #             }
        #         )

        # race QUESTION: http://hl7.org/fhir/us/core/StructureDefinition/us-core-race
        #     if race:
        #         if omb_race_category.get(race):
        #             entity.setdefault("extension", []).append(
        #                 {
        #                     "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
        #                     "extension": [
        #                         omb_race_category[race],
        #                         {"url": "text", "valueString": race},
        #                     ],
        #                 }
        #             )

        # species QUESTION: species ?
        #     if species:
        #         if species_dict.get(species):
        #             entity.setdefault("extension", []).append(species_dict[species])

        if subject.gender:
            entity["gender"] = subject.gender

        return entity

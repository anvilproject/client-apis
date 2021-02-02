"""Represent fhir entity."""

from anvil.transformers.fhir import join, make_identifier


class Patient:
    """Create fhir entity."""

    class_name = "patient"
    resource_type = "Patient"

    @staticmethod
    def build_entity(subject):
        """Create fhir entity."""
        study_id = subject.workspace_name
        subject_id = subject.id
        subject_id_slug = make_identifier('P', subject_id)

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
                    "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{study_id}",
                    "value": subject_id,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": join(Patient.resource_type, study_id, subject_id),
                },
            ],
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

"""Represent fhir entity."""

from anvil.transformers.fhir import join


class Patient:
    """Create fhir entity."""

    class_name = "patient"
    resource_type = "Patient"

    @staticmethod
    def build_entity(subject):
        """Create fhir entity."""
        study_id = subject.workspace_name
        participant_id = subject.id
        # ethnicity = None
        # race = None
        # species = None
        # gender = None

        entity = {
            "resourceType": Patient.resource_type,
            "id": participant_id,
            "meta": {
                "profile": [
                    "http://fhir.kids-first.io/StructureDefinition/kfdrc-patient"
                ]
            },
            "identifier": [
                {
                    "system": f"https://kf-api-dataservice.kidsfirstdrc.org/participants?study_id={study_id}&external_id=",
                    "value": participant_id,
                },
                {
                    "system": "urn:kids-first:unique-string",
                    "value": join(Patient.resource_type, study_id, participant_id),
                },
            ],
        }

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

        #     if species:
        #         if species_dict.get(species):
        #             entity.setdefault("extension", []).append(species_dict[species])

        #     if gender:
        #         if administrative_gender.get(gender):
        #             entity["gender"] = administrative_gender[gender]

        return entity

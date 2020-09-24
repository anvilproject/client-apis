"""Represent fhir entity."""
from anvil.transformers.fhir import join

# # https://www.hl7.org/fhir/v2/0487/index.html
# specimen_type = {
#     constants.SPECIMEN.COMPOSITION.BLOOD: {
#         "system": "http://terminology.hl7.org/CodeSystem/v2-0487",
#         "code": "BLD",
#         "display": "Whole blood",
#     },
#     constants.SPECIMEN.COMPOSITION.SALIVA: {
#         "system": "http://terminology.hl7.org/CodeSystem/v2-0487",
#         "code": "SAL",
#         "display": "Saliva",
#     },
#     constants.SPECIMEN.COMPOSITION.TISSUE: {
#         "system": "http://terminology.hl7.org/CodeSystem/v2-0487",
#         "code": "TISS",
#         "display": "Tissue",
#     },
# }


class Specimen:
    """Create fhir entity."""

    class_name = "specimen"
    resource_type = "Specimen"

    @staticmethod
    def build_entity(sample):
        """Create fhir entity."""
        study_id = sample.workspace_name
        biospecimen_id = sample.id
        event_age_days = None
        concentration_mg_per_ml = None
        composition = None
        volume_ul = None

        entity = {
            "resourceType": Specimen.resource_type,
            "id": sample.id,
            "meta": {
                "profile": [
                    "http://fhir.kids-first.io/StructureDefinition/kfdrc-specimen"
                ]
            },
            "identifier": [
                {
                    "system": f"http://kf-api-dataservice.kidsfirstdrc.org/biospecimens?study_id={study_id}&external_aliquot_id=",
                    "value": biospecimen_id,
                },
                {
                    "system": "urn:kids-first:unique-string",
                    "value": join(Specimen.resource_type, study_id, sample.id),
                },
            ],
            "subject": {
                "reference": f"Patient/{sample.subject_id}"
            },
        }

        if event_age_days:
            entity.setdefault("extension", []).append(
                {
                    "url": "http://fhir.kids-first.io/StructureDefinition/age-at-event",
                    "valueAge": {
                        "value": int(event_age_days),
                        "unit": "d",
                        "system": "http://unitsofmeasure.org",
                        "code": "days",
                    },
                }
            )

        if concentration_mg_per_ml:
            entity.setdefault("extension", []).append(
                {
                    "url": "http://fhir.kids-first.io/StructureDefinition/concentration",
                    "valueQuantity": {
                        "value": float(concentration_mg_per_ml),
                        "unit": "mg/mL",
                    },
                }
            )

        if composition:
            entity["type"] = {
                "coding": "TODO",  # [specimen_type[composition]],
                "text": composition,
            }

        if volume_ul:
            entity.setdefault("collection", {})["quantity"] = {
                "unit": "uL",
                "value": float(volume_ul),
            }

        return entity

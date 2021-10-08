"""Represent fhir entity."""
from anvil.transformers.fhir import CANONICAL, make_id
from anvil.transformers.fhir.patient import Patient

# specimen_type QUESTION: https://www.hl7.org/fhir/v2/0487/index.html
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
    def slug(specimen):
        """Make id."""
        return make_id(specimen.workspace_name, specimen.id)

    @staticmethod
    def build_entity(specimen, subject):
        """Create fhir entity."""
        # study_id = specimen.workspace_name
        # study_id_slug = make_identifier(study_id)
        specimen_slug = Specimen.slug(specimen)
        patient_slug = Patient.slug(subject)
        event_age_days = None
        concentration_mg_per_ml = None
        composition = None
        volume_ul = None

        entity = {
            "resourceType": Specimen.resource_type,
            "id": specimen_slug,
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/Specimen"
                ]
            },
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{subject.workspace_name}",
                    "value": specimen.id,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": f"{subject.workspace_name}/Patient/{subject.id}/Specimen/{specimen.id}",
                },
            ],
            "subject": {
                "reference": f"Patient/{patient_slug}"
            },
        }

        # event_age_days: QUESTION extension ?
        if event_age_days:
            entity.setdefault("extension", []).append(
                {
                    "url": f"{CANONICAL}/StructureDefinition/age-at-event",
                    "valueAge": {
                        "value": int(event_age_days),
                        "unit": "d",
                        "system": "http://unitsofmeasure.org",
                        "code": "days",
                    },
                }
            )

        # concentration_mg_per_ml: QUESTION extension ?
        if concentration_mg_per_ml:
            entity.setdefault("extension", []).append(
                {
                    "url": f"{CANONICAL}/StructureDefinition/concentration",
                    "valueQuantity": {
                        "value": float(concentration_mg_per_ml),
                        "unit": "mg/mL",
                    },
                }
            )

        # composition: QUESTION extension ?
        if composition:
            entity["type"] = {
                "coding": "TODO",  # [specimen_type[composition]],
                "text": composition,
            }

        # volume_ul: QUESTION extension ?
        if volume_ul:
            entity.setdefault("collection", {})["quantity"] = {
                "unit": "uL",
                "value": float(volume_ul),
            }
        return entity

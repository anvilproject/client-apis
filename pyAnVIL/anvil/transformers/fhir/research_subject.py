"""Represent fhir entity."""

from anvil.transformers.fhir.patient import Patient
from anvil.transformers.fhir.research_study import ResearchStudy


class ResearchSubject:
    """Create fhir entity."""

    class_name = "research_subject"
    resource_type = "ResearchSubject"
    # Patient already uses CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
    # so the below is set to None
    target_id_concept = None

    @staticmethod
    def build_entity(subject):
        """Create fhir entity."""
        study_slug = ResearchStudy.slug(subject)
        patient_slug = Patient.slug(subject)
        research_subject_status = "on-study"  # QUESTION: https://www.hl7.org/fhir/valueset-research-subject-status.html

        entity = {
            "resourceType": ResearchSubject.resource_type,
            "id": patient_slug,
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/ResearchSubject"
                ]
            },
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{subject.workspace_name}",
                    "value": subject.id,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": f"{subject.workspace_name}/ResearchSubject/{subject.id}",
                },
            ],
            "status": research_subject_status,
            "study": {
                "reference": f"ResearchStudy/{study_slug}"
            },
            "individual": {
                "reference": f"Patient/{patient_slug}"
            },
        }

        return entity

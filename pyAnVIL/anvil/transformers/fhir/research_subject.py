"""Represent fhir entity."""


from anvil.transformers.fhir import join
from anvil.transformers.fhir.patient import Patient


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
        study_id = subject.workspace_name
        participant_id = subject.id
        # https://www.hl7.org/fhir/valueset-research-subject-status.html
        research_subject_status = "off-study"

        entity = {
            "resourceType": ResearchSubject.resource_type,
            "id": subject.id,
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/ResearchSubject"
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
            "status": research_subject_status,
            "study": {
                "reference": f"ResearchStudy/{study_id}"
            },
            "individual": {
                "reference": f"Patient/{participant_id}"
            },
        }

        return entity

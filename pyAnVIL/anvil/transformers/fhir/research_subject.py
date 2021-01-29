"""Represent fhir entity."""


from anvil.transformers.fhir import join, make_identifier
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
        study_id_slug = make_identifier(study_id)
        subject_id = subject.id
        subject_id_slug = make_identifier('P', subject_id)
        research_subject_status = "off-study"  # QUESTION: https://www.hl7.org/fhir/valueset-research-subject-status.html        

        entity = {
            "resourceType": ResearchSubject.resource_type,
            "id": subject_id_slug,
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/ResearchSubject"
                ]
            },
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{study_id}",
                    "value": subject_id,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": make_identifier('P', subject_id), # join(Patient.resource_type, study_id, subject_id),
                },
            ],
            "status": research_subject_status,
            "study": {
                "reference": f"ResearchStudy/{study_id_slug}"
            },
            "individual": {
                "reference": f"Patient/{subject_id_slug}"
            },
        }

        return entity

"""Represent fhir entity."""

from anvil.transformers.fhir import make_identifier
import logging
from anvil.transformers.fhir.disease_normalizer import disease_text, disease_system

logged_already = []


class DiseaseObservation:
    """Create fhir entity."""

    class_name = "research_study"
    resource_type = "ResearchStudy"

    @staticmethod
    def build_entity(subject, disease):
        """Create FHIR entity."""
        assert disease, f'Should have disease {subject}'

        workspace_diseaseOntologyId = disease  # subject.workspace_diseaseOntologyId
        diseaseOntologyText = disease_text.get(disease, None)
        diseaseOntologySystem = disease_system.get(disease.split(':')[0], None)
        if workspace_diseaseOntologyId and not diseaseOntologyText:
            if workspace_diseaseOntologyId not in logged_already:
                logging.getLogger(__name__).error(f'Need text "{workspace_diseaseOntologyId}"')
                logged_already.append(workspace_diseaseOntologyId)
        assert diseaseOntologySystem, "Should have system"
        slug = make_identifier(f"Observation|{subject.id}|{workspace_diseaseOntologyId}")

        entity = {
            "resourceType": "Observation",
            "id": slug,
            "meta": {
                "profile": [
                    "http://fhir.ncpi-project-forge.io/StructureDefinition/ncpi-phenotype"
                ]
            },
            "identifier": [
                {
                    "system": "urn:ncpi:unique-string",
                    "value": slug
                }
            ],
            "status": "final",
            "code": {
                "coding": [
                    {
                        "system": diseaseOntologySystem,
                        "code": workspace_diseaseOntologyId,
                        "display": diseaseOntologyText
                    }
                ],
                "text": f"{diseaseOntologyText}"
            },
            "subject": {
                "reference": f"Patient/{make_identifier('P', subject.id)}"
            },
            "valueCodeableConcept": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "373573001",
                        "display": "Clinical finding present (situation)"
                    }
                ],
                "text": "Phenotype Present"
            },
            "interpretation": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
                            "code": "POS",
                            "display": "Positive"
                        }
                    ],
                    "text": "Present"
                }
            ],
            "extension": []
        }
        if subject.age:
            entity['extension'].append(
                {
                    "url": "http://fhir.ncpi-project-forge.io/StructureDefinition/age-at-event",
                    "valueAge": {
                        "value": subject.age,
                        "unit": "y",
                        "system": "http://unitsofmeasure.org",
                        "code": "years"
                    }
                }
            )

        return entity

"""Represent fhir entity."""

from anvil.transformers.fhir import make_identifier
import logging
from anvil.transformers.fhir.disease_normalizer import ontology_text, disease_system
from anvil.transformers.fhir import CANONICAL
from anvil.transformers.fhir.patient import Patient
from anvil.transformers.fhir import make_id

logged_already = []


class DiseaseObservation:
    """Create fhir entity."""

    @staticmethod
    def slug(subject, disease):
        """Make id."""        
        return make_id(Patient.slug(subject), f"SNOMED:373573001/{disease}")


    @staticmethod
    def build_entity(subject, disease):
        """Create FHIR entity."""

        assert disease, f'Should have disease {subject}'
        if disease.startswith("PS"):
            disease = f"OMIM:{disease}"
        if ":" not in disease:
            disease = f"OMIM:{disease}"
        workspace_diseaseOntologyId = disease  # subject.workspace_diseaseOntologyId
        diseaseOntologyText = ontology_text.get(disease, None)
        diseaseOntologySystem = disease_system.get(disease.split(':')[0], None)
        workspace_diseaseOntologyId = disease.split(':')[1]
        slug = DiseaseObservation.slug(subject, workspace_diseaseOntologyId)

        if workspace_diseaseOntologyId and not diseaseOntologyText:
            if workspace_diseaseOntologyId not in logged_already:
                logging.getLogger(__name__).error(f'Need text "{workspace_diseaseOntologyId}"')
                logged_already.append(workspace_diseaseOntologyId)
        if not diseaseOntologySystem:
            if workspace_diseaseOntologyId not in logged_already:
                logging.getLogger(__name__).error(f"Should have system. {subject} {disease}")
            diseaseOntologySystem = "MISSING"

        entity = {
            "resourceType": "Observation",
            "id": slug,
            "meta": {
                "profile": [
                    f"http://{CANONICAL}/StructureDefinition/ncpi-phenotype"
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
                "reference": f"Patient/{Patient.slug(subject)}"
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
                    "url": f"http://{CANONICAL}/StructureDefinition/age-at-event",
                    "valueAge": {
                        "value": subject.age,
                        "unit": "y",
                        "system": "http://unitsofmeasure.org",
                        "code": "years"
                    }
                }
            )

        return entity

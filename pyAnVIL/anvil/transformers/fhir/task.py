"""Represent fhir entity."""

from anvil.transformers.fhir.research_study import ResearchStudy
from anvil.transformers.fhir.specimen import Specimen
from anvil.transformers.fhir import make_id
from anvil.transformers.fhir.patient import Patient


class Task:
    """Create fhir entity."""

    class_name = "task"
    resource_type = "Task"

    @staticmethod
    def build_entity():
        """Create fhir entity."""
        pass


class SpecimenTask:
    """Create fhir entity linking Specimen to Blobs."""

    class_name = "task"
    resource_type = "Task"

    @staticmethod
    def slug(specimen):
        """Make id."""
        return make_id("Task", specimen.id)

    @staticmethod
    def build_entity(inputs, outputs, subject):
        """Create fhir entity."""
        specimen = inputs[0]
        inputs = [
            {
                "type": {
                    "coding": [
                        {
                            "code": Specimen.resource_type
                        }
                    ]
                },
                "valueReference": {
                    "reference": f"{Specimen.resource_type}/{Specimen.slug(specimen)}"
                }
            }
        ]
        # inject this task context into DocumentReferences
        for blob in outputs:
            blob['context'] = {
                "related": [
                    {
                        "reference": f"Task/{SpecimenTask.slug(specimen)}"
                    }
                ]
            }

        outputs = [
            {
                "type": {
                    "coding": [
                        {
                            "code": blob['resourceType']
                        }
                    ]
                },
                "valueReference": {
                    "reference": f"{blob['resourceType']}/{blob['id']}"
                }
            }
            for blob in outputs]

        return {
            "resourceType": "Task",
            "id": SpecimenTask.slug(specimen),
            "meta": {
                "profile": [
                    "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/ncpi-specimen-task"
                ]
            },
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{subject.workspace_name}",
                    "value": f"{specimen.id}/Task/AnVILInjest",
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": f"{subject.workspace_name}/Patient/{subject.id}/Specimen/{specimen.id}/Task/AnVILInjest",
                },
            ],
            "status": "accepted",
            "intent": "unknown",
            "input": inputs,
            "output": outputs,
            "focus": {
                "reference": f"{Specimen.resource_type}/{Specimen.slug(specimen)}"
            },
            "for": {
                "reference": f"{Patient.resource_type}/{Patient.slug(subject)}"
            },
            "owner": {
                "reference": f"Organization/{ResearchStudy.slug(specimen)}"
            }
        }

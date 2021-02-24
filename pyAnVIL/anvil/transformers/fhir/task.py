"""Represent fhir entity."""

from anvil.transformers.fhir import make_identifier


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
    def build_entity(inputs, outputs):
        """Create fhir entity."""
        specimen = inputs[0]
        inputs = [
            {
                "type": {
                    "coding": [
                        {
                            "code": specimen['resourceType']
                        }
                    ]
                },
                "valueReference": {
                    "reference": f"{specimen['resourceType']}/{specimen['id']}"
                }
            }
        ]
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
            "id": make_identifier("T", specimen['id']),
            "status": "accepted",
            "intent": "unknown",
            "input": inputs,
            "output": outputs,
            "owner": {
                "reference": "Organization/thousandgenomes"
            }
        }

"""Represent fhir entity."""

from anvil.transformers.fhir import CANONICAL
from anvil.transformers.fhir.research_study import ResearchStudy

logged_already = []


class ResearchStudyObservation:
    """Create fhir entity."""

    class_name = "observation"
    resource_type = "Observation"

    @staticmethod
    def build_entity(workspace):
        """Create FHIR entity."""
        slug = ResearchStudy.slug(workspace)

        entity = {
            "resourceType": "Observation",
            "id": slug,
            "status": "final",
            "code": {
                "coding": [
                    {
                        "system": f"{CANONICAL}",
                        "code": "Summary",
                        "display": "Variable Summary"
                    }
                ]
            },
            "focus": [
                {
                    "reference": f"ResearchStudy/{slug}"
                }
            ],
            "component": [
                # {
                # "code": {
                #     "coding": [
                #     {
                #         "code": "CohortCount",
                #         "display": "Number of Cohorts"
                #     }
                #     ]
                # },
                # "valueInteger": 41
                # },
                {
                    "code": {
                        "coding": [
                            {
                                "system": f"{CANONICAL}",
                                "code": "SampleCount",
                                "display": "Number of Samples"
                            }
                        ]
                    },
                    "valueInteger": len(workspace.samples)
                },
                {
                    "code": {
                        "coding": [
                            {
                                "system": f"{CANONICAL}",
                                "code": "Participant",
                                "display": "Number of Participants"
                            }
                        ]
                    },
                    "valueInteger": len(workspace.subjects)
                },
                {
                    "code": {
                        "coding": [
                            {
                                "system": f"{CANONICAL}",
                                "code": "StorageSize",
                                "display": "Size on Disk"
                            }
                        ]
                    },
                    "valueQuantity": {
                        "value": sum(workspace.blob_sizes.values()),
                        "system": "http://unitsofmeasure.org",
                        "code": "L"
                    }
                }
            ]
        }
        return entity

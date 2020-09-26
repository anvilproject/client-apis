"""Represent fhir entity."""


from anvil.transformers.fhir import CANONICAL


class Attachment:
    """Create fhir entity."""

    class_name = "attachment"
    resource_type = "Attachment"

    @staticmethod
    def build_entity(blob):
        """Create fhir entity."""
        # assert False, blob.attributes
        attributes = blob.attributes
        # AttrDict({'size': 32,
        # 'etag': 'CPDS7ffR9+kCEAE=',
        # 'crc32c': 'Lyw+Kw==',
        # 'time_created': '2020-06-10T16:13:14.003000+00:00',
        # 'name': 'gs://fc-secure-004e5c03-d24d-4f7f-a26b-9fdc64b0ca3c/AnVIL_CMG_Broad_Muscle_KNC_WGS_Mar2020/RP-1687/WGS/192CP_ZS_1/v1/192CP_ZS_1.cram.md5',
        # 'property_name': 'md5_path'})
        entity = {
            "resourceType": Attachment.resource_type,
            "id": attributes.etag,
            "extension": [
                {"extension": [
                    {
                        "url": "id",
                        "valueString": "any-id"
                    },
                    {
                        "url": "self_uri",
                        "valueString": "drs://url-here"
                    },
                    {
                        "url": "size",
                        "valueInteger": attributes.size
                    },
                    {
                        "url": "created_time",
                        "valueDateTime": attributes.time_created
                    },
                    {
                        "extension": [
                            {
                                "url": "checksum",
                                "valueString": attributes.crc32c
                            },
                            {
                                "url": "type",
                                "valueString": attributes.etag
                            }
                        ],
                        "url": f"{CANONICAL}/StructureDefinition/drs-checksum"
                    },
                    {
                        "extension": [
                            {
                                "url": "type",
                                "valueString": "gs"
                            },
                            {
                                "url": "access_url",
                                "valueString": attributes.name
                            },
                        ],
                        "url": f"{CANONICAL}/StructureDefinition/drs-access-method"
                    },
                    {
                        "url": "name",
                        "valueString": "any-file-name"
                    },
                    {
                        "url": "mime_type",
                        "valueString": "application/json"
                    }
                ],
                    "url": f"{CANONICAL}/StructureDefinition/drs-object"
                }
            ]
        }
        return entity

"""Represent fhir entity."""

from anvil.transformers.fhir import join
# from anvil.transformers.fhir.patient import Patient


class GENOMIC_FILE:
    """Define constants."""

    class AVAILABILITY:
        """Define constants."""

        IMMEDIATE = "Immediate Download"
        COLD_STORAGE = "Cold Storage"

    class DATA_TYPE:
        """Define constants."""

        ALIGNED_READS = "Aligned Reads"
        ALIGNED_READS_INDEX = "Aligned Reads Index"
        EXPRESSION = "Expression"
        GVCF = "gVCF"
        GVCF_INDEX = "gVCF Index"
        HISTOLOGY_IMAGES = "Histology Images"
        NUCLEOTIDE_VARIATION = "Simple Nucleotide Variations"
        OPERATION_REPORTS = "Operation Reports"
        PATHOLOGY_REPORTS = "Pathology Reports"
        RADIOLOGY_IMAGES = "Radiology Images"
        RADIOLOGY_REPORTS = "Radiology Reports"
        UNALIGNED_READS = "Unaligned Reads"
        VARIANT_CALLS = "Variant Calls"
        VARIANT_CALLS_INDEX = "Variant Calls Index"
        ANNOTATED_SOMATIC_MUTATIONS = "Annotated Somatic Mutations"
        GENE_EXPRESSION = "Gene Expression"
        GENE_FUSIONS = "Gene Fusions"
        ISOFORM_EXPRESSION = "Isoform Expression"
        SOMATIC_COPY_NUMBER_VARIATIONS = "Somatic Copy Number Variations"
        SOMATIC_STRUCTURAL_VARIATIONS = "Somatic Structural Variations"

    class FORMAT:
        """Define constants."""

        BAI = "bai"
        BAM = "bam"
        CRAI = "crai"
        CRAM = "cram"
        DCM = "dcm"
        FASTQ = "fastq"
        GPR = "gpr"
        GVCF = "gvcf"
        IDAT = "idat"
        PDF = "pdf"
        SVS = "svs"
        TBI = "tbi"
        VCF = "vcf"


# http://fhir.kids-first.io/ValueSet/data-type
data_type_dict = {
    GENOMIC_FILE.DATA_TYPE.ALIGNED_READS: {
        "coding": [
            {
                "system": "http://fhir.kids-first.io/CodeSystem/data-type",
                "code": "C164052",
                "display": "Aligned Sequence Read",
            }
        ],
        "text": GENOMIC_FILE.DATA_TYPE.ALIGNED_READS,
    },
    GENOMIC_FILE.DATA_TYPE.GENE_EXPRESSION: {
        "coding": [
            {
                "system": "http://fhir.kids-first.io/CodeSystem/data-type",
                "code": "C16608",
                "display": "Gene Expression",
            }
        ],
        "text": GENOMIC_FILE.DATA_TYPE.GENE_EXPRESSION,
    },
    GENOMIC_FILE.DATA_TYPE.GENE_FUSIONS: {
        "coding": [
            {
                "system": "http://fhir.kids-first.io/CodeSystem/data-type",
                "code": "C20195",
                "display": "Gene Fusion",
            }
        ],
        "text": GENOMIC_FILE.DATA_TYPE.GENE_FUSIONS,
    },
    GENOMIC_FILE.DATA_TYPE.OPERATION_REPORTS: {
        "coding": [
            {
                "system": "http://fhir.kids-first.io/CodeSystem/data-type",
                "code": "C114420",
                "display": "Operative Report",
            }
        ],
        "text": GENOMIC_FILE.DATA_TYPE.OPERATION_REPORTS,
    },
    GENOMIC_FILE.DATA_TYPE.PATHOLOGY_REPORTS: {
        "coding": [
            {
                "system": "http://fhir.kids-first.io/CodeSystem/data-type",
                "code": "C28277",
                "display": "Pathology Report",
            }
        ],
        "text": GENOMIC_FILE.DATA_TYPE.PATHOLOGY_REPORTS,
    },
    GENOMIC_FILE.DATA_TYPE.ANNOTATED_SOMATIC_MUTATIONS: {
        "coding": [
            {
                "system": "http://fhir.kids-first.io/CodeSystem/data-type",
                "code": "C18060",
                "display": "Somatic Mutation",
            }
        ],
        "text": GENOMIC_FILE.DATA_TYPE.ANNOTATED_SOMATIC_MUTATIONS,
    },
    GENOMIC_FILE.DATA_TYPE.UNALIGNED_READS: {
        "coding": [
            {
                "system": "http://fhir.kids-first.io/CodeSystem/data-type",
                "code": "C164053",
                "display": "Unaligned Sequence Read",
            }
        ],
        "text": GENOMIC_FILE.DATA_TYPE.UNALIGNED_READS,
    },
}


class DocumentReference:
    """Render entity."""

    class_name = "document_reference"
    resource_type = "DocumentReference"

    @staticmethod
    def build_entity(blob):
        """Render entity."""
        # assert False, blob.attributes
        study_id = blob.sample.workspace_name
        genomic_file_id = join(study_id, blob.sample.id, blob.attributes.property_name)
        acl = None
        participant_id = blob.sample.subject_id
        size = blob.attributes.size
        url_list = [blob.attributes.name]
        file_name = blob.attributes.name.split('/')[-1]
        file_format = file_name.split('.')[-1]
        data_type = file_format

        entity = {
            "resourceType": DocumentReference.resource_type,
            "id": blob.attributes.name,
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/DocumentReference"
                ]
            },
            "identifier": [
                {
                    "system": f"https://kf-api-dataservice.kidsfirstdrc.org/genomic-files?study_id={study_id}&external_id=",
                    "value": genomic_file_id,
                },
                {
                    "system": "urn:kids-first:unique-string",
                    "value": genomic_file_id,
                },
            ],
            "status": "current",
        }

        if acl:
            entity.setdefault("extension", []).append(
                {
                    "extension": [
                        {
                            "url": "file-accession",
                            "valueIdentifier": {
                                "value": accession
                            },
                        }
                        for accession in acl
                    ],
                    "url": "http://fhir.kids-first.io/StructureDefinition/accession-identifier",
                }
            )

        if data_type:
            if data_type_dict.get(data_type):
                entity["type"] = data_type_dict[data_type]
            else:
                entity.setdefault('type', {})['text'] = data_type

        if participant_id:
            entity["subject"] = {
                "reference": f"Patient/{participant_id}"
            }

        content = {}

        if size:
            content.setdefault('attachment', {})["extension"] = [
                {
                    "url": "http://fhir.kids-first.io/StructureDefinition/large-size",
                    "valueDecimal": size,
                }
            ]

        if url_list:
            content.setdefault('attachment', {})["url"] = url_list[0]

        if file_name:
            content.setdefault('attachment', {})["title"] = file_name

        if file_format:
            content["format"] = {
                "display": file_format
            }

        if content:
            entity.setdefault("content", []).append(content)

        return entity

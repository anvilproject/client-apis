"""Represent fhir entity."""

from anvil.transformers.fhir import join, make_identifier
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
    def identifier(blob):
        """Make identifier."""
        sample_id_slug = make_identifier(blob.sample.id)
        return make_identifier(join(sample_id_slug, blob.attributes.property_name))

    @staticmethod
    def build_entity(blob, subject):
        """Render entity."""
        study_id = blob.sample.workspace_name
        subject_id = subject.id
        subject_id_slug = make_identifier('P', subject_id)

        genomic_file_id = DocumentReference.identifier(blob)
        # logging.getLogger(__name__).debug(f"\n\n\n\n\n{blob.attributes}\n\n\n\n\n")
        # AttrDict({'size': 17734638122, 'etag': 'CJaG//fR9+kCEAE=', 'crc32c': 'eqDkoQ==',
        # 'time_created': '2020-06-10T16:13:14.288000+00:00',
        # 'name': 'gs://fc-secure-004e5c03-d24d-4f7f-a26b-9fdc64b0ca3c/AnVIL_CMG_Broad_Muscle_KNC_WGS_Mar2020/RP-1687/WGS/192CP_ZS_1/v1/192CP_ZS_1.cram',
        # 'property_name': 'cram_path'})
        # acl = None
        # size = blob.attributes.size
        # url_list = [blob.attributes.name]
        file_name = blob.attributes.name.split('/')[-1]
        file_format = file_name.split('.')[-1]
        # data_type = file_format
        # time_created = blob.attributes.time_created

        entity = {
            "resourceType": DocumentReference.resource_type,
            "id": genomic_file_id,
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{study_id}",
                    "value": genomic_file_id,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": genomic_file_id,
                },
            ],
            "status": "current",
        }

        if subject_id_slug:
            entity["subject"] = {
                "reference": f"Patient/{subject_id_slug}"
            }

        content = {}

        # start attachment
        if 'ga4gh_drs_uri' in blob.attributes:
            entity["meta"] = {
                "profile": [
                    "http://fhir.ncpi-project-forge.io/StructureDefinition/ncpi-drs-document-reference"
                ]
            }
        content["attachment"] = {
            "url": blob.attributes.get('ga4gh_drs_uri', blob.attributes['name'])
        }

        # end attachment

        if file_format:
            content["format"] = {
                "display": file_format
            }

        if content:
            entity.setdefault("content", []).append(content)

        return entity

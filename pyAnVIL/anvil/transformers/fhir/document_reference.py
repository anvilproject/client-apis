"""Represent fhir entity."""

from anvil.transformers.fhir import make_id
from anvil.transformers.fhir.organization import Organization
from urllib.parse import urlsplit, urlunsplit

from anvil.transformers.fhir.patient import Patient


def strip_port(url):
    """Remove port from url (BUG in gen3)."""
    if not url:
        return url
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc.split(':')[0], parts.path, parts.query, parts.fragment,))


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
    def slug(blob):
        """Make id."""
        return make_id(blob.sample.workspace_name, blob.attributes.name)

    @staticmethod
    def build_entity(blob, subject):
        """Render entity."""
        study_id = blob.sample.workspace_name
        subject_slug = Patient.slug(subject)
        blob_slug = DocumentReference.slug(blob)
        # acl = None
        # size = blob.attributes.size
        # url_list = [blob.attributes.name]
        file_name = blob.attributes.name.split('/')[-1]
        file_format = file_name.split('.')[-1]
        # data_type = file_format
        # time_created = blob.attributes.time_created

        entity = {
            "resourceType": DocumentReference.resource_type,
            "id": blob_slug,
            "meta": {
                "profile": [
                    "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/ncpi-research-document-reference"
                ]
            },
            "identifier": [
                {
                    "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{study_id}",
                    "value": blob.attributes.name,
                },
                {
                    "system": "urn:ncpi:unique-string",
                    "value": blob_slug,
                },
            ],
            "status": "current",
            "custodian": {
                "reference": f"Organization/{Organization.slug(subject)}"
            }
        }

        if subject_slug:
            entity["subject"] = {
                "reference": f"Patient/{subject_slug}"
            }

        content = {}

        # start attachment
        if 'ga4gh_drs_uri' in blob.attributes:
            entity["meta"] = {
                "profile": [
                    "http://fhir.ncpi-project-forge.io/StructureDefinition/ncpi-drs-document-reference"
                ]
            }
        url = strip_port(blob.attributes.get('ga4gh_drs_uri', None))
        if not url:
            url = blob.attributes['name']
        content["attachment"] = {
            "url": url
        }

        # end attachment

        if file_format:
            content["format"] = {
                "display": file_format
            }

        if content:
            entity.setdefault("content", []).append(content)

        return entity

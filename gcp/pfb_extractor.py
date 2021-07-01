"""
Extracts .avro and creates .ndjson files in FHIR format
"""

import subprocess
import json
import os

from anvil.gen3.entities import Entities
from anvil.terra.reconciler import Reconciler
from anvil.terra.workspace import Workspace
from anvil.terra.sample import Sample
from anvil.transformers.fhir.transformer import FhirTransformer
from anvil.util.reconciler import DEFAULT_NAMESPACE

from dotenv import load_dotenv

# Load env variables
load_dotenv()
BILLING_PROJECT = os.getenv("GCP_PROJECT_ID")
AVRO_PATH = os.getenv("AVRO_PATH", "./export_1000_genomes.avro")
DASHBOARD_OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./data")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCP_LOCATION = os.getenv("GCP_LOCATION", "")
GCP_DATASET = os.getenv("GCP_DATASET", "")
GCP_DATASTORE = os.getenv("GCP_DATASTORE", "")
GCP_JSON_BUCKET = os.getenv("GCP_JSON_BUCKET", "")
SA_NAME = os.getenv("SA_NAME", "")


def reconcile_all(
    user_project,
    consortiums,
    namespace=DEFAULT_NAMESPACE,
    output_path=DASHBOARD_OUTPUT_PATH,
):
    """Reconcile and aggregate results

    e.g. bin/reconciler --user_project <your-billing-project> --consortium ThousandGenomes ^1000G-high-coverage-2019$
    --consortium CMG AnVIL_CMG.* --consortium CCDG AnVIL_CCDG.* --consortium GTEx ^AnVIL_GTEx_V8_hg38$
    """
    for (name, workspace_regex) in consortiums:
        print(f"Reconciling {name}...")
        reconciler = Reconciler(
            name, user_project, namespace, workspace_regex, AVRO_PATH
        )
        num_processed = 0
        for workspace in reconciler.workspaces:
            transformer = FhirTransformer(workspace=workspace)
            num_processed += 1
            print(
                f"Reconciled: {str(num_processed)}/{str(len(reconciler.workspaces))}"
            )
            for item in transformer.transform():
                yield item
        print("Reconciliation Completed!")


def append_drs(sample):
    """Add ga4gh_drs_uri to blob"""
    try:
        for key in sample.blobs.keys():
            filename = key.split("/")[-1]
            gen3_file = gen3_entities.get(submitter_id=filename)
            # f"https://gen3.theanvil.io/ga4gh/drs/v1/objects/{gen3_file['object']['object_id']}"
            sample.blobs[key]["ga4gh_drs_uri"] = gen3_file["object"][
                "ga4gh_drs_uri"
            ]
    except Exception as err:
        print(f"{err}: {sample.id}")


def all_instances(clazz):
    """Return all subjects"""
    print(
        "Starting aggregation for all AnVIL workspaces, this will take several minutes"
    )

    consortiums = (
        # ("CMG", "AnVIL_CMG_.*"),
        # ("CCDG", "AnVIL_CCDG_.*"),
        # ("GTEx", "^AnVIL_GTEx_V8_hg38$"),
        ("ThousandGenomes", "^1000G-high-coverage-2019$"),
    )

    for item in reconcile_all(
        user_project=BILLING_PROJECT, consortiums=consortiums
    ):
        if isinstance(item, Sample):
            append_drs(item)
        if clazz is None or isinstance(item, clazz):
            yield item


def save_all(workspaces):
    """Save all data to the file system"""
    emitters = {}
    entity = None

    workspace_exceptions = {}
    current_workspace = None

    num_workspaces = len(workspaces)
    workspace_index = 1
    for workspace in workspaces:
        print(
            f"Processing {str(workspace_index)}/{str(num_workspaces)}: {workspace.name}..."
        )
        workspace_index += 1

        current_workspace = workspace.name
        transformer = FhirTransformer(workspace=workspace)

        try:
            # Create output directory
            if not os.path.isdir(DASHBOARD_OUTPUT_PATH):
                os.mkdir(DASHBOARD_OUTPUT_PATH)

            for item in transformer.transform():
                for entity in item.entity():
                    resourceType = entity["resourceType"]
                    emitter = emitters.get(resourceType, None)
                    if emitter is None:
                        emitter = open(
                            f"{DASHBOARD_OUTPUT_PATH}/{resourceType}.ndjson",
                            "w+",
                        )
                        emitters[resourceType] = emitter
                    json.dump(entity, emitter, separators=(",", ":"))
                    emitter.write("\n")
        except Exception as e:
            if current_workspace not in workspace_exceptions:
                print(f"{e}: {current_workspace}")
                workspace_exceptions[current_workspace] = True
    for stream in emitters.values():
        stream.close()


def validate():
    """Check all validations exist"""
    print("Validating files...")
    FHIR_OUTPUT_PATHS = [
        f"{DASHBOARD_OUTPUT_PATH}/{p}"
        for p in """
    DocumentReference.ndjson
    Organization.ndjson
    Patient.ndjson
    Practitioner.ndjson
    ResearchStudy.ndjson
    ResearchSubject.ndjson
    Specimen.ndjson
    Task.ndjson""".split()
    ]

    for path in FHIR_OUTPUT_PATHS:
        if not os.path.isfile(path):
            err = f"{path} should exist"
            raise Exception(f"500 Internal Server Error: {err}")
        with open(path, "r") as inputs:
            for line in inputs.readlines():
                fhir_obj = json.loads(line)
                if not fhir_obj:
                    err = "{path} must be non-null"
                    raise Exception(f"500 Internal Server Error: {err}")
                print(f"Validated {path}")
                break
    print("Validated files!")


def main():
    # setup gcloud
    try:
        gcloud_cmd = f"gcloud auth activate-service-account {SA_NAME}@{GCP_PROJECT_ID}.iam.gserviceaccount.com --key-file=./creds.json"
        print(f"CMD: {gcloud_cmd}")
        process = subprocess.Popen(gcloud_cmd.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        print(f"OUTPUT: {output}")

        if error:
            raise Exception(error)
    except Exception as err:
        print(f"[Error] 500 Internal Server Error: {err}")
        return f"[Error] 500 Internal Server Error: {err}", 202

    # init AVRO file
    global gen3_entities
    gen3_entities = Entities(AVRO_PATH)

    # generate JSON
    print("Loading entities...")
    gen3_entities.load()
    workspaces = list(all_instances(Workspace))
    save_all(workspaces)
    print("Loaded entities!")
    validate()


if __name__ == "__main__":
    main()

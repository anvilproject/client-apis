import os
import json

from dotenv import load_dotenv
load_dotenv()

from anvil.gen3.entities import Entities
from anvil.terra.reconciler import Reconciler
from anvil.terra.workspace import Workspace
from anvil.terra.sample import Sample
from anvil.transformers.fhir.transformer import FhirTransformer
from anvil.util.reconciler import DEFAULT_NAMESPACE

# env constants
BILLING_PROJECT = os.getenv("GCP_BILLING_PROJECT")
AVRO_PATH = os.getenv("AVRO_PATH", './export_1000_genomes.avro')
OUTPUT_DIR = os.getenv("OUTPUT_PATH", "./data")

# generate initial entities with AVRO file
gen3_entities = Entities(AVRO_PATH)


def reconcile_all(user_project, consortiums, namespace=DEFAULT_NAMESPACE, output_path=OUTPUT_DIR):
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
            print(f"Reconciled: {str(num_processed)}/{str(len(reconciler.workspaces))}" )
            for item in transformer.transform():
                yield item
        print("[DONE]")


def append_drs(sample):
    """Add ga4gh_drs_uri to blob"""
    try:
        for key in sample.blobs.keys():
            filename = key.split('/')[-1]
            gen3_file = gen3_entities.get(submitter_id=filename)
            # f"https://gen3.theanvil.io/ga4gh/drs/v1/objects/{gen3_file['object']['object_id']}"
            sample.blobs[key]['ga4gh_drs_uri'] = gen3_file['object']['ga4gh_drs_uri']
    except Exception as e:
        print(f"[Error] {e}:", sample.id)


def all_instances(clazz):
    """Return all subjects"""
    print("Starting aggregation for all AnVIL workspaces, this will take several minutes")

    consortiums = (
        ('ThousandGenomes', '^1000G-high-coverage-2019$'),
    )

    for item in reconcile_all(user_project=BILLING_PROJECT, consortiums=consortiums):
        if isinstance(item, Sample):
            append_drs(item)
        if clazz is None or isinstance(item, clazz):
            yield item


def save_summary(workspace, emitter):
    """Save a workspace summary for downstream QA"""
    try:
        for subject in workspace.subjects:
            for sample in subject.samples:
                for property, blob in sample.blobs.items():
                    json.dump(
                        {
                            "workspace_id": workspace.id,
                            "subject_id": subject.id,
                            "sample_id": sample.id,
                            "blob": blob['name']
                        },
                        emitter,
                        separators=(',',':')
                    )
                    emitter.write('\n')
    except:
        print("Summary save failed")


def save_all(workspaces):
    """Save all data to the file system"""
    emitters = {}
    entity = None

    workspace_exceptions = {}
    current_workspace = None
    summary_emitter = open(f"{OUTPUT_DIR}/terra_summary.json", "w+")

    num_workspaces = len(workspaces)
    workspace_index = 1
    for workspace in workspaces:
        print(f"Processing {str(workspace_index)}/{str(num_workspaces)}: {workspace.name}...")
        workspace_index += 1

        current_workspace = workspace.name
        transformer = FhirTransformer(workspace=workspace)
        save_summary(workspace, summary_emitter)
        
        try:
            for item in transformer.transform():
                for entity in item.entity():
                    resourceType = entity['resourceType']
                    emitter = emitters.get(resourceType, None)
                    if emitter is None:
                        emitter = open(f"{OUTPUT_DIR}/{resourceType}.json", "w+")
                        emitters[resourceType] = emitter
                    json.dump(entity, emitter, separators=(',', ':'))
                    emitter.write('\n')
        except Exception as e:
            if current_workspace not in workspace_exceptions:
                print(f"[Error] {e}", {current_workspace})
                workspace_exceptions[current_workspace] = True
    for stream in emitters.values():
        stream.close()
    summary_emitter.close()


def validate():
    """Check all validations exist"""
    FHIR_OUTPUT_PATHS = [f"{OUTPUT_DIR}/{p}" for p in """
    DocumentReference.json
    Organization.json
    Patient.json
    Practitioner.json
    ResearchStudy.json
    ResearchSubject.json
    Specimen.json
    Task.json""".split()]

    for path in FHIR_OUTPUT_PATHS:
        assert os.path.isfile(path), f"{path} should exist"
        with open(path, 'r') as inputs:
            for line in inputs.readlines():
                fhir_obj = json.loads(line)
                assert fhir_obj, f"json de-serialization failed {line}"
                break


# generate JSON
print("Loading entities...")
gen3_entities.load()
workspaces = list(all_instances(Workspace))
save_all(workspaces)
validate()
print("Entities loaded")
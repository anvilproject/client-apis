"""Extract all workspaces."""
import sqlite3
import os
import logging
from anvil.gen3.entities import Entities
from anvil.terra.reconciler import Reconciler
from anvil.terra.workspace import Workspace
from anvil.terra.sample import Sample
# from anvil.terra.subject import Subject
from anvil.transformers.fhir.transformer import FhirTransformer
from anvil.util.reconciler import DEFAULT_NAMESPACE

import pandas
import upsetplot
import matplotlib.pyplot
from collections import defaultdict

import requests
from requests.auth import HTTPBasicAuth

import json

logging.basicConfig(level=logging.WARN, format='%(asctime)s %(levelname)-8s %(message)s')
DASHBOARD_OUTPUT_PATH = "/tmp"
gen3_entities = Entities("/Users/walsbr/Downloads/export_2020-11-04T17_48_47.avro")


def reconcile_all(user_project, consortiums, namespace=DEFAULT_NAMESPACE, output_path=DASHBOARD_OUTPUT_PATH):
    """Reconcile and aggregate results.

    e.g. bin/reconciler --user_project <your-billing-project>  --consortium CMG AnVIL_CMG.* --consortium CCDG AnVIL_CCDG.* --consortium GTEx ^AnVIL_GTEx_V8_hg38$ --consortium ThousandGenomes ^1000G-high-coverage-2019$
    """
    for (name, workspace_regex) in consortiums:
        reconciler = Reconciler(name, user_project, namespace, workspace_regex)
        for workspace in reconciler.workspaces:
            transformer = FhirTransformer(workspace=workspace)
            for item in transformer.transform():
                yield item


def append_drs(sample):
    """Add ga4gh_drs_uri to blob."""
    try:
        for key in sample.blobs.keys():
            filename = key.split('/')[-1]
            gen3_file = gen3_entities.get(submitter_id=filename)
            sample.blobs[key]['ga4gh_drs_uri'] = f"https://gen3.theanvil.io/ga4gh/drs/v1/objects/{gen3_file['object']['ga4gh_drs_uri']}"
    except Exception as e:
        logging.info(f"Error sample: {sample.id} {e}")


def all_instances(clazz):
    """Return all subjects."""
    logging.info("Starting aggregation for all AnVIL workspaces, this will take several minutes.")

    consortiums = (
        ('CMG', 'AnVIL_CMG_.*'),
        ('CCDG', 'AnVIL_CCDG_.*'),
        ('GTEx', '^AnVIL_GTEx_V8_hg38$'),
        ('ThousandGenomes', '^1000G-high-coverage-2019$')
    )
    for item in reconcile_all(user_project=os.environ['GOOGLE_PROJECT'], consortiums=consortiums):
        if isinstance(item, Sample):
            append_drs(item)
        if clazz is None or isinstance(item, clazz):
            yield item


def entity_plots(clazz):
    """Plot upsert figures of entity in all terra workspaces."""
    if (clazz == Workspace):
        clazz_dictionaries = {s.id: set(s.attributes.workspace.attributes.keys()) for s in all_instances(clazz)}
    else:
        clazz_dictionaries = {s.workspace_name: set(s.attributes.attributes.keys()) for s in all_instances(clazz)}
    sample_df = pandas.DataFrame(upsetplot.from_contents(clazz_dictionaries))
    upsetplot.plot(sample_df, sort_by="cardinality", sum_over=False, show_counts='%d')
    current_figure = matplotlib.pyplot.gcf()
    current_figure.suptitle(f'Count of shared {clazz.__name__} properties')
    current_figure.savefig(f"{clazz.__name__}_projects.png")
    entity_by_project = defaultdict(set)
    for workspace_name, keys in clazz_dictionaries.items():
        for k in keys:
            entity_by_project[k].add(workspace_name)
    entity_df = pandas.DataFrame(upsetplot.from_contents(entity_by_project))
    upsetplot.plot(entity_df, sort_by="cardinality", sum_over=False, show_counts='%d')
    current_figure = matplotlib.pyplot.gcf()
    current_figure.set_size_inches(10.5, 40.5)
    current_figure.suptitle(f'"{clazz.__name__}" Count of shared attribute names')
    current_figure.savefig(f"{clazz.__name__}_attributes.png")
    return clazz_dictionaries


def contains(strings, matches):
    """Find true if match in string."""
    for s in strings:
        for m in matches:
            if m.lower() in s.lower():
                return True
    return False


# subject_dictionaries = entity_plots(Subject)

# missing_age = [(k, ','.join(sorted(v))) for k, v in subject_dictionaries.items() if not contains(v, ['age'])]

# missing_sex = [(k, ','.join(sorted(v))) for k, v in subject_dictionaries.items() if not contains(v, ['sex', 'gender'])]

# print('missing subject age field')
# print('projects', [s[0] for s in missing_age])
# print('dictionaries', set([s[1] for s in missing_age]))

# print('missing subject sex field')
# print('projects', [s[0] for s in missing_sex])
# print('dictionaries', set([s[1] for s in missing_sex]))


FHIR_API = os.getenv("FHIR_API") or "http://localhost:8000"
FHIR_USER = os.getenv("FHIR_USER") or "admin"
FHIR_PW = os.getenv("FHIR_PW") or "password"


def config():
    """Configure context used for all tests."""
    session = requests.session()
    session.auth = HTTPBasicAuth(FHIR_USER, FHIR_PW)
    session.headers = {"Content-Type": "application/fhir+json"}

    class Config:
        """Store config in class."""

        base_url = FHIR_API
        connection = session

    return Config()


def load_all(workspaces):
    """Load all data to the FHIR server."""
    _config = config()
    for workspace in workspaces:
        transformer = FhirTransformer(workspace=workspace)
        for item in transformer.transform():
            for entity in item.entity():
                resourceType = entity['resourceType']
                id = entity['id']
                url = f"{_config.base_url}/{resourceType}/{id}"
                response = _config.connection.put(
                    url=url,
                    json=entity,
                )
                assert response.ok, f"url: {url}\nbody: {entity}\nerror: {response.text}"
                response_body = response.json()
                logging.getLogger(__name__).debug(f"created {resourceType}/{response_body['id']} at {url}")


def save_all(workspaces):
    """Save all data to the file system."""
    emitters = {}
    entity = None

    workspace_exceptions = {}
    current_workspace = None
    for workspace in workspaces:
        current_workspace = workspace.name
        transformer = FhirTransformer(workspace=workspace)
        try:
            for item in transformer.transform():
                for entity in item.entity():
                    resourceType = entity['resourceType']
                    emitter = emitters.get(resourceType, None)
                    if emitter is None:
                        emitter = open(f"{DASHBOARD_OUTPUT_PATH}/{resourceType}.json", "w")
                        emitters[resourceType] = emitter
                    json.dump(entity, emitter, separators=(',', ':'))
                    emitter.write('\n')
        except Exception as e:
            if current_workspace not in workspace_exceptions:
                logging.getLogger(__name__).warning(f"{current_workspace} {e}")
                workspace_exceptions[current_workspace] = True
    for stream in emitters.values():
        stream.close()


def load_all_files():
    """Load all data to the FHIR server."""
    _config = config()
    for resourceType in ['Practitioner', 'Organization', 'ResearchStudy', 'Patient', 'ResearchSubject', 'Specimen', 'DocumentReference', 'Task']:
        with open(f"{DASHBOARD_OUTPUT_PATH}/{resourceType}.json", "r") as inputs:
            for line in inputs.readlines():
                entity = json.loads(line)
                id = entity['id']
                url = f"{_config.base_url}/{resourceType}/{id}"
                response = _config.connection.put(
                    url=url,
                    json=entity,
                )
                assert response.ok, f"body:{json.dumps(entity)}\nerror: {response.text}"


def all_schemas():
    """Return all subjects."""
    logging.info("Starting aggregation for all AnVIL workspaces, this will take several minutes.")
    consortiums = (
        ('CMG', 'AnVIL_CMG_.*'),
        ('CCDG', 'AnVIL_CCDG_.*'),
        ('GTEx', '^AnVIL_GTEx_V8_hg38$'),
        ('ThousandGenomes', '^1000G-high-coverage-2019$')
    )
    for (name, workspace_regex) in consortiums:
        reconciler = Reconciler(name, user_project=os.environ['GOOGLE_PROJECT'], namespaces=DEFAULT_NAMESPACE, project_pattern=workspace_regex)
        yield reconciler.reconcile_schemas()


# emitter = open(f"{DASHBOARD_OUTPUT_PATH}/terra_summary.json", "w")
# for workspace in all_instances(Workspace):
#     for subject in workspace.subjects:
#         for sample in subject.samples:
#             for property, blob in sample.blobs.items():
#                 json.dump(
#                     {
#                         "workspace_id": workspace.id,
#                         "subject_id": subject.id,
#                         "sample_id": sample.id,
#                         "blob": blob['name'],
#                     },
#                     emitter,
#                     separators=(',', ':')
#                 )
#                 emitter.write('\n')
# emitter.close()

# _conn = sqlite3.connect('/tmp/gen3-drs.sqlite')
# cur = _conn.cursor()
# cur.executescript("""
#         CREATE TABLE IF NOT EXISTS terra_details (
#             workspace_id text,
#             subject_id text,
#             sample_id text,
#             blob text
#         );
#         """)
# _conn.commit()
# cur = _conn.cursor()
# with open(f"{DASHBOARD_OUTPUT_PATH}/terra_summary.json", 'rb') as fo:
#     for line in fo.readlines():
#         record = json.loads(line)
#         cur.execute("REPLACE into terra_details values (?, ?, ?, ?);", (record['workspace_id'], record['subject_id'], record['sample_id'], record['blob'],))
# _conn.commit()

# cur.executescript("""
# CREATE UNIQUE INDEX IF NOT EXISTS terra_details_idx ON terra_details(workspace_id, subject_id, sample_id, blob);
# """)
# _conn.commit()

# workspaces = list(all_instances(Workspace))
# save_all(workspaces)

load_all_files()

# family_relationships = set()
# for w in workspaces:
#     for s in w.subjects:
#         if 'Family_Relationship' in s.attributes.attributes:
#             family_relationships.add(s.attributes.attributes['Family_Relationship'])

# schemas = all_schemas()

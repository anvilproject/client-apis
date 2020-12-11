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

import concurrent.futures
CONNECTIONS = 1 # 100
TIMEOUT = 50

import requests
from requests.auth import HTTPBasicAuth

import json

logging.basicConfig(level=logging.WARN, format='%(asctime)s %(levelname)-8s %(message)s')
DASHBOARD_OUTPUT_PATH = "/tmp"
gen3_entities = Entities("/Users/walsbr/Downloads/export_2020-11-04T17_48_47.avro")

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


def put(connection, url, entity):
    """Write entity to connection."""
    response = connection.put(
        url=url,
        json=entity,
    )
    assert response.ok, f"body:{json.dumps(entity)}\nerror: {response.text}"


def entity_reader(_config, resourceType, inputs):
    """Read inputs, yield entity."""
    for line in inputs.readlines():
        entity = json.loads(line)
        id = entity['id']
        url = f"{_config.base_url}/{resourceType}/{id}"
        yield (url, entity, )


def load_all_files():
    """Load all data to the FHIR server."""
    _config = config()
    for resourceType in ['Practitioner', 'Organization', 'ResearchStudy', 'Patient', 'ResearchSubject', 'Specimen', 'Observation', 'DocumentReference', 'Task']:
        if not os.path.isfile(f"{DASHBOARD_OUTPUT_PATH}/{resourceType}.json"):
            print(f"Loading {resourceType} missing")
            continue
        with open(f"{DASHBOARD_OUTPUT_PATH}/{resourceType}.json", "r") as inputs:
            print(f"Loading {resourceType}")

            with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
                future_to_url = (executor.submit(put, _config.connection, url, entity) for url, entity in entity_reader(_config, resourceType, inputs))
                for future in concurrent.futures.as_completed(future_to_url):
                    try:
                        data = future.result()
                    except Exception as exc:
                        logging.getLogger(__name__).error(f"{exc}")
                        exit()


load_all_files()

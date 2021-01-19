"""Extract all workspaces."""
import os
import logging
import glob
import concurrent.futures
import requests
from requests.auth import HTTPBasicAuth
import json

# TODO - smilecdr throws exceptions when multiple connections (validating each request)


CONNECTIONS = 10
TIMEOUT = 50


logging.basicConfig(level=logging.WARN, format='%(asctime)s %(levelname)-8s %(message)s')
DASHBOARD_OUTPUT_PATH = "/tmp/ThousandGenomes"


FHIR_API = os.getenv("FHIR_API") or "http://localhost:8000"
FHIR_USER = os.getenv("FHIR_USER") or "admin"
FHIR_PW = os.getenv("FHIR_PW") or "password"

# see https://github.com/ncpi-fhir/ncpi-api-fhir-service
FHIR_COOKIE = os.getenv("FHIR_COOKIE") or None


def config():
    """Configure context used for all tests."""
    session = requests.session()
    if not FHIR_COOKIE:
        session.auth = HTTPBasicAuth(FHIR_USER, FHIR_PW)
    session.headers = {
        "Content-Type": "application/fhir+json",
        "accept": "application/fhir+json;charset=utf-8"
    }
    if FHIR_COOKIE:
        session.headers["cookie"] = f"AWSELBAuthSessionCookie-0={FHIR_COOKIE}"

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
    try:
        response.json()
    except Exception as ex:
        logging.error(f"url:{url}\nbody:{json.dumps(entity)}\nerror: {response.text}\n{ex}")


def entity_reader(_config, resourceType, inputs):
    """Read inputs, yield entity."""
    for line in inputs.readlines():
        entity = json.loads(line)
        id = entity['id']
        url = f"{_config.base_url}{resourceType}/{id}"
        yield (url, entity, )


def load_all_files():
    """Load all data to the FHIR server."""
    _config = config()
    for resourceType in ['Practitioner', 'Organization', 'ResearchStudy', 'Patient', 'ResearchSubject', 'Specimen', 'Observation', 'DocumentReference', 'Task']:
        path = f"{DASHBOARD_OUTPUT_PATH}/**/{resourceType}.json"
        paths = glob.glob(path, recursive=True)
        if len(paths) == 0:
            print(f"Loading {resourceType} missing")
        for path in paths:
            with open(path, "r") as inputs:
                print(f"Loading {path}")

                with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
                    future_to_url = (executor.submit(put, _config.connection, url, entity) for url, entity in entity_reader(_config, resourceType, inputs))
                    for future in concurrent.futures.as_completed(future_to_url):
                        try:
                            data = future.result()
                            assert data, "Future should return result"
                        except Exception as exc:
                            logging.getLogger(__name__).error(f"{exc}")


def get(connection, url):
    """Read entity from connection."""
    print(connection.headers)
    print(url)
    response = connection.get(
        url=url
    )
    assert response.ok, f"url:{url}\nerror: {response.text}"
    print(response)
    print(response.text)
    return response.json()


def read_all(resourceTypes):
    """Load all data to the FHIR server."""
    _config = config()
    for resourceType in resourceTypes:
        url = f"{_config.base_url}{resourceType}"
        entity = get(_config.connection, url)
        assert entity, f"{url} should return entity"


# TODO - add cli handler
load_all_files()
# read_all(['ResearchStudy'])

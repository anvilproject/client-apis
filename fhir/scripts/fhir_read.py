"""Extract all workspaces."""
import os
import logging
import requests
from requests.auth import HTTPBasicAuth
import json

# TODO - smilecdr throws exceptions when multiple connections (validating each request)


CONNECTIONS = 10
TIMEOUT = 50


logging.basicConfig(level=logging.WARN, format='%(asctime)s %(levelname)-8s %(message)s')


FHIR_API = os.getenv("FHIR_API") or "http://localhost:8000"
FHIR_USER = os.getenv("FHIR_USER") or "admin"
FHIR_PW = os.getenv("FHIR_PW") or "password"

# see https://github.com/ncpi-fhir/ncpi-api-fhir-service
# in a nutshell:
# * visit https://ncpi-api-fhir-service-dev.kidsfirstdrc.org/metadata with your browser
# * auth using your google id
# * use the browser's dev tools to retrieve the AWSELBAuthSessionCookie-0=
# * * set the env's FHIR_COOKIE
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


def entity_reader(_config, resourceType, inputs):
    """Read inputs, yield entity."""
    for line in inputs.readlines():
        entity = json.loads(line)
        id = entity['id']
        url = f"{_config.base_url}{resourceType}/{id}"
        yield (url, entity, )


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
read_all(['ResearchStudy'])

"""Setup test context."""
import logging
import os
import json
import pytest
import requests
import glob
from requests.auth import HTTPBasicAuth
import time

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))

RESOURCE_DIR = os.path.join('ncpi-model-forge', 'site_root', 'input', 'resources')
TEST_DIR = os.path.dirname(__file__)
# TEST_DATA_DIR = os.path.join(TEST_DIR, "data")
FHIR_API = os.getenv("FHIR_API") or "http://localhost:8000"
FHIR_USER = os.getenv("FHIR_USER") or "admin"
FHIR_PW = os.getenv("FHIR_PW") or "password"

logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    """Add command line options."""
    parser.addoption(
        "--user_project", action="store", default=None, help="user's billing project"
    )
    parser.addoption(
        "--namespaces", action="store", default='anvil-datastorage', help="terra namespaces"
    )
    parser.addoption(
        "--project_pattern", action="store", default=None, help="regexp filter applied to workspace name"
    )


@pytest.fixture
def user_project(request):
    """Return command line options as fixture."""
    return request.config.getoption("--user_project")


@pytest.fixture
def namespaces(request):
    """Return command line options as fixture."""
    return request.config.getoption("--namespaces")


@pytest.fixture
def project_pattern(request):
    """Return command line options as fixture."""
    return request.config.getoption("--project_pattern")


@pytest.fixture(scope="function")
def debug_caplog(caplog):
    """Pytest capture log output at level=DEBUG."""
    caplog.set_level(logging.DEBUG)
    print('set caplog')
    return caplog


@pytest.fixture(scope="session")
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


def loader(path):
    """Load and close helper."""
    with open(path, "rb") as input_stream:
        return json.load(input_stream)


@pytest.fixture(scope="session")
def extensions():
    """Load extension paths."""
    return [(loader(path), path) for path in glob.glob(os.path.join(RESOURCE_DIR, "extensions", "*.json"))]


@pytest.fixture(scope="session")
def profiles():
    """Load profile paths."""
    return [(loader(path), path) for path in glob.glob(os.path.join(RESOURCE_DIR, "profiles", "*.json"))]


@pytest.fixture(scope="session")
def examples():
    """Load example paths."""
    return [(loader(path), path) for path in glob.glob(os.path.join(RESOURCE_DIR, "examples", "*.json"))]


@pytest.fixture(scope="session")
def codesystems():
    """Load terminology.codesystems paths."""
    return [(loader(path), path) for path in glob.glob(os.path.join(RESOURCE_DIR, "terminology", "CodeSystem*.json"))]


@pytest.fixture(scope="session")
def valuesets():
    """Load terminology.valuesets paths."""
    return [(loader(path), path) for path in glob.glob(os.path.join(RESOURCE_DIR, "terminology", "ValueSet*.json"))]


@pytest.fixture(scope="session")
def load_configurations(config, extensions, profiles, examples, codesystems, valuesets):
    """Load customizations from IG into server.

    This is done once before any tests run.
    """
    # return list of urls configured on server
    config_resource_urls = []

    for extension, path in extensions:
        id = extension['id']
        url = f"{config.base_url}/StructureDefinition/{id}"
        response = config.connection.put(
            url=url,
            json=extension,
        )
        assert response.ok, f"body:{extension}\nerror: {response.text}"
        response_body = response.json()
        logger.debug(f"created {url} from {path}")
        config_resource_urls.append(url)

    for profile, path in profiles:
        id = profile['id']
        url = f"{config.base_url}/StructureDefinition/{id}"
        response = config.connection.put(
            url=url,
            json=profile,
        )
        assert response.ok, f"body:{profile}\nerror: {response.text}"
        response_body = response.json()
        logger.debug(f"created {url} from {path}")
        config_resource_urls.append(url)

    for codesystem, path in codesystems:
        id = codesystem['id']
        url = f"{config.base_url}/CodeSystem/{id}"
        response = config.connection.put(
            url=url,
            json=codesystem,
        )
        assert response.ok, f"body:{codesystem}\nerror: {response.text}"
        response_body = response.json()
        logger.info(f"created {url} from {path}")
        print(f"created {url} from {path}")
        config_resource_urls.append(url)

    for valueset, path in valuesets:
        id = valueset['id']
        url = f"{config.base_url}/ValueSet/{id}"
        response = config.connection.put(
            url=url,
            json=valueset,
        )
        assert response.ok, f"body:{valueset}\nerror: {response.text}"
        response_body = response.json()
        logger.info(f"created {url} from {path}")
        print(f"created {url} from {path}")
        config_resource_urls.append(url)

    time.sleep(60)
    for example_type in ['Organization', 'Practitioner', 'PractitionerRole', 'ResearchStudy', 'Patient', 'ResearchSubject', 'DocumentReference', 'Specimen', 'Observation']:
        for example, path in examples:
            if example_type not in path:
                continue
            print(f"created example from {path}")
            resourceType = example['resourceType']
            id = example['id']
            url = f"{config.base_url}/{resourceType}/{id}"
            response = config.connection.put(
                url=url,
                json=example,
            )
            assert response.ok, f"path:{json.dumps(path)}\nbody:{json.dumps(example)}\nerror: {response.text}"
            response_body = response.json()
            logger.debug(f"created {resourceType}/{response_body['id']} from {path}")
            config_resource_urls.append(url)

    return config_resource_urls

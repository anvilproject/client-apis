"""Provide test fixtures."""

import pytest
from anvil.clients.gen3_auth import TERRA_TOKEN_URL


def pytest_addoption(parser):
    """Add command line options."""
    parser.addoption(
        "--terra_auth_url", action="store", default=TERRA_TOKEN_URL, help="Full url for terra fence/accesstoken endpoint"
    )
    parser.addoption(
        "--user_email", action="store", default=None, help="gmail account registered with terra (None will use default)"
    )
    parser.addoption(
        "--gen3_endpoint", action="store", default="https://gen3.theanvil.io", help="gen3 endpoint"
    )
    parser.addoption(
        "--user_project", action="store", default=None, help="user's billing project"
    )
    parser.addoption(
        "--namespaces", action="store", default='anvil-datastorage', help="terra namespaces"
    )
    parser.addoption(
        "--project_pattern", action="store", default=None, help="regexp filter applied to workspace name"
    )
    parser.addoption(
        "--output_path", action="store", default="/tmp", help="Where to find cache"
    )
    parser.addoption(
        "--avro_path", action="store", default=None, help="Where to find avro"
    )

@pytest.fixture
def terra_auth_url(request):
    """Return command line options as fixture."""
    return request.config.getoption("--terra_auth_url")


@pytest.fixture
def user_email(request):
    """Return command line options as fixture."""
    return request.config.getoption("--user_email")


@pytest.fixture
def gen3_endpoint(request):
    """Return command line options as fixture."""
    return request.config.getoption("--gen3_endpoint")


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

@pytest.fixture
def output_path(request):
    """Return command line options as fixture."""
    return request.config.getoption("--output_path")

@pytest.fixture
def avro_path(request):
    """Return command line options as fixture."""
    return request.config.getoption("--avro_path")

@pytest.fixture
def dashboard_output_path(output_path):
    """Return command line options as fixture."""
    return f"{output_path}/data_dashboard.json"

@pytest.fixture
def terra_output_path(output_path):
    """Return command line options as fixture."""
    return f"{output_path}/terra.sqlite"

@pytest.fixture
def drs_output_path(output_path):
    """Return command line options as fixture."""
    return f"{output_path}/gen3-drs.sqlite"

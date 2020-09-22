"""Provide test fixtures."""

import pytest
from anvil.gen3_auth import TERRA_TOKEN_URL


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

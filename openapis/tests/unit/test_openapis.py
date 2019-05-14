"""Validate combined swagger document created by swagger-combine."""

import os.path
import pytest
import yaml

@pytest.fixture
def configuration_path():
    """File created by bin/combine."""
    return 'config/swagger-combine.yml'


@pytest.fixture
def combined_apis_path():
    """File created by bin/combine."""
    return 'output/combined-apis.yml'


@pytest.fixture
def combined_apis(combined_apis_path):
    """Load file created by bin/combine. note: big file, takes >6 secs to load"""
    with open(combined_apis_path, 'r') as stream:
        return yaml.load(stream, Loader=yaml.FullLoader)


@pytest.fixture
def configuration(configuration_path):
    """Load our configuration."""
    with open(configuration_path, 'r') as stream:
        return yaml.load(stream, Loader=yaml.FullLoader)

@pytest.fixture
def expected_base_paths(configuration):
    """API base paths e.g ['/api/indexd', '/api/leo', '/api/sheepdog', '/api/peregrine', '/api/dockstore']."""
    return [api['paths']['base'] for api in configuration['apis']]


def test_file_exists(combined_apis_path):
    """File should exist."""
    assert os.path.isfile(combined_apis_path), '{} should exist'.format(combined_apis_path)


def test_urls_paths_exists(combined_apis, expected_base_paths):
    """API URL paths should exist."""
    paths = combined_apis['paths'].keys()
    for expected_base_path in expected_base_paths:
        assert len([path for path in paths if path.startswith(expected_base_path)]), '{} should exist'.format(expected_base_path)



def test_provenance_exists(expected_base_paths):
    """Provenance files should exist."""
    for expected_base_path in expected_base_paths:
        service = expected_base_path.split('/')[-1]
        provenance_file = os.path.join('provenance','{}.commit'.format(service))
        assert os.path.isfile(provenance_file), '{} should exist'.format(provenance_file)


def test_endpoint_counts(combined_apis, expected_base_paths):
    """API counts ."""
    actual = {}
    paths = combined_apis['paths'].keys()
    for expected_base_path in expected_base_paths:
        actual[expected_base_path] = len([path for path in paths if path.startswith(expected_base_path)])
    expected = {
        '/api/dockstore': 172,
        '/api/indexd': 18,
        '/api/leo': 17,
        '/api/peregrine': 6,
        '/api/sheepdog': 41
    }
    assert actual == expected

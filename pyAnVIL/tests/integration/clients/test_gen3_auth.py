"""Test gen3_auth."""
import os

import pytest

from anvil.clients.gen3_auth import Gen3TerraAuth
from anvil.clients.gen3_auth import TERRA_TOKEN_URL
from gen3.submission import Gen3Submission
from gen3.auth import Gen3Auth

import logging
logging.getLogger('anvil.test_gen3_auth').setLevel(logging.DEBUG)
logger = logging.getLogger('anvil.test_gen3_auth')


@pytest.fixture
def submission_client(terra_auth_url, user_email, gen3_endpoint):
    """Query terra_auth_url for access token and ensure inserted onto all gen3 requests."""
    auth = Gen3TerraAuth(endpoint=gen3_endpoint, terra_auth_url=terra_auth_url, user_email=user_email)
    return Gen3Submission(gen3_endpoint, auth)


@pytest.fixture
def native_submission_client(gen3_endpoint):
    """Query terra_auth_url for access token and ensure inserted onto all gen3 requests."""
    # Install n API Key downloaded from the
    # commons' "Profile" page at ~/.gen3/credentials.json
    path = os.path.expanduser("~/.gen3/credentials.json")
    if not os.path.isfile(path):
        logger.warning(f"No gen3 credentials file found at {path}")
    else:
        from datetime import datetime, timezone
        from pathlib import Path
        stat_result = Path(path).stat()
        modified = datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc)
        now = datetime.now(tz=timezone.utc)
        delta = now - modified
        if delta.days > 30:
            logger.warning(f"Gen3 credentials file found at {path} are over 30 days old")
    auth = Gen3Auth(endpoint=gen3_endpoint)
    return Gen3Submission(gen3_endpoint, auth)


def test_get_graphql_schema(submission_client):
    """Read schema (AFAIK, gen3 does not check auth)."""
    schema = submission_client.get_graphql_schema()
    assert schema, "MUST be able to access open url"
    # save for downstream development
    # with open("schema.json", "w") as outs:
    #     json.dump(schema, outs)


def test_get_programs(submission_client):
    """Read programs list (AFAIK, gen3 does not check auth)."""
    programs = submission_client.get_programs()
    assert len(programs) > 0, 'MUST have at least one program'


def test_get_projects(submission_client):
    """Read project list."""
    programs = submission_client.get_programs()
    # >>> {'links': ['/v0/submission/open_access', '/v0/submission/CF']}
    programs = [p.split('/')[-1] for p in programs['links']]
    for program in programs:
        projects = submission_client.get_projects(program)
        assert len(projects) > 0, f'MUST have at least one project {program}'
        # >>> {'links': ['/v0/submission/open_access/1000Genomes']}
        # >>> {'links': ['/v0/submission/CF/GTEx']}


def test_query(caplog, submission_client):
    caplog.set_level(logging.DEBUG)
    """Submit simple graphql query."""
    query = '{project(first:0) {code,  subjects {submitter_id}, programs {name}  }}'
    results = submission_client.query(query)
    logger.debug(f"graphql {results}")
    assert len(results['data']['project']) > 1, "MUST have more than one project"
    for project in results['data']['project']:
        if len(project['subjects']) == 0:
            logger.warning(f"gen3 {project['code']} SHOULD have subjects?")
    program_names = set()
    for project in results['data']['project']:
        for program in project['programs']:
            program_names.add(program['name'])
    assert len(program_names) > 1, "MUST have more than one program"


@pytest.mark.skip(reason="TODO: no manifests returned?")
def test_get_project_manifest(submission_client):
    """Read program/project list, request manifest for each."""
    programs = submission_client.get_programs()
    # >>> {'links': ['/v0/submission/open_access', '/v0/submission/CF']}
    programs = [p.split('/')[-1] for p in programs['links']]
    failed_manifests = []
    for program in programs:
        projects = submission_client.get_projects(program)
        # >>> {'links': ['/v0/submission/open_access/1000Genomes']}
        # >>> {'links': ['/v0/submission/CF/GTEx']}
        for project in projects['links']:
            project_code = project.split('/')[-1]
            logger.debug(f"parameters to get_project_manifest: program {program}, project_code {project_code}")
            project_manifest = submission_client.get_project_manifest(program, project_code)
            # assert project_manifest, f"MUST have manifest {program}/{project_code}"
            if not project_manifest:
                failed_manifests.append(f"MUST have manifest {program}/{project_code}")
            logger.debug(project_manifest)
    assert len(failed_manifests) == 0, failed_manifests


def test_get_project_dictionary(submission_client):
    """Read program/project list, request dictionary for each."""
    programs = submission_client.get_programs()
    # >>> {'links': ['/v0/submission/open_access', '/v0/submission/CF']}
    programs = [p.split('/')[-1] for p in programs['links']]
    for program in programs:
        projects = submission_client.get_projects(program)
        # >>> {'links': ['/v0/submission/open_access/1000Genomes']}
        # >>> {'links': ['/v0/submission/CF/GTEx']}
        for project in projects['links']:
            project_code = project.split('/')[-1]
            logger.debug(f"parameters to get_project_dictionary: program {program}, project_code {project_code}")
            project_dictionary = submission_client.get_project_dictionary(program, project_code)
            assert project_dictionary, f"MUST have manifest {program}/{project_code}"


def test_get_dictionary_all(submission_client):
    """Read program/project list, requests dictionary for each."""
    dictionary_all = submission_client.get_dictionary_all()
    logger.debug(dictionary_all.keys())
    for k in ['core_metadata_collection', 'data_release', 'discovery', 'family', 'metaschema', 'program', 'project', 'root', 'sample', 'sequencing', 'subject']:
        assert k in dictionary_all, f'dictionary MUST contain {k}'


def test_project_access(native_submission_client, submission_client):
    """Native token and token proxied by terra should return same project list."""
    query = """{
        project(first: 1000) {
            code
        }
    }"""
    proxy_token_results = submission_client.query(query)
    logger.debug(f"graphql {proxy_token_results}")
    native_results = native_submission_client.query(query)
    logger.debug(f"graphql {native_results}")
    assert set([p['code'] for p in proxy_token_results['data']['project']]) == set([p['code'] for p in native_results['data']['project']]), "MUST return same set of projects,  You may need to re-link your account."

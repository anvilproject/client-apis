"""Test terra wrapper."""

from anvil.terra import get_projects, whoami
import logging

logging.getLogger('anvil.test_terra').setLevel(logging.DEBUG)
logger = logging.getLogger('anvil.test_terra')


def test_whoami(user_email):
    """Ensure terra."""
    me = whoami()
    logger.debug(me)
    assert me == user_email, "MUST have terra identity"


def test_get_projects():
    """Print list of projects."""
    namespace = 'anvil-datastorage'
    project_pattern = 'AnVIL_CCDG.*'
    projects = get_projects([namespace], project_pattern=project_pattern)
    assert len(projects) > 0, "Should have at least 1 project matching {}".format(project_pattern)
    for name in [w['workspace']['name'] for w in projects]:
        assert 'CCDG' in name, "Only CCDG workspaces should be included"

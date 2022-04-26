"""Test terra wrapper."""

from anvil.terra.api import get_projects, whoami
import logging

logging.getLogger('anvil.test_terra').setLevel(logging.DEBUG)
logger = logging.getLogger('anvil.test_terra')


def test_whoami():
    """Ensure terra."""
    me = whoami()
    logger.debug(me)
    assert me, "MUST have terra identity"


def test_get_projects():
    """Print list of projects."""
    namespace = 'anvil-datastorage'
    project_pattern = 'AnVIL_CCDG.*'
    projects = get_projects([namespace], project_pattern=project_pattern)
    assert len(projects) > 0, "Should have at least 1 project matching {}".format(project_pattern)
    for name in [w['workspace']['name'] for w in projects]:
        assert 'ccdg' in name.lower(), "Only CCDG workspaces should be included"

"""Wrap firecloud api."""

import firecloud.api as FAPI
import logging
import re

logger = logging.getLogger('terra')

USER_PROJECT = None


def whoami():
    """Wrap fapi's whoami.

    Returns:
        str: google id

    """
    return FAPI.whoami()


def get_projects(namespaces=None, project_pattern=None):
    """Filter terra workspaces by namespaces and project_pattern.

    Args:
        namespaces ([str]): Optional, list of workspace `namespace` to match ex: 'anvil-datastorage'.
        project_pattern (str): Optional, regexp to match workspace `name` ex: 'AnVIL_CCDG.*'.

    Returns:
        dict: keys ['accessLevel', 'public', 'workspace', 'workspaceSubmissionStats']

    """
    workspaces = FAPI.list_workspaces()
    workspaces = workspaces.json()

    if namespaces:
        workspaces = [w for w in workspaces if w['workspace']['namespace'] in namespaces]

    if project_pattern:
        workspaces = [w for w in workspaces if re.match(project_pattern, w['workspace']['name'])]
    return workspaces

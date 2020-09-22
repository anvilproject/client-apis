"""Wraps  firecloud api."""

from anvil.util.cache import memoize
import firecloud.api as FAPI
import logging
import re
from attrdict import AttrDict

logger = logging.getLogger('anvil.terra.api')

USER_PROJECT = None


def whoami():
    """Wrap fapi's whoami.

    Returns:
        str: google id

    """
    return FAPI.whoami()


@memoize
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
        workspaces = [AttrDict(w) for w in workspaces if w['workspace']['namespace'] in namespaces]

    if project_pattern:
        workspaces = [AttrDict(w) for w in workspaces if re.match(project_pattern, w['workspace']['name'], re.IGNORECASE)]

    # normalize fields
    for w in workspaces:
        if 'project_files' not in w.workspace:
            w.workspace.project_files = []
    return workspaces


@memoize
def get_entities(namespace='anvil-datastorage', workspace=None, entity_name=None):
    """Return all entities in a workspace."""
    entities = [AttrDict(e) for e in FAPI.get_entities(namespace, workspace, entity_name).json()]
    return entities


@memoize
def get_schema(namespace, workspace):
    """Fetch all entity types."""
    return FAPI.list_entity_types(namespace=namespace, workspace=workspace).json()

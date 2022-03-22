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
    logger.debug(f"get_entities {namespaces} {project_pattern}")

    workspaces = FAPI.list_workspaces()
    workspaces = workspaces.json()
    logger.debug(f'all workspaces len {len(workspaces)}')
    logger.debug(f"all namespaces {set(sorted([w['workspace']['namespace'] for w in workspaces]))}")

    if namespaces:
        workspaces = [AttrDict(w) for w in workspaces if w['workspace']['namespace'] in namespaces]

    logger.debug(f'after namespace >{namespaces}<', len(workspaces))
    logger.debug(f"all namespaces {set(sorted([w['workspace']['namespace'] for w in workspaces]))}")

    if project_pattern:
        workspaces = [AttrDict(w) for w in workspaces if re.match(project_pattern, w['workspace']['name'], re.IGNORECASE)]

    logger.debug(f'after project_pattern >{project_pattern}<', len(workspaces))
    logger.debug(f"all workspaces {set(sorted([w['workspace']['name'] for w in workspaces]))}")

    # normalize fields
    for w in workspaces:
        if 'project_files' not in w.workspace:
            w.workspace.project_files = []
    return workspaces


@memoize
def get_entities(namespace='anvil-datastorage', workspace=None, entity_name=None):
    """Return all entities in a workspace."""
    logger.debug(f"get_entities {namespace} {workspace} {entity_name}")
    try:
        entities = [AttrDict(e) for e in FAPI.get_entities(namespace, workspace, entity_name).json()]
        return entities
    except Exception as e:
        logger.error(f"{workspace} {entity_name} {e}")
        return []
    


@memoize
def get_schema(namespace, workspace):
    """Fetch all entity types."""
    logger.debug(f"get_schema {namespace} {workspace}")
    return FAPI.list_entity_types(namespace=namespace, workspace=workspace).json()

import gen3 as GEN3
import firecloud.api as FAPI

import re
from attrdict import AttrDict

def dump(fapi=FAPI):
    print(fapi.list_workspaces().json())


def get_programs(fapi=FAPI):
  """Maps terra namespaces to gen3.programs"""
  return list(
    set(
      [workspace['workspace']['namespace'] for workspace in fapi.list_workspaces().json()]
      )
  )


def get_namespaces(fapi=FAPI):
    """Synonym for get_programs."""
    return get_programs(fapi)


def get_projects(namespaces=None, project_pattern=None, fapi=FAPI):
  """Maps terra workspaces to gen3.projects"""
  workspaces = fapi.list_workspaces().json()
  if namespaces:
    workspaces = [
        AttrDict({'project': w['workspace']['name'], 'program': w['workspace']['namespace']}) for w in workspaces if w['workspace']['namespace'] in namespaces
    ]
  if project_pattern:
    workspaces = [w for w in workspaces if re.match(project_pattern, w.project) ]

  return workspaces


def get_workspaces(namespaces=None, fapi=FAPI):
    """Synonym for get_programs."""
    return get_projects(namespaces=namespaces, fapi=fapi)


def get_project_schema(project, fapi=FAPI):
  """Fetches all entity types"""
  project.schema = fapi.list_entity_types(namespace=project.program, workspace=project.project).json()
  return project


def get_project_schemas(namespaces=None, fapi=FAPI):
    projects = get_projects(namespaces, fapi)
    project_schemas = []
    for project in projects:
        project_schemas.append({
            'project': project,
            'schema': get_project_schema(namespaces[0], project, fapi)
        })
    return project_schemas

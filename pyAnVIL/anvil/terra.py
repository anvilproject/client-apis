import re
import logging
from attrdict import AttrDict
import firecloud.api as FAPI
from google.cloud import storage
import sqlite3
import json

USER_PROJECT = None
BLOB_CACHE = sqlite3.connect('blob_cache.sqlite')
logger = logging.getLogger('terra')


def CREATE_GOOGLE_STORAGE_CACHE_TABLE():
    cur = BLOB_CACHE.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS blobs (
        bucketName text PRIMARY KEY,
        json text NOT NULL
    );""")
    BLOB_CACHE.commit()


# static
CREATE_GOOGLE_STORAGE_CACHE_TABLE()


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


def blob_cache_get(bucketName):
    cur = BLOB_CACHE.cursor()
    cur.execute(f"SELECT json FROM blobs where bucketName='{bucketName}'")
    rows = cur.fetchall()
    logger.debug(f'cache get {bucketName}, {len(rows)} rows')
    if len(rows) == 0:
        return None
    return json.loads(rows[0][0])


def blob_cache_put(bucketName, blobs):
    cur = BLOB_CACHE.cursor()
    logger.debug(f'cache put {bucketName}, {len(blobs)} blobs')
    cur.execute(f"insert into blobs values ('{bucketName}', '{json.dumps(blobs)}') ON CONFLICT(bucketName) DO UPDATE SET json=excluded.json;")
    BLOB_CACHE.commit()


def get_blobs(workspace, user_project):
    """Retrieves all blobs in terra bucket associtated with workspace."""
    # in cache?
    blobs = blob_cache_get(workspace['bucketName'])
    if blobs:
        return blobs
    # Instantiates a google client
    # get all blobs in bucket
    # print(f"fetching blobs from google {workspace['bucketName']}")
    storage_client = storage.Client(project=user_project)
    bucket = storage_client.bucket(workspace['bucketName'], user_project)
    # return only name and size
    blobs = {}
    for b in list(bucket.list_blobs()):
        blobs[f"gs://{workspace['bucketName']}/{b.name}"] = {'size': b.size}
    blob_cache_put(workspace['bucketName'], blobs)
    return blobs


def get_projects(namespaces=None, project_pattern=None, fapi=FAPI, user_project=USER_PROJECT):
    """Maps terra workspaces to gen3.projects"""
    logger.debug(f"get_projects {project_pattern} ...")
    workspaces = fapi.list_workspaces().json()
    if namespaces:
        workspaces = [
            AttrDict({'project_id': f"{w['workspace']['namespace']}/{w['workspace']['name']}",
                      'project': w['workspace']['name'],
                      'program': w['workspace']['namespace'],
                      'bucketName': w['workspace']['bucketName']}) for w in workspaces if w['workspace']['namespace'] in namespaces
        ]
    if project_pattern:
        workspaces = [w for w in workspaces if re.match(project_pattern, w.project)]
    for w in workspaces:
        w.blobs = get_blobs(w, user_project=user_project)
    logger.debug(f"get_projects {project_pattern} DONE")
    return workspaces


def get_workspaces(namespaces=None, fapi=FAPI, user_project=USER_PROJECT):
    """Synonym for get_programs."""
    return get_projects(namespaces=namespaces, fapi=fapi, user_project=user_project)


def get_project_schema(project, fapi=FAPI):
    """Fetches all entity types"""
    project.schema = fapi.list_entity_types(namespace=project.program, workspace=project.project).json()
    return project


def get_project_schemas(namespaces=None, fapi=FAPI):
    """Returns schema for all namespaces."""
    projects = get_projects(namespaces, fapi)
    project_schemas = []
    for project in projects:
        project_schemas.append({
            'project': project,
            'schema': get_project_schema(namespaces[0], project, fapi)
        })
    return project_schemas


def get_entities(namespace='anvil-datastorage', workspace=None, entity_name=None, fapi=FAPI):
    """Returns all entities in a workspace."""
    entities = [AttrDict(e) for e in fapi.get_entities(namespace, workspace, entity_name).json()]
    return entities

#!/usr/bin/env python3

"""Read meta data from terra workspaces."""

import logging
import re
from attrdict import AttrDict
import firecloud.api as FAPI
import json
import click
import os
from urllib.parse import urlparse
from collections import defaultdict

from anvil.etl.utilities.entities import Entities

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s %(levelname)-8s %(message)s')
logger = logging.getLogger('anvil.etl_old.extractors.terra')


# where all workspaces are kept w/in terra
DEFAULT_NAMESPACE = 'anvil-datastorage'

# workspace patterns
DEFAULT_CONSORTIUMS = (
    ('CMG', 'AnVIL_CMG_.*'),
    ('CCDG', 'AnVIL_CCDG_.*'),
    ('GTEx', '^AnVIL_GTEx_V8_hg38$'),
    ('Public', '^1000G-high-coverage-2019$'),
    ('NHGRI', '^AnVIL_NHGRI'),
    ('NIMH', '^AnVIL_NIMH'),
)


def recursive_default_dict():
    """Recursive default dict, any key defaults to a dict."""
    return defaultdict(recursive_default_dict)


def get_workspaces(namespaces=None, name_pattern=None):
    """Filter terra workspaces by namespaces and project_pattern.

    Args:
        namespaces ([str]): Optional, list of workspace `namespace` to match ex: 'anvil-datastorage'.
        name_pattern (str): Optional, regexp to match workspace `name` ex: 'AnVIL_CCDG.*'.

    Returns:
        dict: keys ['accessLevel', 'public', 'workspace', 'workspaceSubmissionStats']

    """
    logger.debug(f"get_entities {namespaces} {name_pattern}")

    workspaces = FAPI.list_workspaces()
    workspaces = workspaces.json()

    if namespaces:
        workspaces = [AttrDict(w) for w in workspaces if w['workspace']['namespace'] in namespaces]

    if name_pattern:
        workspaces = [AttrDict(w) for w in workspaces if re.match(name_pattern, w['workspace']['name'], re.IGNORECASE)]

    # normalize fields
    for w in workspaces:
        if 'project_files' not in w.workspace:
            w.workspace.project_files = []
    return workspaces


def get_entities(namespace='anvil-datastorage', workspace=None, entity_name=None):
    """Return all entities in a workspace."""
    logger.debug(f"get_entities {namespace} {workspace} {entity_name}")
    try:
        entities = [AttrDict(e) for e in FAPI.get_entities(namespace, workspace, entity_name).json()]
        return entities
    except Exception as e:
        logger.error(f"{workspace} {entity_name} {e}")
        return []


def get_schema(namespace, workspace):
    """Fetch all entity types."""
    logger.debug(f"get_schema {namespace} {workspace}")
    schema = FAPI.list_entity_types(namespace=namespace, workspace=workspace).json()
    if 'statusCode' in schema['schema']:
        raise Exception(schema)
    return schema


def extract_bucket_fields(output_path=None, entities=None, workspace_name=None):
    """Query db, sample first row in all entities, determine fields that have references to bucket object."""
    if not entities:
        assert output_path
        entities = Entities(path=f"{output_path}/terra_entities.sqlite")
    for workspace in entities.get_by_label('workspace'):
        workspace = AttrDict(workspace)
        if workspace_name and workspace.workspace.name != workspace_name:
            continue
        schema = AttrDict(entities.get_edges_by_label(src=workspace.workspace.name, src_name='workspace', dst_name='schema'))
        for entity_name in schema.schema.keys():
            if entity_name == 'schema':
                continue
            child = entities.get_edges_by_label(src=workspace.workspace.name, src_name='workspace', dst_name=entity_name, limit=1)
            if entity_name not in child:
                logger.error(('invalid.child', workspace.consortium_name, workspace.workspace.name, entity_name, child))
                continue
            bucket_keys = [k for k, v in child[entity_name]['attributes'].items() if isinstance(v, str) and v.startswith('gs://')]
            if len(bucket_keys) == 0:
                continue
            buckets = [child[entity_name]['attributes'][key] for key in bucket_keys]
            buckets = list(set([urlparse(b).netloc for b in buckets]))
            # TODO - move this exception to config or other ?
            if entity_name == 'sequencing' and 'AnVIL_CMG_Broad' in workspace.workspace.name:
                bucket_keys = 'crai_or_bai_path,crai_path,cram_or_bam_path,cram_path,md5_path'.split(',')
            yield {
                'consortium_name': workspace.consortium_name,
                'workspace_name': workspace.workspace.name,
                'entity_name': entity_name,
                'bucket_fields': bucket_keys,
                'buckets': buckets
            }


def make_entity_key(workspace_name, entity):
    """Edges and vertices are stored with this key structure."""
    return f"{entity['entityType']}/{workspace_name}/{entity['name']}"


@click.group()
@click.pass_context
def cli(ctx):
    """Read meta data from terra workspaces."""
    # ensure that ctx.obj exists and is a dict
    # in case we want to eventually chain these commands together into a pipeline
    assert ctx.obj['output_path'], "Missing DEFAULT_OUTPUT_PATH"


@cli.command('clean')
@click.pass_context
def clean(ctx):
    """Remove database."""
    path = f"{ctx.obj['output_path']}/terra_entities.sqlite"
    try:
        os.remove(path)
        logger.info(('removed', path))
    except OSError as e:
        logger.warning((e, path))


@cli.command('extract')
@click.option('--namespace', default=DEFAULT_NAMESPACE, help=f'Terra namespace default={DEFAULT_NAMESPACE}')
@click.pass_context
def extract_workspaces(ctx, namespace):
    """Read workspaces from terra, write to database. Do this first! May take several minutes."""
    output_path = ctx.obj['output_path']
    consortiums = ctx.obj['config']['consortiums']    
    logger.info(f"Extracting metadata for {len(consortiums)} consortiums, this may take several minutes.")
    entities = Entities(path=f"{output_path}/terra_entities.sqlite")
    for consortium_name, config in consortiums.items():
        projects = get_workspaces(namespace, name_pattern=config['workspaces'])
        for workspace in projects:
            logger.info((consortium_name, workspace.workspace.name))
            workspace.consortium_name = consortium_name
            schema = FAPI.list_entity_types(namespace=namespace, workspace=workspace.workspace.name).json()
            if 'statusCode' in schema:
                logger.error(('no.schema', consortium_name, workspace.workspace.name, schema))
                continue
            entities.put(key=workspace.workspace.name, label='workspace', data=workspace)
            entities.put(key=f"schema/{workspace.workspace.name}", label='schema', data=schema)
            entities.put_edge(
                src=workspace.workspace.name, src_name='workspace',
                dst=f"schema/{workspace.workspace.name}", dst_name='schema',
            )
            for entity_name in schema.keys():
                problem_occurred = False
                for entity in FAPI.get_entities(namespace, workspace.workspace.name, entity_name).json():
                    if problem_occurred:
                        break
                    if not isinstance(entity, dict):
                        # write a problem record
                        entities.put(key=f"problem/{workspace.workspace.name}/{entity_name}", label='problem', data={'error': f'no.entity.returned {entity_name}'})
                        entities.put_edge(
                            src=workspace.workspace.name, src_name='workspace',
                            dst=f"problem/{workspace.workspace.name}/{entity_name}", dst_name='problem',
                        )
                        problem_occurred = True
                        continue
                    # entity['name'] aka 'id'
                    id = make_entity_key(workspace.workspace.name, entity)
                    entities.put(key=id, label=entity_name, data=entity)
                    entities.put_edge(
                        src=workspace.workspace.name, src_name='workspace',
                        dst=id, dst_name=entity_name
                    )
    entities.commit(True)
    entities.index()


@cli.command('cat')
@click.option('--workspace', default=None, help='<Regexp> e.g "AnVIL_CCDG.*"')
@click.pass_context
def cat(ctx, workspace):
    """Read workspaces and their children from database, write to stdout."""
    output_path = ctx.obj['output_path']
    name_pattern = workspace
    entities = Entities(path=f"{output_path}/terra_entities.sqlite")
    for workspace in entities.get_by_label('workspace'):
        if name_pattern:
            if not re.match(name_pattern, workspace['workspace']['name'], re.IGNORECASE):
                continue
        workspace = AttrDict(workspace)
        children = entities.get_edges(src=workspace.workspace.name, src_name='workspace')
        workspace.children = children
        print(json.dumps(workspace))


@cli.command('bucket_fields')
@click.option('--workspace', default=None, help='filter, only this workspace')
@click.pass_context
def bucket_fields(ctx, workspace):
    """Read workspaces from database, determine children with bucket references, write to stdout."""
    output_path = ctx.obj['output_path']
    print(json.dumps([bucket_fields for bucket_fields in extract_bucket_fields(output_path, workspace_name=workspace)]))


# @cli.command('bucket_graph')
# @click.option('--details', default=False, is_flag=True, help="Show the details.")
# @click.pass_context
# def bucket_graph(ctx, details):
#     """Read bucket fields from database, analyze patterns, write to stdout."""
#     output_path = ctx.obj['output_path']

#     G = recursive_default_dict()
#     for bf in extract_bucket_fields(output_path):
#         # {
#         #     'consortium_name': workspace.consortium_name,
#         #     'workspace_name': workspace.workspace.name,
#         #     'entity_name': entity_name,
#         #     'bucket_fields': bucket_keys,
#         #     'buckets': buckets
#         # }
#         bf = AttrDict(bf)
#         G[bf.consortium_name][bf.entity_name][','.join(sorted(bf.bucket_fields))]['workspaces'][bf.workspace_name] = bf.buckets

#     analysis = recursive_default_dict()
#     for consortium_name in G:
#         for entity_name in G[consortium_name]:
#             version_count = len(G[consortium_name][entity_name].keys())
#             analysis[consortium_name][entity_name]['version_count'] = version_count
#             if details:
#                 analysis[consortium_name][entity_name]['details'] = G[consortium_name][entity_name]
#     print(json.dumps(analysis))


@cli.command('schema')
@click.option('--workspace', default=None, help='filter, only this workspace')
@click.pass_context
def schema(ctx, workspace):
    """Read schema fields from database, analyze patterns, write to stdout."""
    output_path = ctx.obj['output_path']
    workspace_name = workspace
    entities = Entities(path=f"{output_path}/terra_entities.sqlite")

    G = recursive_default_dict()
    for workspace in entities.get_by_label('workspace'):
        workspace = AttrDict(workspace)
        if workspace_name and workspace.workspace.name != workspace_name:
            continue
        schema = entities.get_edges_by_label(src=workspace.workspace.name, src_name='workspace', dst_name='schema')
        for entity_name in schema['schema']:
            assert 'attributeNames' in schema['schema'][entity_name], (entity_name, schema)
            attributeNames = ','.join(sorted(schema['schema'][entity_name]['attributeNames']))
            G[workspace.consortium_name][entity_name][attributeNames][workspace.workspace.name] = {}

    analysis = recursive_default_dict()
    for consortium_name in G:
        for entity_name in G[consortium_name]:
            version_count = len(G[consortium_name][entity_name].keys())
            analysis[consortium_name][entity_name]['version_count'] = version_count
            analysis[consortium_name][entity_name]['details'] = G[consortium_name][entity_name]
    print(json.dumps(analysis))

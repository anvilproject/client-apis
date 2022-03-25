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
import functools
import operator
import uuid

from numpy import isin

from anvil.etl.utilities.entities import Entities

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s %(levelname)-8s %(message)s')
logger = logging.getLogger('anvil.extractors.terra')


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

DEFAULT_OUTPUT_PATH = os.environ.get('OUTPUT_PATH','./DATA')


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


def extract_workspaces(namespaces=None, project_pattern=None):
    logger.debug(f"get_entities {namespaces} {project_pattern}")
    workspaces = get_workspaces(namespaces, project_pattern)
    for workspace in workspaces:
        dir(workspace)
        break


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
            bucket_keys = [k for k,v in child[entity_name]['attributes'].items() if isinstance(v, str) and v.startswith('gs://')]
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
    ctx.ensure_object(dict)


@cli.command('clean')
@click.option('--output_path', default=DEFAULT_OUTPUT_PATH, help=f'output path default={DEFAULT_OUTPUT_PATH}')
def clean(output_path):
    """Remove database."""
    path = f"{output_path}/terra_entities.sqlite"
    try:
        os.remove(path)
        logger.info(('removed', path))
    except OSError as e:
        logger.warning((e, path))


@cli.command('extract')
@click.option('--namespace', default=DEFAULT_NAMESPACE, help=f'Terra namespace default={DEFAULT_NAMESPACE}')
@click.option('--consortiums', type=(str, str), default=DEFAULT_CONSORTIUMS, multiple=True, help=f'<Name Regexp> e.g "CCDG AnVIL_CCDG.*" default {DEFAULT_CONSORTIUMS}')
@click.option('--output_path', default=DEFAULT_OUTPUT_PATH, help=f'output path default={DEFAULT_OUTPUT_PATH}')
def extract_workspaces(namespace, consortiums, output_path):    
    """Read workspaces from terra, write to database. Do this first! May take several minutes."""
    logger.info(f"Extracting metadata for {len(consortiums)} consortiums, this may take several minutes.")
    entities = Entities(path=f"{output_path}/terra_entities.sqlite")
    for consortium_name, project_pattern in consortiums:
        projects = get_workspaces(namespace, name_pattern=project_pattern)
        for workspace in projects:
            logger.info((consortium_name, workspace.workspace.name))
            workspace.consortium_name = consortium_name
            schema = FAPI.list_entity_types(namespace=namespace, workspace=workspace.workspace.name).json()
            if 'statusCode' in schema:
                logger.error(('no.schema', consortium_name, workspace.workspace.name,  schema))
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
                    id = make_entity_key(entity_name, workspace.workspace.name, entity['name'])
                    entities.put(key=id, label=entity_name, data=entity)
                    entities.put_edge(
                        src=workspace.workspace.name, src_name='workspace',
                        dst=id, dst_name=entity_name
                    )
    entities.commit(True)
    entities.index()


@cli.command('cat')
@click.option('--label', default='workspace', help=f'Terra workspace name')
@click.option('--output_path', default=DEFAULT_OUTPUT_PATH, help=f'output path default={DEFAULT_OUTPUT_PATH}')
@click.option('--name_pattern', default=None, help=f'<Regexp> e.g "AnVIL_CCDG.*" default')
def get_by_label(label, output_path, name_pattern):    
    """Read workspaces and their children from database, write to stdout."""
    entities = Entities(path=f"{output_path}/terra_entities.sqlite")
    for workspace in entities.get_by_label(label):
        if name_pattern:
            if not re.match(name_pattern, workspace['workspace']['name'], re.IGNORECASE):
                continue
        workspace = AttrDict(workspace)
        children = entities.get_edges(src=workspace.workspace.name, src_name='workspace')
        workspace.children = children
        print(json.dumps(workspace))


@cli.command('bucket_fields')
@click.option('--output_path', default=DEFAULT_OUTPUT_PATH, help=f'output path default={DEFAULT_OUTPUT_PATH}')
@click.option('--workspace_name', default=None, help=f'filter, only this workspace')
def bucket_fields(output_path, workspace_name):    
    """Read workspaces from database, determine children with bucket references, write to stdout."""
    for bucket_fields in extract_bucket_fields(output_path, workspace_name=workspace_name):
        print(json.dumps(bucket_fields))

@cli.command('bucket_graph')
@click.option('--output_path', default=DEFAULT_OUTPUT_PATH, help=f'output path default={DEFAULT_OUTPUT_PATH}')
@click.option('--details', default=False, is_flag=True, help="Show the details.")
def bucket_graph(output_path, details):    
    """Read bucket fields from database, analyze patterns, write to stdout."""
        
    G = recursive_default_dict()
    for bf in extract_bucket_fields(output_path):
        # {
        #     'consortium_name': workspace.consortium_name,
        #     'workspace_name': workspace.workspace.name,
        #     'entity_name': entity_name,
        #     'bucket_fields': bucket_keys,
        #     'buckets': buckets
        # }
        bf = AttrDict(bf)
        G[bf.consortium_name][bf.entity_name][','.join(sorted(bf.bucket_fields))]['workspaces'][bf.workspace_name] = bf.buckets

    analysis = recursive_default_dict()
    for consortium_name in G:
        for entity_name in G[consortium_name]:
            version_count = len(G[consortium_name][entity_name].keys())
            analysis[consortium_name][entity_name]['version_count'] = version_count
            if details:
                analysis[consortium_name][entity_name]['details'] = G[consortium_name][entity_name]
    print(json.dumps(analysis))


@cli.command('schema_graph')
@click.option('--output_path', default=DEFAULT_OUTPUT_PATH, help=f'output path default={DEFAULT_OUTPUT_PATH}')
@click.option('--details', default=False, is_flag=True, help="Show the details.")
def schema_graph(output_path, details):    
    """Read schema fields from database, analyze patterns, write to stdout."""
        
    entities = Entities(path=f"{output_path}/terra_entities.sqlite")
    
    G = recursive_default_dict()
    for workspace in entities.get_by_label('workspace'):
        workspace = AttrDict(workspace)
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
            if details:
                analysis[consortium_name][entity_name]['details'] = G[consortium_name][entity_name]
    print(json.dumps(analysis))

def normalize_workspace(consortium_name, workspace, config, entities, output_path):
    """Stich the graph together."""

    #
    # Uses command pattern to wrangle some very messy data
    #

    # set up a command library

    def consortium_config(context, consortium_name, workspace, config):
        context.consortium_config = config[consortium_name]
        inverted_config = recursive_default_dict()
        for fhir_entity in context.consortium_config:
            for alias in context.consortium_config[fhir_entity]['aliases']:
                inverted_config[alias] = fhir_entity
        context.inverted_config = inverted_config

    def patient_entity_names(context, consortium_name, workspace, config):
        context.patient_entity_names = context.consortium_config['Patient']['aliases']

    def patient_entity_names_CMG(context, consortium_name, workspace, config):
        patient_entity_names(context, consortium_name, workspace, config)

    def patient_entity_names_CCDG(context, consortium_name, workspace, config):
        patient_entity_names(context, consortium_name, workspace, config)

    def specimen_entity_name(context, consortium_name, workspace, config):
        context.specimen_entity_name = context.consortium_config['Specimen']['aliases'][0]

    def patient_from_specimen_numbered(context, consortium_name, workspace, config):
        """Look for workspace.specimen.01-subject_id -> workspace.subject."""

        return context.specimen['attributes']['01-subject_id']

    def patient_from_specimen_subject_id(context, consortium_name, workspace, config):
        """Look for workspace.specimen.subject_id -> workspace.subject."""

        return context.specimen['attributes']['subject_id']

    def patient_from_specimen_participant_id(context, consortium_name, workspace, config):
        """Look for context.specimen.participant_id -> workspace.subject."""

        return context.specimen['attributes']['participant_id']        

    def patient_from_specimen_subject(context, consortium_name, workspace, config):
        """Look for context.specimen.subject -> workspace.subject."""

        return context.specimen['attributes']['subject']

    def patient_from_participant(context, consortium_name, workspace, config):
        """Look for context.specimen.subject -> workspace.subject."""

        return context.specimen['attributes']['participant']

    def patient_from_specimen(context, consortium_name, workspace, config):
        """Introspect specimen, see if it has a dict pointing to subject."""

        patient_entity_name = None
        for n in context.patient_entity_names:
            if n in context.specimen['attributes']:
                patient_entity_name = n
                break

        if not patient_entity_name:
            if 'no.find.patient_entity_name' not in context['logged_already']:
                logger.error(('no.find.patient_entity_name', consortium_name, workspace.workspace.name, context.patient_entity_names, workspace.children.keys()))
                context['logged_already'].append('no.find.patient_entity_name')
            return None

        if patient_entity_name not in context.specimen['attributes']:
            if 'specimen.missing.patient' not in context['logged_already']:
                logger.error(('specimen.missing.patient', consortium_name, workspace.workspace.name, context.specimen))
                context['logged_already'].append('specimen.missing.patient')
            return None

        patient_key_from_specimen = context.specimen['attributes'][patient_entity_name]
        if isinstance(patient_key_from_specimen, dict ):
            patient_key_from_specimen = patient_key_from_specimen['entityName']
        return patient_key_from_specimen

    def patient_from_participant_pfb_subject(context, consortium_name, workspace, config):
        """Introspect specimen, see if it has a dict pointing to subject."""

        patient_entity_name = None
        for n in context.patient_entity_names:
            if n in context.specimen['attributes']:
                patient_entity_name = n
                break

        if not patient_entity_name:
            if 'no.find.patient_entity_name' not in context['logged_already']:
                logger.error(('no.find.patient_entity_name', consortium_name, workspace.workspace.name))
                context['logged_already'].append('no.find.patient_entity_name')
            return None

        if patient_entity_name not in context.specimen['attributes']:
            if 'specimen.missing.patient' not in context['logged_already']:
                logger.error(('specimen.missing.patient', consortium_name, workspace.workspace.name, context.specimen))
                context['logged_already'].append('specimen.missing.patient')
            return None

        patient_key_from_specimen = context.specimen['attributes'][patient_entity_name]
        if isinstance(patient_key_from_specimen, dict ):
            patient_key_from_specimen = f"pfb:{patient_key_from_specimen['entityName']}"
        return patient_key_from_specimen

    def patient_from_specimen_all(context, consortium_name, workspace, config):
        """Try all commands."""
        cmd = None
        if 'subject' in context.specimen['attributes']:
            cmd = patient_from_specimen_subject
        if 'participant_id' in context.specimen['attributes']:
            cmd = patient_from_specimen_participant_id
        if '01-subject_id' in context.specimen['attributes']:
            cmd = patient_from_specimen_numbered
        if 'subject_id' in context.specimen['attributes']:
            cmd = patient_from_specimen_subject_id
        if 'specimen' in context.specimen['attributes']:
            cmd = patient_from_specimen
        if 'participant' in context.specimen['attributes']:
            cmd = patient_from_participant
        if 'pfb:subject' in context.specimen['attributes']:
            cmd = patient_from_participant_pfb_subject

        if cmd:
            return cmd(context, consortium_name, workspace, config)
        else:
            if 'unparsable.specimen' not in context['logged_already']:
                logger.error(('unparsable.specimen', consortium_name, workspace.workspace.name, context.specimen))
                context['logged_already'].append('unparsable.specimen')
            return None

    def task_entity_names(context, consortium_name, workspace, config):
        """Set task_entity_names on context to aliases, or _implied if no Task configured."""
        if 'Task' in context.consortium_config:
            context.task_entity_names = context.consortium_config['Task']['aliases']
        else:
            context.task_entity_names = ['_implied']

    def ensure_tasks(context, consortium_name, workspace, config):
        """Set tasks on workspace to a workspace child, or create an artificial one if _implied."""
        workspace['tasks'] = {}
        if '_implied' in context.task_entity_names:
            workspace['tasks']['_implied'] = recursive_default_dict()
        else:
            for task_entity_name in context.task_entity_names:
                if task_entity_name in workspace.children.keys():
                    workspace['tasks'][task_entity_name] = workspace.children[task_entity_name]
            if not workspace['tasks']:
                workspace['tasks']['_implied'] = recursive_default_dict()

    def ensure_bucket_fields(context, consortium_name, workspace, config):
        """Introspect workspace schema looking for fields that have bucket objects."""
        context['bucket_fields'] = [bf for bf in extract_bucket_fields(entities=entities, workspace_name=workspace.workspace.name)]

    def ensure_bucket_fieldsAnVIL_CMG_BaylorHopkins_HMB_NPU_WES(context, consortium_name, workspace, config):
        """Override sample.bucket_fields"""
        ensure_bucket_fields(context, consortium_name, workspace, config)
        assert len(context['bucket_fields']) == 1
        context['bucket_fields'][0]['bucket_fields'] = "bam,bam_md5,crai,cram,cram_md5".split(',')        

    def ensure_bucket_fieldsAnVIL_CMG_Broad_crai_or_bai(context, consortium_name, workspace, config):
        """Override sample.bucket_fields"""
        ensure_bucket_fields(context, consortium_name, workspace, config)
        assert len(context['bucket_fields']) == 1
        context['bucket_fields'][0]['bucket_fields'] = 'crai_or_bai_path,crai_path,cram_or_bam_path,cram_path,md5_path'.split(',')        

    def ensure_tasks_populated(context, consortium_name, workspace, config):
        """Create a terra style entity for an implied task if none exists."""
        if '_implied' in workspace['tasks']:
            assert workspace.specimens
            workspace['tasks']['_implied'] = []
            for specimen in workspace.specimens:
                # create terra type entity
                workspace['tasks']['_implied'].append(AttrDict({
                    'entityType': 'Task',
                    'name': specimen['name'],
                    'attributes': {
                        # TODO - are there any derived attributes?
                    }                    
                }))
        # check that other keys exist
        assert len(workspace['tasks']) > 0, ('missing.tasks', consortium_name, workspace.workspace.name)

    def extract_specimen_reference(context, consortium_name, workspace, config):
        """Extract a specimen key from arbitrary entity."""
        assert 'entity_with_specimen_reference' in context
        entity = context['entity_with_specimen_reference']
        if 'sample_alias' in entity['attributes']:
            context['specimen_reference'] = {
                'name': entity['attributes']['sample_alias'],
                'entityType': 'sample',
                'fhir_entity': context.inverted_config['sample']
            }

    def ensure_tasks_linked_to_documents(context, consortium_name, workspace, config):
        """Ensure tasks have input that is a specimen, and outputs that are document references."""

        # set up a hash of children with buckets for quick lookup
        _children = recursive_default_dict()
        for bf in context.bucket_fields:
            if bf['entity_name'] in  workspace.children:
                for child in workspace.children[bf['entity_name']]:
                    _children[bf['entity_name']][child['name']] = child
        # create a task, whose inputs are entities with bucket fields.
        # and whose outputs are those bucket fields
        deletion_keys = []
        for task_entity in workspace['tasks']:
            document_count = 0
            for task in workspace['tasks'][task_entity]:
                _task_children = recursive_default_dict()
                _task_inputs = []
                for bf in context.bucket_fields:
                    if bf['entity_name'] in  _children:
                        if task['name'] in _children[bf['entity_name']]:
                            child = _children[bf['entity_name']][task['name']]
                            _task_inputs.append(
                                {
                                    'entityType': child['entityType'],
                                    'name': child['name'],
                                    'fhir_entity': context.inverted_config[child['entityType']]
                                }
                            )
                            for field in bf['bucket_fields']:
                                if workspace.workspace.name not in ['AnVIL_CMG_BaylorHopkins_HMB-NPU_WES']:
                                    # TODO - remove if not needed
                                    if 'AnVIL_CMG_Broad' not in workspace.workspace.name:
                                        assert field in child['attributes'], (field, child, bf)
                                if field in child['attributes']:
                                    _task_children[bf['entity_name']][field] = child['attributes'][field]
                                    document_count += 1                           

                if 'Specimen' not in [ti['fhir_entity'] for ti in _task_inputs]:
                    no_specimen = True
                    for ti in _task_inputs:
                        context.entity_with_specimen_reference = _children[ti['entityType']][ti['name']]
                        exec_command(context, consortium_name, workspace, config, 'extract_specimen_reference')
                        if 'specimen_reference' in context:
                            _task_inputs.append(context['specimen_reference'])
                            no_specimen = False
                            del context['specimen_reference']
                    if no_specimen and len(_task_inputs) > 0:
                        logger.error(('no.specimen.in.task', consortium_name, workspace.workspace.name, _task_inputs))
                
                # It is possible for an entity that has  bucket fields to be classified as a task,
                # make sure we don't self reference
                _task_inputs = [ti for ti in _task_inputs if not (task['name'] == ti['name'] and  task['entityType']  == ti['entityType'] )]

                task['outputs'] = _task_children
                task['inputs'] = _task_inputs
                if document_count == 0:
                    deletion_keys.append(task_entity)

        # delete task entities without documents
        for key in set(deletion_keys):
            del workspace['tasks'][key]
            logger.warning(('no.task', consortium_name, workspace.workspace.name, key))

    def blob_attributes(context, consortium_name, workspace, config):
        """Decorate task outputs with blob attributes."""
        google_entities = Entities(path=f"{output_path}/google_entities.sqlite")

        for task_entity in workspace['tasks']:
            for task in workspace['tasks'][task_entity]:
                for output_source, output in task['outputs'].items():
                    for output_property, url in output.items():
                        blob = google_entities.get(url)
                        if not blob:
                            blob = {'url':url}
                            # if 'blob.not.in.bucket' not in context['logged_already']:                            
                            logger.error(('blob.not.in.bucket', consortium_name, workspace.workspace.name, output_source, output_property, url))                            
                            context['logged_already'].append('blob.not.in.bucket')
                        task['outputs'][output_source][output_property] = blob
                        # print('OK', consortium_name, workspace.workspace.name, output_source, output_property, url)

    def practitioner(context, consortium_name, workspace, config):
        """Determine the PI, set practitioner on workspace."""
        assert 'study_pi' in workspace.workspace.attributes, workspace.workspace.attributes
        workspace.practitioner = workspace.workspace.attributes['study_pi']

    #
    # command specializations hierarchy
    #
    C = {}
    C['//consortium_config'] = lambda context, consortium_name, workspace, config: consortium_config(context, consortium_name, workspace, config)
    C['CMG//patient_entity_names'] = lambda context, consortium_name, workspace, config: patient_entity_names_CMG(context, consortium_name, workspace, config)
    C['CCDG//patient_entity_names'] = lambda context, consortium_name, workspace, config: patient_entity_names_CCDG(context, consortium_name, workspace, config)
    C['//patient_entity_names'] = lambda context, consortium_name, workspace, config: patient_entity_names(context, consortium_name, workspace, config)
    C['//specimen_entity_name'] = lambda context, consortium_name, workspace, config: specimen_entity_name(context, consortium_name, workspace, config)
    C['//patient_from_specimen'] = lambda context, consortium_name, workspace, config: patient_from_specimen_all(context, consortium_name, workspace, config)
    C['//task_entity_names'] = lambda context, consortium_name, workspace, config: task_entity_names(context, consortium_name, workspace, config)
    C['//ensure_tasks'] = lambda context, consortium_name, workspace, config: ensure_tasks(context, consortium_name, workspace, config)
    C['//ensure_tasks_populated'] = lambda context, consortium_name, workspace, config: ensure_tasks_populated(context, consortium_name, workspace, config)
    C['//ensure_tasks_linked_to_documents'] = lambda context, consortium_name, workspace, config: ensure_tasks_linked_to_documents(context, consortium_name, workspace, config)
    C['//ensure_bucket_fields'] = lambda context, consortium_name, workspace, config: ensure_bucket_fields(context, consortium_name, workspace, config)
    C['CMG/AnVIL_CMG_BaylorHopkins_HMB-NPU_WES/ensure_bucket_fields'] = lambda context, consortium_name, workspace, config: ensure_bucket_fieldsAnVIL_CMG_BaylorHopkins_HMB_NPU_WES(context, consortium_name, workspace, config)
    C['CMG/AnVIL_CMG_Broad_Kidney_Hildebrandt_WES/ensure_bucket_fields'] = lambda context, consortium_name, workspace, config: ensure_bucket_fieldsAnVIL_CMG_Broad_crai_or_bai(context, consortium_name, workspace, config)
    C['CMG/AnVIL_CMG_Broad_Muscle_KNC_WES/ensure_bucket_fields'] = lambda context, consortium_name, workspace, config: ensure_bucket_fieldsAnVIL_CMG_Broad_crai_or_bai(context, consortium_name, workspace, config)
    C['//blob_attributes'] = lambda context, consortium_name, workspace, config: blob_attributes(context, consortium_name, workspace, config)
    C['CMG/ANVIL_CMG_Broad_Muscle_Laing_WES/extract_specimen_reference'] = lambda context, consortium_name, workspace, config: extract_specimen_reference(context, consortium_name, workspace, config)
    C['//practitioner'] = lambda context, consortium_name, workspace, config: practitioner(context, consortium_name, workspace, config)
    
    #
    # runner
    #
    def exec_command(context, consortium_name, workspace, config, cmd):
        """Find and run version of the command that matches."""
        workspace_name = workspace['workspace']['name']
        
        paths = [
            f"{consortium_name}/{workspace_name}/{cmd}",
            f"{consortium_name}//{cmd}",
            f"//{workspace_name}/{cmd}",
            f"//{cmd}"
        ]

        for path in paths:
            try:
                runner = C.get(path, None)
                if runner:
                    return runner(context, consortium_name, workspace, config)
            except Exception as e:
                raise e

        assert False, f"Misconfiguration, we shouldn't get here. {path}"

    #
    # 'main'
    #
    if list(workspace.children.keys()) == ['schema']:
        logger.error(('no.child.entities', consortium_name, workspace.workspace.name ))
        return None

    # pass context to commands
    context = AttrDict({'logged_already': []})

    # find config
    exec_command(context, consortium_name, workspace, config, 'consortium_config')
    assert context.consortium_config, "Should set context config"
    assert context.inverted_config, "Should set context inverted_config"

    # who ran this study?
    exec_command(context, consortium_name, workspace, config, 'practitioner')
    assert workspace.practitioner, "Should set workspace.workspacepractitioner"

    # deduce entity names in workspace
    exec_command(context, consortium_name, workspace, config, 'patient_entity_names')
    assert context.patient_entity_names, "Should set context patient_entity_names"

    exec_command(context, consortium_name, workspace, config, 'specimen_entity_name')
    assert context.specimen_entity_name, "Should set context specimen_entity_name"
    assert isinstance(context.specimen_entity_name, str), context.specimen_entity_name
    # logger.info(('context.specimen_entity_name', consortium_name, workspace, context.specimen_entity_name))

    # start traversal from specimen
    specimens = workspace.children.get(context.specimen_entity_name, None)
    if not specimens or len(specimens) == 0:
        logger.error(('no.find.specimens', consortium_name, workspace.workspace.name))
        return
    workspace.specimens = specimens

    # traverse up to patient
    logged_already = False
    specimen_patient_ids = []
    for specimen in workspace.specimens:
        context.specimen = specimen
        patient_id = exec_command(context, consortium_name, workspace, config, 'patient_from_specimen')
        if not patient_id and not logged_already:
            logged_already = True
            logger.error(("no.find.patient.from.sample", consortium_name, workspace.workspace.name, context.specimen, workspace.children.keys()))
        else:    
            specimen_patient_ids.append((specimen.name, patient_id))
    # store sample->patient on workspace for now    
    total_patient_count = 0
    for patient_entity_name in context.patient_entity_names:
        # patient_entity_names are possibilities, they don't have to exist        
        total_patient_count += len(workspace.children.get(patient_entity_name, []))
    workspace.specimen_patient_ids = specimen_patient_ids
    workspace.total_patient_count = total_patient_count

    # find tasks
    exec_command(context, consortium_name, workspace, config, 'task_entity_names')
    assert context.task_entity_names, "Should set context.task_entity_names"

    exec_command(context, consortium_name, workspace, config, 'ensure_tasks')
    assert workspace.tasks, ("context.tasks.empty", context.keys(), context.task_entity_names, context.consortium_config, workspace.children.keys() )
    assert len(workspace.tasks) > 0, "context.tasks should be > 0"

    exec_command(context, consortium_name, workspace, config, 'ensure_tasks_populated')    
    assert workspace.tasks, ("context.tasks.empty", context.keys(), context.task_entity_names, workspace.children.keys() )
    assert len(workspace.tasks) > 0, "context.tasks should be > 0"

    exec_command(context, consortium_name, workspace, config, 'ensure_bucket_fields')    
    if not context['bucket_fields']:
        logger.warning( ("no.entity.contains.bucket.objects", consortium_name, workspace.workspace.name))

    exec_command(context, consortium_name, workspace, config, 'ensure_tasks_linked_to_documents')    
    if len(workspace.tasks) == 0:
        logger.warning( ("no.tasks", consortium_name, workspace.workspace.name))
    else:
        for task_source, task_list in workspace.tasks.items():
            for task in task_list:
                assert len(task['inputs']) > 0, ("tasks.missing.inputs", task_source, task)
                assert len(task['outputs']) > 0, ("tasks.missing.outputs", task_source, task)
    # logger.info(('task.count', consortium_name, workspace.workspace.name, workspace.tasks.keys(), len(task_list) ))

    exec_command(context, consortium_name, workspace, config, 'blob_attributes')    

    # setup a hash of patients
    patients = {}
    specimens = {}
    if 'patient_entity_names' not in context:
        logger.warning(('missing.context.patient_entity_names', consortium_name, workspace.workspace.name))
        return
    for patient_entity_name in context.patient_entity_names:
        if patient_entity_name in workspace.children:
            for p in workspace.children[patient_entity_name]:
                patients[p['name']] = p
    assert len(patients) > 0, f"{context.patient_entity_names} not found in {workspace.children.keys()}"

    assert context.specimen_entity_name in workspace.children.keys()
    for s in workspace.children[context.specimen_entity_name]:
        specimens[s['name']] = s

    for task_entity in workspace.tasks:
        for task in workspace.tasks[task_entity]:
            for input in task['inputs']:
                if input['fhir_entity'] == 'Specimen':
                    assert input['name'] in specimens 
                    if 'tasks' not in specimens[input['name']]:
                        specimens[input['name']]['tasks'] = []
                    specimens[input['name']]['tasks'].append(task)

    for specimen_id, patient_id in workspace.specimen_patient_ids:
        if not isinstance(patient_id, str):
            # hmmm - should we always just make this a str
            patient_id = patient_id['entityName']
        assert patient_id in patients, f"{patient_id} not in {patients}"
        assert specimen_id in specimens, f"{specimen_id} not in {specimens}"
        if 'specimens' not in patients[patient_id]:
            patients[patient_id]['specimens'] = []
        patients[patient_id]['specimens'].append(specimens[specimen_id])
    # set on workspace
    workspace.patients = patients


@cli.command('normalize')
@click.option('--output_path', default=DEFAULT_OUTPUT_PATH, help=f'output path default={DEFAULT_OUTPUT_PATH}')
@click.option('--details', default=False, is_flag=True, help="Show the details.")
def normalize(output_path, details):
    """Read workspaces from db, map entity to standard FHIR entity names, write to stdout."""
    config = {
        "CMG": {
            "Patient": {
                "aliases": ["subject", "participant"]
            },
            "Specimen": {
                "aliases": ["sample"]                
            },
            "Task": {
                "aliases": ["sequencing", "discovery"]                
            },
            "FamilyRelationship": {
                "aliases": ["family"]                                
            }
        },
        "CCDG": {
            "Patient": {
                "aliases": ["subject", "participant", "pfb:subject"]
            },
            "Specimen": {
                "aliases": ["sample", "sample_set", "qc_result_sample"]                
            },
            "Task": {
                "aliases": ["sequencing", "discovery"]                
            },
            "FamilyRelationship": {
                "aliases": ["family"]                                
            }
        },
        "GTEx": {
            "Patient": {
                "aliases": ["participant"]
            },
            "Specimen": {
                "aliases": ["sample"]                
            }
        },
        "Public": {
            "Patient": {
                "aliases": ["participant"]
            },
            "Specimen": {
                "aliases": ["sample"] # , "sample_set", "qc_result_sample"]                
            },
            "FamilyRelationship": {
                "aliases": ["pedigree"]                                
            }
        },
        "NHGRI": {
            "Patient": {
                "aliases": ["participant"]
            },
            "Specimen": {
                "aliases": ["sample"] # , "sample_set"]                
            }
        },
        "NIMH": {
            "Patient": {
                "aliases": ["subject", "participant"]
            },
            "Specimen": {
                "aliases": ["sample"]                
            },
            "Task": {
                "aliases": ["sequencing"]                
            }
        }
    }

    entities = Entities(path=f"{output_path}/terra_entities.sqlite")

    # consortiums = recursive_default_dict()

    for workspace in entities.get_by_label('workspace'):
        workspace = AttrDict(workspace)

        consortium_name = workspace.consortium_name
        # if consortium_name not in ['Public']:
        #     continue
        if workspace.workspace.name != 'ANVIL_CMG_Broad_Muscle_Laing_WES':
            continue

        logger.info((consortium_name, workspace.workspace.name))

        # get all the other entities for this workspace
        children = entities.get_edges(src=workspace.workspace.name, src_name='workspace')
        workspace.children = children
        
        # wrangle the entities into a consistent, predictable form
        normalize_workspace(consortium_name, workspace, config, entities, output_path)
        try:
            assert workspace.patients, 'missing.patients'
            # assert workspace.specimens, 'missing.specimens'
            # assert workspace.specimen_patient_ids, 'missing.specimen.to.patient.links'
            # assert workspace.tasks, 'missing.tasks'
            # for task_entity in workspace.tasks:
            #     for task in workspace.tasks[task_entity]:
            #         for input in task['inputs']:
            #             assert 'fhir_entity' in input, 'missing.fhir_entity.in.task'
            #         for output_source, output in task['outputs'].items():
            #             for output_property, blob in output.items():
            #                 assert 'url' in blob, ('missing.url.in.blob', output_source, output_property)
        except Exception as e:
            logger.error((str(e) , consortium_name, workspace.workspace.name))
            continue

        import fhirclient.models.patient as FHIRPatient
        import fhirclient.models.organization as FHIROrganization
        import fhirclient.models.researchstudy as FHIRResearchStudy
        import fhirclient.models.researchsubject as FHIRResearchSubject
        import fhirclient.models.specimen as FHIRSpecimen
        import fhirclient.models.task as FHIRTask
        import fhirclient.models.documentreference as FHIRDocumentReference
        import fhirclient.models.observation as FHIRObservation
        import fhirclient.models.fhirreference as FHIRReference
        import fhirclient.models.identifier as FHIRIdentifier
        import fhirclient.models.identifier as FHIRIdentifier
        import fhirclient.models.practitioner as FHIRPractitioner
        import fhirclient.models.practitionerrole as FHIRPractitionerRole
        
        def _ref(fhir_resource):
            """Create a local reference for a fhir resource."""
            return FHIRReference.FHIRReference({'reference': f"{fhir_resource.resource_type}/{fhir_resource.id}"})

        def _id(*args):
            """Create legal fhir id, a reproducible SHA-1 hash of a workspace_name and resource_id."""
            name = '-'.join([x for x in args])
            namespace = bytes(bytearray("pyAnVIL".ljust(16), 'utf-8'))
            namespace_uuid = uuid.UUID(bytes=namespace)
            return str(uuid.uuid5(namespace_uuid, name))

        def _identifier(terra_entity):
            """Create FHIR compliant identifier from terra entity"""
            js = {
                "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{workspace.workspace.name}",
                "value": f"{terra_entity['entityType']}/{terra_entity['name']}",
            }
            _identifier = FHIRIdentifier.Identifier(js)
            return _identifier

        def _document_reference_identifier(output_source, output_property, specimen):
            """Create FHIR compliant identifier for document reference, add the blob origination entity and property."""
            js = {
                "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/{workspace.workspace.name}",
                "value": f"{specimen['entityType']}/{specimen['name']}#{output_source}/{output_property}",
            }
            _identifier = FHIRIdentifier.Identifier(js)
            return _identifier

        def _fhir_id(*args):
            """Create legal fhir id."""
            return re.sub(r"[^A-Za-z0-9\-\.]", "-", ".".join(str(a) for a in args))[-64:]


        def generate_fhir(workspace):
            """Generate FHIR resources from normalized workspace."""
            workspace_name = workspace.workspace.name
            anvil_org = FHIROrganization.Organization({'id': 'anvil'})
            consortium_org = FHIROrganization.Organization({'id': consortium_name})
            consortium_org.partOf = _ref(anvil_org)
            practitioner = FHIRPractitioner.Practitioner({'id': _fhir_id(workspace.practitioner)})
            workspace_org = FHIROrganization.Organization({'id': _id('Organization', workspace_name)})
            workspace_org.partOf = _ref(consortium_org)
            workspace_org.identifier = [FHIRIdentifier.Identifier({
                "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/",
                "value": f"{workspace_name}",
            })]
            practitioner_role = FHIRPractitionerRole.PractitionerRole(
                {'id': _id('PractitionerRole', workspace_name, workspace.practitioner)}
            )
            practitioner_role.practitioner = _ref(practitioner)
            practitioner_role.organization = _ref(workspace_org)
            practitioner_role.identifier = [FHIRIdentifier.Identifier({
                "system": f"https://anvil.terra.bio/#workspaces/anvil-datastorage/",
                "value": f"{workspace_name}",
            })]

            research_study = FHIRResearchStudy.ResearchStudy({'id': workspace_name, 'status': 'completed', })
            research_study.sponsor = _ref(workspace_org)

            yield anvil_org
            yield consortium_org
            yield practitioner
            yield workspace_org
            yield practitioner_role
            yield research_study

            for patient in workspace.patients.values():
                fhir_patient = FHIRPatient.Patient(
                    {
                        'id': _id(workspace_name, 'Patient', patient['name']),
                        'managingOrganization': _ref(workspace_org).as_json()
                    }
                )
                fhir_patient.identifier = [_identifier(patient)]
                yield fhir_patient
                research_subject = FHIRResearchSubject.ResearchSubject(
                    {
                        'id': _id(workspace_name, 'ResearchSubject', patient['name']),
                        'status': 'on-study',
                        'individual': _ref(fhir_patient).as_json(),
                        'study': _ref(research_study).as_json()
                    }
                )
                research_subject.identifier = [_identifier(patient)]
                yield research_subject
                for specimen in patient['specimens']:
                    fhir_specimen = FHIRSpecimen.Specimen({'id': _id(workspace_name, 'Specimen', specimen['name']), 'subject':_ref(fhir_patient).as_json()})
                    fhir_specimen.identifier = [_identifier(specimen)]
                    yield fhir_specimen
                    for task in specimen['tasks']:
                        # create unique task id
                        _task_id_keys = [input['name'] for input in task['inputs']]
                        for output_source, output in task['outputs'].items():
                            for output_property, blob in output.items():
                                _task_id_keys.append(blob['url'])
                        task_id = _id(*_task_id_keys)
                        fhir_task = FHIRTask.Task({'id': task_id, 'input':[], 'output': [], 'status': 'accepted', 'intent': 'unknown'})
                        fhir_task.identifier = [_identifier(task)]
                        for input in task['inputs']:
                            assert 'fhir_entity' in input, 'missing.fhir_entity.in.task'                    
                            fhir_task.input.append(FHIRTask.TaskInput(
                                {
                                    'type': {'coding': [{'code': 'Reference'}]},                                    
                                    'valueReference': {'reference': f"{input['fhir_entity']}/{_id(workspace_name, input['fhir_entity'], input['name'])}"}
                                }
                            ))
                        seen_already = set()
                        for output_source, output in task['outputs'].items():
                            for output_property, blob in output.items():
                                assert 'url' in blob, ('missing.url.in.blob', output_source, output_property)
                                document_reference = FHIRDocumentReference.DocumentReference(
                                    {
                                        'id': _id(blob['url']),
                                        'status': 'current',
                                        'content': [{'attachment': {'url': blob['url']} }],
                                        'subject': _ref(fhir_patient).as_json()
                                    }
                                )
                                document_reference.identifier = [_document_reference_identifier(output_source, output_property, task['inputs'][0])]

                                # FHIRDocumentReference.DocumentReferenceContent({'attachment': {'url': blob['url']} })
                                fhir_task.output.append(
                                    FHIRTask.TaskOutput(
                                        {'type': {'coding': [{'code': 'Reference'}]}, 'valueReference':  _ref(document_reference).as_json() }
                                    )
                                )
                                # multiple tasks can refer to same document, so de-duplicate
                                if document_reference.id not in seen_already:
                                    yield document_reference
                                seen_already.add(document_reference.id)
                        yield fhir_task


        # write FHIR resources
        emitters = {}
        for fhir_resource in generate_fhir(workspace):
            # print(fhir_resource.relativePath())
            resourceType = fhir_resource.resource_type
            dir_path = f"{output_path}/{consortium_name}/{workspace.workspace.name}"
            public_protected = 'protected'
            if resourceType in ['ResearchStudy', 'Organization', 'Practitioner', 'PractitionerRole']:
                public_protected = 'public'
            file_path = None
            if resourceType == 'Observation' and fhir_resource.focus:
                print(fhir_resource.focus)
                exit()
                if resourceType == 'Observation' and 'ResearchStudy' in focus_reference:
                    file_path = f"{dir_path}/public/ResearchStudyObservation.json"
                else:
                    file_path = f"{dir_path}/protected/Observation.json"
            if not file_path:
                file_path = f"{dir_path}/{public_protected}/ResearchStudyObservation.json"

            file_path = f"{dir_path}/protected/{fhir_resource.relativeBase()}.ndjson"
            emitter = emitters.get(file_path, None)
            if emitter is None:
                os.makedirs(f"{dir_path}/public", exist_ok=True)
                os.makedirs(f"{dir_path}/protected", exist_ok=True)
                emitter = open(file_path, "w")
                logging.info(f"Writing {file_path}")
                emitters[file_path] = emitter
            json.dump(fhir_resource.as_json(), emitter, separators=(',', ':'))
            emitter.write('\n')



        # consortiums[consortium_name][workspace.workspace.name] = workspace


    # counts = recursive_default_dict()
    # for consortium_name, consortium in consortiums.items():
    #     for workspace_name in consortium:
    #         workspace = consortium[workspace_name]
    #         total_patient_count = workspace.get('total_patient_count', None)
    #         sample_patient_ids = workspace.get('sample_patient_ids', None)
    #         if not total_patient_count:
    #             logger.error(('normalization_failed', consortium_name, workspace_name))
    #             continue
    #         if total_patient_count != len(sample_patient_ids):
    #             logger.error(('sample_patient_ids.reconcile.fail', consortium_name, workspace_name, total_patient_count, len(sample_patient_ids)))
    #         # counts[consortium_name][workspace_name] = len(sample_patient_ids)
    #         counts[consortium_name][workspace_name] = len(workspace.tasks.values())
    #         tasks_length = functools.reduce(operator.add, [len(d) for d in workspace.tasks.values()], 0)
    #         if len(workspace.specimens) != tasks_length:
    #             msg = {k:len(v)for k, v in workspace.tasks.items()}
    #             logger.error(('tasks.not.equal.specimens', consortium_name, workspace_name, total_patient_count, msg))
    #         # print(json.dumps(workspace.tasks))
    
    # print(json.dumps(counts))

if __name__ == '__main__':
    cli()

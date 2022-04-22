#!/usr/bin/env python3

"""Read metadata from terra workspaces."""

import json
import os.path

from anvil.etl.transformers import _recursive_default_dict
from anvil.etl.utilities.entities import Entities
from pathlib import Path
import pickle

# from . import LogCapture
# import logging
# logger = LogCapture('anvil.etl_old.transformers.normalizer')
# logger.addHandler(logging.root.handlers[0])

from . import logger

from anvil.etl.transformers.normalizer_methods import *
from .fhir_writer import ensure_data_store_name


def _extract_ontology_fields(output_path=None, entities=None, workspace_name=None):
    """Query db, sample first row in all entities, determine fields that have references to ontology."""
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
            ontology_keys = set()

            children = entities.get_edges_by_label(src=workspace.workspace.name, src_name='workspace', dst_name=entity_name, limit=20)
            for entity_name, child in children.items():

                for k, v in child['attributes'].items():
                    if isinstance(v, str) and 'gs://' not in v and ':' in v and 'date' not in k and 'pfb' not in k:
                        ontology_keys.add(k)

            if len(ontology_keys) == 0:
                continue
            yield {
                'consortium_name': workspace.consortium_name,
                'workspace_name': workspace.workspace.name,
                'entity_name': entity_name,
                'ontology_fields': list(ontology_keys),
            }


def _normalize_workspace(consortium_name, workspace, config, entities, output_path, validate_buckets):
    """Stitch the graph together."""

    #
    # Uses command pattern to wrangle some very messy data
    #

    #
    # 'main'
    #
    if list(workspace.children.keys()) == ['schema']:
        logger.error(('no.child.entities', consortium_name, workspace.workspace.name))
        return None

    # pass context to commands
    context = AttrDict({'logged_already': [], 'output_path': output_path, 'entities': entities})

    # find config
    exec_command(context, consortium_name, workspace, config, 'consortium_config')
    assert context.consortium_config, "Should set context config"
    assert context.inverted_config, "Should set context inverted_config"

    # who ran this study?
    exec_command(context, consortium_name, workspace, config, 'practitioner')
    assert workspace.practitioner, (f"Should set workspace.practitioner `{workspace.practitioner}`", workspace.workspace.attributes)

    # find entities with bucket fields
    exec_command(context, consortium_name, workspace, config, 'ensure_bucket_fields')
    if not context['bucket_fields']:
        logger.warning(("no.entity.contains.bucket.objects", consortium_name, workspace.workspace.name))

    # deduce entity names in workspace
    exec_command(context, consortium_name, workspace, config, 'patient_entity_names')
    assert context.patient_entity_names, "Should set context patient_entity_names"

    exec_command(context, consortium_name, workspace, config, 'specimen_entity_name')
    assert context.specimen_entity_name, "Should set context specimen_entity_name"
    assert isinstance(context.specimen_entity_name, str), context.specimen_entity_name

    # traverse from specimen to patient
    exec_command(context, consortium_name, workspace, config, 'link_specimen_to_patient')
    if 'specimen_patient_ids' not in workspace:
        logger.error(("no.specimen_patient_ids", consortium_name, workspace.workspace.name))
        return
    for specimen_id, patient_id in workspace.specimen_patient_ids:
        assert specimen_id and patient_id, f"All specimens should have patient: {(specimen_id, patient_id)}"

    # find tasks
    exec_command(context, consortium_name, workspace, config, 'task_entity_names')
    assert context.task_entity_names, "Should set context.task_entity_names"

    exec_command(context, consortium_name, workspace, config, 'ensure_tasks')
    assert workspace.tasks, ("context.tasks.empty", context.keys(), context.task_entity_names, context.consortium_config, workspace.children.keys())
    assert len(workspace.tasks) > 0, "context.tasks should be > 0"

    exec_command(context, consortium_name, workspace, config, 'ensure_tasks_populated')
    assert workspace.tasks, ("context.tasks.empty", context.keys(), context.task_entity_names, workspace.children.keys())
    assert len(workspace.tasks) > 0, "context.tasks should be > 0"

    exec_command(context, consortium_name, workspace, config, 'ensure_tasks_linked_to_documents')
    if len(workspace.tasks) == 0:
        logger.warning(("no.tasks", consortium_name, workspace.workspace.name))
    else:
        for task_source, task_list in workspace.tasks.items():
            for task in task_list:
                assert len(task['inputs']) > 0, ("tasks.missing.inputs", workspace.workspace.name, task_source, task, context.bucket_fields)
                assert len(task['outputs']) > 0, ("tasks.missing.outputs", workspace.workspace.name, task_source, task, context.bucket_fields)

    # link bucket data
    context['validate_buckets'] = validate_buckets
    exec_command(context, consortium_name, workspace, config, 'blob_attributes')

    # setup a normalized patient model
    # patient has specimens
    # specimens have tasks
    # tasks have input = specimen  
    # tasks have output = [document_reference]
    exec_command(context, consortium_name, workspace, config, 'patient_model')
    if 'patients' not in workspace:
        logger.error(('no.patients', workspace.workspace.name,))


def ontologies(output_path, consortium_name, workspace_name, details):
    """Determine if there are any fields that are ontologies in workspace."""
    for _consortium_name, _workspace_name in fetch_workspace_names(output_path=output_path,
                                                    requested_consortium_name=consortium_name,
                                                    workspace_name=workspace_name):
        yield [ontology_field for ontology_field in
               _extract_ontology_fields(output_path=output_path, workspace_name=_workspace_name)]


def get_pickled_workspace(output_path, consortium_name, workspace_name):
    """Unpickle the normalized workspace."""
    path = f"{output_path}/workspaces/{consortium_name}/{workspace_name}.pickle"
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'rb') as input_stream:
            return pickle.load(input_stream)
    except Exception as e:
        logger.warning(("no.pickle", workspace_name, str(e)))


def write_workspace(output_path, consortium_name, workspace_name, validate_buckets, config):
    """Read workspaces from db, normalize and write to file system."""

    for consortium_name, workspace in normalize(output_path, consortium_name, workspace_name, validate_buckets, config=config):
        Path(f"{output_path}/workspaces/{consortium_name}").mkdir(parents=True, exist_ok=True)
        with open(f"{output_path}/workspaces/{consortium_name}/{workspace.workspace.name}.pickle", 'wb') as output_stream:
            pickle.dump(workspace, output_stream)


def analyze(output_path, consortium_name, workspace_name, details, validate_buckets, config):
    """Read workspaces from db, normalize and analyze."""
    assert consortium_name
    G = _recursive_default_dict()
    for consortium_name, workspace in normalize(output_path, consortium_name, workspace_name, validate_buckets, config):
        specimen_count = 0
        task_count = 0
        patient_count = 0
        uniq_documents = set()
        documents_size = 0
        uniq_genders = set()
        phenotype_values = defaultdict(set)
        disease_values = defaultdict(set)
        family_relationship_keys = set()
        family_relationship_items = {}
        files = {}
        data_store_name = "None"

        for p in workspace.patients.values():
            patient_count += 1
            if p['gender']:
                uniq_genders.add(p['gender'])
            assert isinstance(p['phenotype_values'], list), (p['phenotype_values'], workspace_name)
            assert isinstance(p['disease_values'], list), (p['disease_values'], workspace_name)
            for phenotype_value in p['phenotype_values']:
                for key, value in phenotype_value.items():
                    # add list of values to set
                    phenotype_values[key].update(value)
            for disease_value in p['disease_values']:
                for key, value in disease_value.items():
                    disease_values[key].update(value)
            for family_relationship_value in p['family_relationship_values']:
                for key, value in family_relationship_value.items():
                    family_relationship_keys.add(key)
                family_relationship_items[p['name']] = family_relationship_value

            if 'specimens' not in p:
                continue

            specimen_count += len(p['specimens'])
            for s in p['specimens']:
                if 'tasks' in s:
                    task_count += len(s['tasks'])
                    for task in s['tasks']:
                        for output_source, output in task['outputs'].items():
                            for output_property, blob in output.items():
                                if blob['url'] not in uniq_documents:
                                    documents_size += int(blob.get('size', 0))
                                    uniq_documents.add(blob['url'])
                                    extension = blob['url'].split('/')[-1].replace('.gz', '').split('.')[-1]
                                    if extension not in files:
                                        files[extension] = {'size': 0, 'count': 0, 'drs_count': 0}
                                    files[extension]['size'] += int(blob.get('size', 0))
                                    files[extension]['count'] += 1
                                    if 'drs_uri' in blob and blob['drs_uri']:
                                        files[extension]['drs_count'] += 1

        # xform to a list
        for k, v in phenotype_values.items():
            phenotype_values[k] = sorted(list(v))
        for k, v in disease_values.items():
            disease_values[k] = sorted(list(v))

        G[consortium_name][workspace.workspace.name]['consortium'] = consortium_name
        G[consortium_name][workspace.workspace.name]['workspace'] = workspace.workspace.name
        G[consortium_name][workspace.workspace.name]['nodes']['patients'] = patient_count
        G[consortium_name][workspace.workspace.name]['nodes']['specimens'] = specimen_count
        G[consortium_name][workspace.workspace.name]['nodes']['tasks'] = task_count
        G[consortium_name][workspace.workspace.name]['nodes']['documents'] = len(uniq_documents)
        G[consortium_name][workspace.workspace.name]['phenotype_values'] = phenotype_values
        G[consortium_name][workspace.workspace.name]['disease_values'] = disease_values
        G[consortium_name][workspace.workspace.name]['gender'] = list(uniq_genders)
        G[consortium_name][workspace.workspace.name]['family_relationship_keys'] = list(family_relationship_keys)
        G[consortium_name][workspace.workspace.name]['documents_size'] = documents_size
        G[consortium_name][workspace.workspace.name]['files'] = files
        G[consortium_name][workspace.workspace.name]['data_store_name'] = ensure_data_store_name(workspace)
        G[consortium_name][workspace.workspace.name]['indication'] = None
        if 'tracker' in workspace and workspace['tracker']:
            G[consortium_name][workspace.workspace.name]['indication'] = workspace['tracker']["library:indication"]

        error_count = 0
        for error_name, entries in workspace.errors.items():
            error_count += len(entries)
            if details:
                try:
                    json.dumps(entries)
                except Exception as e:
                    import pprint
                    from itertools import islice
                    iterator = islice(entries, 10)
                    pp = pprint.PrettyPrinter(depth=10)
                    pp.pprint((workspace.workspace.name, [e for e in iterator]))
                    raise e
                G[consortium_name][workspace.workspace.name]['errors'][error_name] = entries
            else:
                G[consortium_name][workspace.workspace.name]['errors'][error_name] = len(entries)
        G[consortium_name][workspace.workspace.name]['error_count'] = error_count

    return G


def qa(output_path, consortium_name, workspace_name, config):
    """Produce a QA report."""
    pass


def fetch_workspace_names(output_path, requested_consortium_name, workspace_name):
    """Query db and return workspace names."""
    workspace_names = []
    entities = Entities(path=f"{output_path}/terra_entities.sqlite")
    for workspace in entities.get_by_label('workspace'):
        workspace = AttrDict(workspace)
        # filter consortium_name
        if requested_consortium_name and requested_consortium_name != workspace.consortium_name:
            continue

        # filter workspace_name
        if workspace_name and workspace.workspace.name != workspace_name:
            continue

        workspace_names.append((workspace.consortium_name, workspace.workspace.name))

    return workspace_names


def normalize(output_path, requested_consortium_name, workspace_name, validate_buckets, config):
    """Read workspaces from db, normalize."""
    workspace = get_pickled_workspace(output_path, requested_consortium_name, workspace_name)
    if workspace:
        yield workspace.consortium_name, workspace
        return

    config = config['consortiums']

    entities = Entities(path=f"{output_path}/terra_entities.sqlite")

    tracker = json.load(open(f"{output_path}/data_ingestion_tracker.json"))

    for workspace in entities.get_by_label('workspace'):
        workspace = AttrDict(workspace)
        # filter consortium_name
        if requested_consortium_name and requested_consortium_name != workspace.consortium_name:
            continue

        consortium_name = workspace.consortium_name
        # filter workspace_name
        if workspace_name and workspace.workspace.name != workspace_name:
            continue

        logger.info((consortium_name, workspace.workspace.name))

        # get all the other entities for this workspace
        children = entities.get_edges(src=workspace.workspace.name, src_name='workspace')
        workspace.children = children

        # clear logger's entries so we can harvest later
        logger.clear()
        # wrangle the entities into a consistent, predictable form
        _normalize_workspace(consortium_name, workspace, config, entities, output_path, validate_buckets)
        try:
            assert 'patients' in workspace , 'missing.patients'
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
            logger.error((str(e), consortium_name, workspace.workspace.name))
            continue

        tracker_info = [p for p in tracker if p['name'] == workspace.workspace.name]
        workspace.tracker = next(iter(tracker_info), None)

        workspace.errors = {k.replace('.', '-'): v for k, v in logger.entries.items()}
        # # workspace.errors = [v for v in logger.entries.values()]
        # for v in logger.entries.values():
        #     for item in v:
        #         print(item)
        # workspace.errors = [list(v) for v in logger.entries.values()]

        yield consortium_name, workspace


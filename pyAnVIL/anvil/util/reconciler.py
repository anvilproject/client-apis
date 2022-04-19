"""Reconcile and aggregate results."""

from anvil.dbgap.api import get_accession
from anvil.dbgap.api import get_study
from anvil.terra.reconciler import Reconciler
from collections import defaultdict
import logging
import os

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

# where we expect to find work databases, etc.
DEFAULT_OUTPUT_PATH = os.environ.get('OUTPUT_PATH', '/tmp')


def reconcile(name, user_project, namespace, workspace_regex, drs_file_path=None, terra_output_path=None, data_ingestion_tracker=None):
    """Run a reconciler on a set of workspaces, ."""
    logger = logging.getLogger('anvil.util.reconciler')
    reconciler = Reconciler(name, user_project, namespace, workspace_regex, drs_file_path, terra_output_path, data_ingestion_tracker)
    reconciler.save()
    reconciled_schemas = reconciler.reconcile_schemas()
    reconciled_schemas['name'] = name
    for view in reconciler.dashboard_views:
        accession = get_accession(namespace, view['project_id'])
        if accession:
            view['accession'] = accession
            study_tuple = get_study(accession)
            if not study_tuple:
                logger.warning(f"No study found {view['project_id']} accession: {accession}")
                view['problems'].append('missing_accession')
            else:
                qualified_accession, study = study_tuple
                view['qualified_accession'] = qualified_accession
                try:
                    view['dbgap_sample_count'] = len(study['DbGap']['Study']['SampleList']['Sample'])
                except Exception as e:
                    logger.warning(f"dbGAP's Study missing sample list {view['project_id']} accession: {accession} {e}")
                    view['dbgap_sample_count'] = 0
        yield view
    yield reconciled_schemas


def aggregate(namespace, user_project, consortium, drs_file_path=None, terra_output_path=None, data_ingestion_tracker=None):
    """Run a series of reconciliations."""
    def counts_factory():
        return {'expected_sample_count': 0, 'actual_sample_count': 0, 'problems': []}
    accessions = defaultdict(counts_factory)
    assert drs_file_path
    for name, workspace_regex in consortium:
        for view in reconcile(name, user_project, namespace, workspace_regex, drs_file_path, terra_output_path, data_ingestion_tracker):
            if 'qualified_accession' in view:
                accessions[view['qualified_accession']]['expected_sample_count'] = view['dbgap_sample_count']
                accessions[view['qualified_accession']]['actual_sample_count'] += [n for n in view['nodes'] if n['type'] == 'Samples'][0]['count']
            yield view
    for accession, counts in accessions.items():
        counts['workspace'] = None
        counts['qualified_accession'] = accession
        counts['project_id'] = accession
        counts['source'] = 'dbGAP'
        if counts['actual_sample_count'] != counts['expected_sample_count']:
            counts['problems'].append('dbgap_sample_count_mismatch')
        yield counts


def flatten(aggregations):
    """Render a dashboard data as key/value (flattened_aggregations, column_names)."""
    file_types = set()
    for p in aggregations:
        for f in p.get('files', []):
            file_types.add(f['type'])
    file_types = sorted(list(file_types))

    node_types = set()
    for p in aggregations:
        for n in p.get('nodes', []):
            node_types.add(n['type'])
    node_types = sorted(list(node_types))

    problem_types = set()
    for p in aggregations:
        for n in p.get('problems', []):
            problem_types.add(n)
    problem_types = sorted(list(problem_types))

    flattened = []
    for p in aggregations:
        flat = [p['source'], p['project_id'], p.get('qualified_accession', None)]

        file_sizes = [''] * len(file_types)
        for f in p.get('files', []):
            file_sizes[file_types.index(f['type'])] = f['size']
        flat.extend(file_sizes)
        flat.append(p.get('size', 0))
        flat.append(p.get('disease_ontology_id', None))

        node_counts = [''] * len(node_types)
        for n in p.get('nodes', []):
            node_counts[node_types.index(n['type'])] = n['count']
        flat.extend(node_counts)

        problems = [''] * len(problem_types)
        for n in p.get('problems', []):
            problems[problem_types.index(n)] = True
        flat.extend(problems)

        flattened.append(flat)

    return flattened, ['source', 'workspace', 'accession'] + file_types + ['size', 'disease_ontology_id'] + node_types + problem_types

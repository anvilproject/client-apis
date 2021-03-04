"""Reconcile and aggregate results."""

from anvil.dbgap.api import get_accession
from anvil.dbgap.api import get_study
from anvil.terra.reconciler import Reconciler
from collections import defaultdict
import logging

DEFAULT_NAMESPACE = 'anvil-datastorage'


def reconcile(name, user_project, namespace, workspace_regex, avro_path=None):
    """Run a reconciler on a set of workspaces, ."""
    logger = logging.getLogger('anvil.util.reconciler')
    reconciler = Reconciler(name, user_project, namespace, workspace_regex, avro_path)
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
                    logger.warning(f"Study missing sample list {view['project_id']} accession: {accession} {e}")
                    view['dbgap_sample_count'] = 0
        yield view
    yield reconciled_schemas


def aggregate(namespace, user_project, consortium, avro_path=None):
    """Run a series of reconciliations."""
    def counts_factory():
        return {'expected_sample_count': 0, 'actual_sample_count': 0, 'problems': []}
    accessions = defaultdict(counts_factory)
    for name, workspace_regex in consortium:
        for view in reconcile(name, user_project, namespace, workspace_regex, avro_path):
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

        node_counts = [''] * len(node_types)
        for n in p.get('nodes', []):
            node_counts[node_types.index(n['type'])] = n['count']
        flat.extend(node_counts)

        problems = [''] * len(problem_types)
        for n in p.get('problems', []):
            problems[problem_types.index(n)] = True
        flat.extend(problems)

        flattened.append(flat)

    return flattened, ['source', 'workspace', 'accession'] + file_types + ['size'] + node_types + problem_types

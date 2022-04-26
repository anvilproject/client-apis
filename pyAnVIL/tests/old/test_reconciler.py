"""Test terra wrapper."""

from anvil.util.reconciler import Reconciler
import logging

# logging.getLogger('anvil.test_terra').setLevel(logging.DEBUG)
logger = logging.getLogger('anvil.test_terra')


def test_reconciler_workspaces(user_project, namespaces, avro_path, terra_output_path, drs_output_path, project_pattern='.*CCDG.*'):
    """Ensure terra."""
    reconciler = Reconciler('CCDG', user_project, namespaces, project_pattern, avro_path, terra_output_path, drs_output_path)
    assert reconciler, "MUST create reconciler"
    assert len(reconciler.workspaces) == 338, "MUST have at least expected number of workspaces"
    for w in reconciler.workspaces:
        assert 'ccdg' in w.attributes['workspace']['name'].lower(), "name should include ccdg"


def test_reconciler_blobs(user_project, namespaces, avro_path, terra_output_path, drs_output_path, project_pattern='.*CCDG.*'):
    """Ensure terra."""
    reconciler = Reconciler('CCDG', user_project, namespaces, project_pattern, avro_path, terra_output_path, drs_output_path)
    attributes = reconciler.workspaces[0].attributes
    dashboard_view = reconciler.workspaces[0].dashboard_view
    assert sorted(dashboard_view.keys()) == ['createdDate', 'data_category', 'data_type', 'file_histogram', 'files', 'lastModified', 'nodes', 'problems', 'project_id', 'public', 'size']

    assert dashboard_view.size, f"MUST return blobs {attributes['workspace']['name']} {dashboard_view}"

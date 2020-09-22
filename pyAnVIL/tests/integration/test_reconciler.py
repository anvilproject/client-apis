"""Test terra wrapper."""

from anvil.util.reconciler import Reconciler
import logging

logging.getLogger('anvil.test_terra').setLevel(logging.DEBUG)
logger = logging.getLogger('anvil.test_terra')


def test_reconciler_workspaces(user_project, namespaces, project_pattern='.*CCDG.*'):
    """Ensure terra."""
    reconciler = Reconciler('CCDG', user_project, namespaces, project_pattern)
    assert reconciler, "MUST create reconciler"
    assert len(reconciler.workspaces) == 149, "MUST have at least expected number of workspaces"
    for w in reconciler.workspaces:
        assert 'ccdg' in w.attributes['workspace']['name'].lower(), "name should include ccdg"


def test_reconciler_blobs(user_project, namespaces, project_pattern='.*CCDG.*'):
    """Ensure terra."""
    reconciler = Reconciler('CCDG', user_project, namespaces, project_pattern)
    attributes = reconciler.workspaces[0].attributes
    dashboard_view = reconciler.workspaces[0].dashboard_view
    assert sorted(dashboard_view.keys()) == ['createdDate', 'data_category', 'data_type', 'file_histogram', 'files', 'lastModified', 'nodes', 'problems', 'project_id', 'public', 'size']
    assert dashboard_view.size, f"MUST return blobs {attributes['workspace']['name']} {dashboard_view}"
    # print(blobs.keys())
    # assert False

# def test_reconciler_no_billing():
#     """Ensure terra."""
#     self._user_project = user_project
#     self._namespaces = namespaces
#     self._project_pattern = project_pattern
#     reconciler = Reconciler()
#     assert reconciler, "MUST create reconciler"
#     assert len(reconciler.workspaces) > 0, "MUST have at least 1 workspace"

# reconciler = Reconciler('CCDG', 'terra-test-bwalsh', 'anvil-datastorage', 'AnVIL_CCDG_NYGC_NP_Autism_SSC_WGS')
# pprint.pprint(reconciler.problems)
# assert 'AnVIL_CCDG_NYGC_NP_Autism_ACE2_DS-MDS_WGS' in reconciler.problems.inconsistent_entityName
# assert 'AnVIL_CCDG_NYGC_NP_Autism_SSC_WGS' in reconciler.problems.missing_blobs
# assert len(reconciler.problems.missing_samples) == 0
# assert 'AnVIL_CCDG_WashU_CVD-NP-AI_Controls_VCControls_WGS' in reconciler.problems.inconsistent_subject
# pprint.pprint(reconciler.blob_sizes)
# pprint.pprint({'project_file_blob_sizes': reconciler.project_file_blob_sizes})
# pprint.pprint(reconciler.reconcile_schemas())
# for v in reconciler.dashboard_views:
#     pprint.pprint(v)

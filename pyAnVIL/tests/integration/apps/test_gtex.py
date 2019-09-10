from anvil.apps.gtex import GTEx
from datetime import datetime

EXPECTED_COHORT = 979
EXPECTED_SAMPLES = 17382
EXPECTED_NODE_COUNTS = {'anvil-datastorage/AnVIL_GTEx_V8_hg38': {'Project': 1, 'Subject': 979, 'Sample': 17382, 'BamFile': 18361, 'BaiFile': 18361, 'Demographic': 979, 'TxtFile': 1676, 'CramFile': 879, 'CraiFile': 879, 'TsvFile': 1676}}


def test_to_graph():
    start = datetime.now()
    gtex = GTEx()
    projects = [p for p in gtex.get_terra_projects()]
    participants = [p for p in gtex.get_terra_participants()]
    samples = [p for p in gtex.get_terra_samples()]
    print(datetime.now() - start)

    assert len(projects) == 1, 'Should have 1 project'
    for p in projects:
        assert p.project_id, 'Project should have project_id'

    assert len(participants) == EXPECTED_COHORT, f'Should have {EXPECTED_COHORT} participants, got {len(participants)}'
    for p in participants:
        assert p.project_id, 'Participant should have project_id'

    assert len(samples) == EXPECTED_SAMPLES, f'Should have {EXPECTED_SAMPLES} samples, got {len(samples)}'
    for s in samples:
        assert s.files, 'Sample should have files'
        assert s.project_id, 'Sample should have project_id'

    graph = gtex.to_graph()
    assert graph

    assert gtex.graph_node_counts() == EXPECTED_NODE_COUNTS, gtex.graph_node_counts()

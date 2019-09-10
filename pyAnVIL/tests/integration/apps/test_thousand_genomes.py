from anvil.apps.thousand_genomes import ThousandGenomes
from datetime import datetime
EXPECTED_SAMPLES = EXPECTED_COHORT = 2504
EXPECTED_NODE_COUNTS = {'anvil-datastorage/1000G-high-coverage-2019': {'Project': 1, 'Subject': 2504, 'Sample': 2504, 'CramFile': 2504, 'Population': 26}}


def test_to_graph():
    start = datetime.now()
    _1000g = ThousandGenomes()
    projects = [p for p in _1000g.get_terra_projects()]
    participants = [p for p in _1000g.get_terra_participants()]
    samples = [p for p in _1000g.get_terra_samples()]
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
    graph = _1000g.to_graph()
    assert graph, 'should return a graph'
    assert _1000g.graph_node_counts() == EXPECTED_NODE_COUNTS, _1000g.graph_node_counts()

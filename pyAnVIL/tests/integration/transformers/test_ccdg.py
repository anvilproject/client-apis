from anvil.transformers.ccdg import CCDG
from datetime import datetime

EXPECTED_PROJECTS = 51
EXPECTED_COHORT = 54302
EXPECTED_SAMPLES = 54303
EXPECTED_FAMILY = 4998
EXPECTED_DIAGNOSIS = 4


def test_to_graph():
    start = datetime.now()
    ccdg = CCDG()
    projects = [p for p in ccdg.get_terra_projects()]
    participants = [p for p in ccdg.get_terra_participants()]
    samples = [p for p in ccdg.get_terra_samples()]
    print(datetime.now() - start)

    assert len(projects) == EXPECTED_PROJECTS, f'Should have {EXPECTED_PROJECTS} project, got {len(projects)}'
    for p in projects:
        assert p.project_id, f'Project should have project_id {p}'

    assert len(participants) == EXPECTED_COHORT, f'Should have {EXPECTED_COHORT} participants, got {len(participants)}'
    for p in participants:
        assert p.project_id, 'Participant should have project_id'

    assert len(samples) == EXPECTED_SAMPLES, f'Should have {EXPECTED_SAMPLES} samples, got {len(samples)}'
    for s in samples:
        assert s.project_id, 'Sample should have project_id'

    graph = ccdg.to_graph()
    assert graph

    node_counts = ccdg.graph_node_counts()
    diagnosis_count = demographic_count = family_count = sample_count = subject_count = 0
    for project_id, node_count in node_counts.items():
        subject_count += node_count['Subject']
        sample_count += node_count['Sample']
        family_count += node_count.get('Family', 0)
        demographic_count += node_count.get('Demographic', 0)
        diagnosis_count += node_count.get('Diagnosis', 0)

    assert sample_count == subject_count == EXPECTED_COHORT
    assert family_count == EXPECTED_FAMILY
    assert len(node_counts.keys()) == EXPECTED_PROJECTS
    assert demographic_count == EXPECTED_COHORT
    assert diagnosis_count == EXPECTED_DIAGNOSIS

"""This module tests adminstration."""

from anvil import terra


def test_anvil_project_entities(namespace='anvil-datastorage', project_pattern="^AnVIL_GTEx_V8_hg38$"):
    """Should return projects."""
    projects = terra.get_projects([namespace], project_pattern=project_pattern)
    assert len(projects) == 1, f"Should have 1 project {project_pattern}"
    project = projects[0]
    participants = terra.get_entities(namespace=project.program, workspace=project.project, entity_name='participant')
    assert len(participants) > 1, f"Should have more than one participant {project_pattern}"
    samples = terra.get_entities(namespace=project.program, workspace=project.project, entity_name='sample')
    assert len(samples) > 1, f"Should have more than one sample {project_pattern}"

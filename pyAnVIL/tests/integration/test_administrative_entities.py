"""This module tests adminstration."""

from anvil import terra


def test_programs():
    """Should return program/namespace and project/workspace."""
    programs = terra.get_programs()
    assert len(programs) > 0, "Should return at least one program"
    namespaces = terra.get_namespaces()
    assert len(namespaces) > 0, "Should return at least one namespace"
    assert namespaces == programs


def test_projects():
    """Should return project/workspace."""
    projects = terra.get_projects()
    assert len(projects) > 0, "Should return at least one project"
    workspaces = terra.get_workspaces()
    assert len(workspaces) > 0, "Should return at least one workspaces"
    assert workspaces == projects


def test_anvil_projects(namespace='anvil-datastorage'):
    """Should return projects."""
    projects = terra.get_projects([namespace])
    assert len(projects) > 0, "Should return at least one project"


def test_anvil_CMG_projects(namespace='anvil-datastorage', project_pattern=".*CMG.*"):
    """Should return projects."""
    projects = terra.get_projects([namespace], project_pattern=project_pattern)
    assert len(projects) > 0, "Should have at least 1 project matching {}".format(project_pattern)
    for p in projects:
        assert 'CMG' in p.project, 'Should be a CMG project {}'.format(p.project)

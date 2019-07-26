"""This module tests adminstration."""

from anvil import anvil

def test_programs():
    """Should return program/namespace and project/workspace."""
    programs = anvil.get_programs()
    assert len(programs) > 0, "Should return at least one program"
    namespaces = anvil.get_namespaces()
    assert len(namespaces) > 0, "Should return at least one namespace"
    assert namespaces == programs


def test_projects():
    """Should return project/workspace."""
    projects = anvil.get_projects()
    assert len(projects) > 0, "Should return at least one project"
    workspaces = anvil.get_workspaces()
    assert len(workspaces) > 0, "Should return at least one workspaces"
    assert workspaces == projects


def test_anvil_projects(namespace='anvil-datastorage'):
    """Should return projects."""
    projects = anvil.get_projects([namespace])
    assert len(projects) > 0, "Should return at least one project"


def test_anvil_CMG_projects(namespace='anvil-datastorage', project_pattern=".*CMG.*"):
    """Should return projects."""
    projects = anvil.get_projects([namespace], project_pattern=project_pattern)
    assert len(projects) > 0, "Should have at least 1 project matching {}".format(project_pattern)
    for p in projects:
        assert 'CMG' in p.project, 'Should be a CMG project {}'.format(p.project)

    projects = [anvil.get_project_schema(p) for p in projects]
    for p in projects:
        if len(p.schema.keys()) == 0:
            print('Should have a schema {}'.format(p.project))
    projects = [p for p in projects if len(p.schema.keys()) > 0]
    assert False


def XXXtest_anvil_CMG_projects(namespace='anvil-datastorage'):
    """Should return projects."""
    projects = anvil.get_project_schemas([namespace])
    CMG_projects = [p for p in projects if 'CMG' in p['project']]
    no_schema_projects = [p for p in projects if len(p['schema']) == 0]
    if len(no_schema_projects) > 0:
        print('Missing schema', [p['project'] for p in no_schema_projects])
    assert False, "Foo"

    # projects = anvil.get_projects([namespace])
    # projects = [project for project in projects if 'CMG' in project]
    # assert len(projects) > 0, "Should return at least one project"
    # participant_schemas = defaultdict(list)
    # for project in projects:
    #     schema = anvil.get_project_schema(namespace, project)
    #     if len(schema.keys()) < 1:
    #         print('no schema for {}/{}'.format(namespace, project))
    #         continue
    #     attributes = ','.join(sorted(schema['participant']['attributeNames']))
    #     participant_schemas[attributes].append(project)
    # schema_0 = list(participant_schemas.keys())[0].split(',')
    # schema_1 = list(participant_schemas.keys())[1].split(',')
    # participant_diff = set(schema_0).symmetric_difference(set(schema_1))
    # # assert not participant_diff, participant_diff
    #
    # sample_schemas = defaultdict(list)
    # for project in projects:
    #     schema = anvil.get_project_schema(namespace, project)
    #     if len(schema.keys()) < 1:
    #         print('no schema for {}/{}'.format(namespace, project))
    #         continue
    #     attributes = ','.join(sorted(schema['sample']['attributeNames']))
    #     sample_schemas[attributes].append(project)
    # print('sample_schemas # : ', len(sample_schemas.keys()))

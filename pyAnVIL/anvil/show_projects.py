import click
import anvil.terra as terra
import sys
import logging


@click.command()
@click.option('--namespace', default='anvil-datastorage', help='List terra namespaces')
@click.option('--project_pattern', default='.*', help='Regular expression')
@click.option('--user_project', default=None, help='Google billing account')
def all_projects(namespace, project_pattern, user_project):
    """Prints list of projects."""
    projects = terra.get_projects([namespace], project_pattern=project_pattern, user_project=user_project)
    assert len(projects) > 0, "Should have at least 1 project matching {}".format(project_pattern)
    projects = [terra.get_project_schema(p) for p in projects]
    for p in projects:
        if len(p.schema.keys()) == 0:
            print('{} missing schema'.format(p.project), file=sys.stderr)
    projects = [p for p in projects if len(p.schema.keys()) > 0]
    for p in projects:
        print(p.project)


if __name__ == '__main__':

    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    all_projects()

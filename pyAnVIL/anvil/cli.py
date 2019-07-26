import click
import anvil
import sys

@click.command()
@click.option('--count', default=1, help='Number of greetings.')
@click.option('--name', prompt='Your name',
              help='The person to greet.')


def hello(count, name):
    """Simple program that greets NAME for a total of COUNT times."""
    for x in range(count):
        click.echo('Hello %s!' % name)

@click.command()
@click.option('--namespace', default='anvil-datastorage', help='Terra namespace to query')
@click.option('--project_pattern', default='.*CMG.*', help='Regular expression')
def CMG_projects(namespace, project_pattern):
    """Should return projects."""
    projects = anvil.get_projects([namespace], project_pattern=project_pattern)
    assert len(projects) > 0, "Should have at least 1 project matching {}".format(project_pattern)
    projects = [anvil.get_project_schema(p) for p in projects]
    for p in projects:
        if len(p.schema.keys()) == 0:
            print('{} missing schema'.format(p.project), file=sys.stderr)
    projects = [p for p in projects if len(p.schema.keys()) > 0]
    for p in projects:
        print(p.project)



if __name__ == '__main__':
    CMG_projects()

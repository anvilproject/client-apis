import os
import pkg_resources
import yaml
import click
from collections import OrderedDict

DEFAULT_OUTPUT_PATH = os.environ.get('OUTPUT_PATH','./DATA')

DEFAULT_GEN3_CREDENTIALS_PATH = os.path.expanduser('~/.gen3/credentials.json')

DEFAULT_GOOGLE_PROJECT = os.environ.get('GOOGLE_PROJECT', None)


def read_config(path=None):
    """Read config, if no path, read from installed resource."""

    if not path:
        resource_package = 'anvil.etl'
        path = 'utilities/config.yaml'  # Do not use os.path.join()
        config_file = pkg_resources.resource_stream(resource_package, path)
    else:
        config_file = open(path)

    config = yaml.load(config_file, Loader=yaml.SafeLoader)
    assert sorted(config.keys()) == ['consortiums', 'mapping'], f"Config missing expected keys. {config.keys()} "

    return config


class NaturalOrderGroup(click.Group):

    def __init__(self, name=None, commands=None, **attrs):
        super(NaturalOrderGroup, self).__init__(
            name=name, commands=None, **attrs)
        if commands is None:
            commands = OrderedDict()
        elif not isinstance(commands, OrderedDict):
            commands = OrderedDict(commands)
        self.commands = commands

    def list_commands(self, ctx):
        return self.commands.keys()
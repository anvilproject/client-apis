import os
import json
import pytest
import yaml
from click.testing import CliRunner
from anvil.etl.anvil_etl import cli
from importlib_metadata import distribution
import logging
from anvil.etl.utilities.shell_helper import PROPERTIES_LIST

logger = logging.getLogger(__name__)


def test_help():
    """Simply check options and commands."""
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    # options
    for k in "--log-level,--output_path,--config_path".split(","):
        assert k in result.output
    # commands
    for k in "extract,transform,load,utility,version".split(","):
        assert k in result.output


def test_version():
    """Version should match."""
    runner = CliRunner()
    result = runner.invoke(cli, ['version'])
    assert result.exit_code == 0, result.output
    dist = distribution('pyAnVIL')
    assert dist.version in result.output


def test_config_format():
    """Should be able to print config in json or yaml."""
    runner = CliRunner()
    # json is default
    result = runner.invoke(cli, ['utility', 'config'])
    assert result.exit_code == 0
    assert json.loads(result.output), result.output

    # specify yaml
    result = runner.invoke(cli, ['utility', 'config', '--format', 'yaml'])
    assert result.exit_code == 0, result.output
    assert yaml.safe_load(result.output), result.output


def test_utility_env():
    """Should be able to print export variables."""
    runner = CliRunner()
    # json is default
    result = runner.invoke(cli, ['utility', 'env'])
    assert result.exit_code == 0, result.output
    for p in PROPERTIES_LIST:
        assert f"export {p}=" in result.output, result.output
    logger.debug("All expected properties present.")


@pytest.mark.skipif(reason="Setting environmental variables to arbitrary values interferes with downstream tests.")
def test_utility_existing_env():
    """Should respect existing variables."""
    runner = CliRunner()
    for p in PROPERTIES_LIST:
        os.environ[p] = "FOO"
    result = runner.invoke(cli, ['utility', 'env'])
    assert result.exit_code == 0, result.output
    for p in PROPERTIES_LIST:
        assert f"export {p}=FOO" in result.output, result.output
    logger.debug("Existing property values respected.")


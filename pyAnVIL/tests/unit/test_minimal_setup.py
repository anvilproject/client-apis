"""This module tests basic project setup."""

import sys
from subprocess import Popen, PIPE


def test_python_version():
    """Should use python 3.7.1."""
    assert sys.version_info.major == 3, "Must use Python 3"
    assert sys.version_info.minor >= 7, "Must use at least Python 3.7"


def test_import_main_package():
    """Should have a dependencies installed."""
    import gen3  # noqa: F401
    import firecloud.api as fapi  # noqa: F401


def test_gcloud_cli():
    """Should have a gcloud cli installed."""
    cmd = ['gcloud', '--version']
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)
    version, stderr = p.communicate()
    version = version.decode("utf-8").rstrip()
    assert 'Google Cloud SDK' in version, f'Must have gcloud installed. gcloud --version {stderr}'

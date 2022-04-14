import subprocess
from subprocess import Popen, PIPE
import os
import logging
logger = logging.getLogger(__name__)


def run_cmd(command_line):
    """Run a command line, return stdout."""
    try:
        return subprocess.check_output(command_line, shell=True).decode("utf-8").rstrip()
    except Exception as e:
        logger.error(e)


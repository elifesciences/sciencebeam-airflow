import logging
import subprocess
import sys


LOGGER = logging.getLogger(__name__)


def run_command(command, **kwargs):
    LOGGER.info('running command: %s', command)
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE, stdout=sys.stdout, stderr=sys.stderr,
        **kwargs
    )
    process.wait()
    if process.returncode != 0:
        raise RuntimeError(
            'Command %s failed: exit code: %s' %
            (command, process.returncode)
        )

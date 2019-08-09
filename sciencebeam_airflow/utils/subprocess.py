import logging
import shlex
import subprocess
import sys


LOGGER = logging.getLogger(__name__)


def run_command(command, **kwargs):
    if isinstance(command, list):
        command_args = command
    else:
        command_args = shlex.split(command)
    LOGGER.info('running command: %s', command_args)
    process = subprocess.Popen(
        command_args,
        stdin=subprocess.PIPE, stdout=sys.stdout, stderr=sys.stderr,
        **kwargs
    )
    stdout_data, _ = process.communicate()
    LOGGER.info('command output: %s', stdout_data)
    if process.returncode != 0:
        raise RuntimeError(
            'Command %s failed: exit code: %s (output: %s)' %
            (command_args, process.returncode, stdout_data)
        )

import argparse
import shlex
import logging
from typing import Dict, Type

from airflow.operators.bash_operator import BashOperator


LOGGER = logging.getLogger(__name__)


def parse_command_arg(command: str, arg_type_by_name: Dict[str, Type]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('command')
    parser.add_argument('positional', nargs='*')
    for arg_name, arg_type in arg_type_by_name.items():
        if isinstance(arg_type, list):
            parser.add_argument(arg_name, action='append', type=arg_type[0])
        elif arg_type == bool:
            parser.add_argument(arg_name, action='store_true')
        else:
            parser.add_argument(arg_name, type=arg_type)
    # parser.add_argument('remainder', nargs='*')
    args = shlex.split(command.strip())
    if '--' in args:
        args.remove('--')
    LOGGER.info('args: %s', args)
    opt, remainder = parser.parse_known_args(args)
    opt.remainder = remainder
    LOGGER.info('opt: %s', opt)
    return opt


def render_bash_command(operator: BashOperator, airflow_context: dict) -> str:
    return operator.render_template(
        operator.bash_command, airflow_context
    )


def create_and_render_command(operator, airflow_context: dict) -> str:
    LOGGER.info('airflow_context: %s', airflow_context)
    LOGGER.info('airflow_context.dag_run.conf: %s', airflow_context['dag_run'].conf)
    rendered_bash_command = render_bash_command(operator, airflow_context).strip()
    LOGGER.info('rendered_bash_command: %s', rendered_bash_command)
    return rendered_bash_command

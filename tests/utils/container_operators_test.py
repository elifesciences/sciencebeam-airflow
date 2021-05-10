import pytest

from sciencebeam_airflow.utils.container import (
    _get_select_preemptible_json,
    _get_highcpu_json
)
from sciencebeam_airflow.utils.container_operators import ContainerRunOperator

from ..test_utils import create_and_render_command, parse_command_arg


class TestContainerRunOperator:
    def test_should_add_requests(self, dag, airflow_context):
        container_requests = 'cpu=123m,memory=123Mi'
        operator = ContainerRunOperator(
            dag,
            task_id='task1',
            namespace='namespace1',
            image='image1',
            name='name1',
            command='command1',
            requests=container_requests
        )
        rendered_bash_command = create_and_render_command(operator, airflow_context)
        args = parse_command_arg(rendered_bash_command, {'--requests': str})
        assert args.requests == container_requests

    def test_should_allow_requests_expression(
        self, dag, airflow_context, dag_run
    ):
        container_requests = 'cpu=123m,memory=123Mi'
        dag_run.conf = {
            'container_requests': container_requests
        }
        operator = ContainerRunOperator(
            dag,
            task_id='task1',
            namespace='namespace1',
            image='image1',
            name='name1',
            command='command1',
            requests='{{ dag_run.conf.container_requests }}'
        )
        rendered_bash_command = create_and_render_command(operator, airflow_context)
        args = parse_command_arg(rendered_bash_command, {'--requests': str})
        assert args.requests == container_requests

    @pytest.mark.parametrize("preemptible", [False, True])
    def test_should_allow_preemptible_expression(
        self, dag, airflow_context, dag_run,
        preemptible: bool
    ):
        dag_run.conf = {
            'preemptible': preemptible
        }
        operator = ContainerRunOperator(
            dag,
            task_id='task1',
            namespace='namespace1',
            image='image1',
            name='name1',
            command='command1',
            preemptible='{{ dag_run.conf.preemptible }}'
        )
        rendered_bash_command = create_and_render_command(operator, airflow_context)
        args = parse_command_arg(rendered_bash_command, {'--overrides': str})
        if preemptible:
            assert args.overrides == _get_select_preemptible_json()
        else:
            assert args.overrides is None

    @pytest.mark.parametrize("highcpu", [False, True])
    def test_should_allow_highcpu_expression(
        self, dag, airflow_context, dag_run,
        highcpu: bool
    ):
        container_requests = 'cpu=123m,memory=123Mi'
        dag_run.conf = {
            'highcpu': highcpu,
            'requests': container_requests
        }
        operator = ContainerRunOperator(
            dag,
            task_id='task1',
            namespace='namespace1',
            image='image1',
            name='name1',
            command='command1',
            highcpu='{{ dag_run.conf.highcpu }}',
            requests='{{ dag_run.conf.requests }}'
        )
        rendered_bash_command = create_and_render_command(operator, airflow_context)
        args = parse_command_arg(rendered_bash_command, {'--overrides': str})
        if highcpu:
            assert args.overrides == _get_highcpu_json()
        else:
            assert args.overrides is None

from unittest.mock import patch, MagicMock

import pytest

import dags.sciencebeam_evaluate as sciencebeam_evaluate_module
from dags.sciencebeam_evaluate import (
    create_dag,
    create_sciencebeam_evaluate_op
)

from .test_utils import create_and_render_command, parse_command_arg

from .sciencebeam_test_utils import DEFAULT_CONF


SCIENCEBEAM_JUDGE_IMAGE_1 = 'judge:0.0.1'


FIELD_1 = 'field1'


@pytest.fixture(name='get_sciencebeam_judge_image_mock')
def _get_sciencebeam_judge_image_mock():
    with patch.object(sciencebeam_evaluate_module, 'get_sciencebeam_judge_image') as mock:
        mock.return_value = SCIENCEBEAM_JUDGE_IMAGE_1
        yield mock


def _create_and_render_evaluate_command(dag, airflow_context: dict) -> str:
    return create_and_render_command(
        create_sciencebeam_evaluate_op(dag=dag),
        airflow_context
    )


class TestScienceBeamEvaluate:
    class TestCreateDag:
        def test_should_be_able_to_create_dag(self):
            create_dag()

    class TestCreateScienceBeamEvaluateOperator:
        def test_should_include_namespace(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--namespace': str})
            assert getattr(opt, 'namespace') == DEFAULT_CONF['namespace']

        def test_should_include_image(
                self, dag, airflow_context, dag_run,
                get_sciencebeam_judge_image_mock: MagicMock):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--image': str})
            assert getattr(opt, 'image') == SCIENCEBEAM_JUDGE_IMAGE_1
            get_sciencebeam_judge_image_mock.assert_called_with(DEFAULT_CONF)

        def test_should_not_pass_fields_argument_if_no_fields_configured(
                self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'fields': ''
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--fields': str})
            assert getattr(opt, 'fields') is None

        def test_should_include_configured_fields(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'fields': FIELD_1
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--fields': str})
            assert getattr(opt, 'fields') == FIELD_1

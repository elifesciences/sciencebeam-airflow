from unittest.mock import patch, MagicMock

import pytest

import dags.sciencebeam_evaluate as sciencebeam_evaluate_module
from dags.sciencebeam_evaluate import (
    create_dag,
    create_sciencebeam_evaluate_op,
    DEFAULT_JUDGE_CONTAINER_REQUESTS
)

from .test_utils import create_and_render_command, parse_command_arg

from .sciencebeam_test_utils import DEFAULT_CONF as _DEFAULT_CONF


SCIENCEBEAM_JUDGE_IMAGE_1 = 'judge:0.0.1'


FIELD_1 = 'field1'


DEFAULT_CONF = {
    **_DEFAULT_CONF,
    'source_data_path': '/path/to/source',
    'source_file_list': 'file-list.lst',
    'output_data_path': '/path/to/output',
    'output_file_list': 'output-file-list.lst',
    'eval_output_path': '/path/to/eval-output',
    'limit': '123'
}


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
                'config': {}
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--fields': str})
            assert getattr(opt, 'fields') is None

        def test_should_include_configured_fields(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'config': {
                    'evaluate': {
                        'fields': FIELD_1
                    }
                }
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--fields': str})
            assert getattr(opt, 'fields') == FIELD_1

        def test_should_include_configured_fields_starting_with_hyphen(
                self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'config': {
                    'evaluate': {
                        'fields': '-' + FIELD_1
                    }
                }
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--fields': str})
            assert getattr(opt, 'fields') == '-' + FIELD_1

        def test_should_not_pass_metrics_argument_if_not_configured(
                self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'config': {}
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--measures': str})
            assert getattr(opt, 'measures') is None

        def test_should_include_configured_metrics(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'config': {
                    'evaluate': {
                        'measures': 'measure1'
                    }
                }
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--measures': str})
            assert getattr(opt, 'measures') == 'measure1'

        def test_should_not_pass_scoring_type_overrides_argument_if_not_configured(
                self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'config': {}
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--scoring-type-overrides': str})
            assert getattr(opt, 'scoring_type_overrides') is None

        def test_should_include_configured_scoring_type_overrides(
                self, dag, airflow_context, dag_run):
            scoring_type_overrides = 'field1=type1|field2=type2'
            dag_run.conf = {
                **DEFAULT_CONF,
                'config': {
                    'evaluate': {
                        'scoring_type_overrides': scoring_type_overrides
                    }
                }
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--scoring-type-overrides': str})
            assert getattr(opt, 'scoring_type_overrides') == scoring_type_overrides

        def test_should_include_target_file_list(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'dataset': {
                    'target_file_list': '/path/to/target/file-list.lst'
                }
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--target-file-list': str})
            assert getattr(opt, 'target_file_list') == '/path/to/target/file-list.lst'

        def test_should_use_source_file_list_by_default(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'source_data_path': '/path/to/source',
                'source_file_list': 'file-list.lst'
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--target-file-list': str})
            assert getattr(opt, 'target_file_list') == '/path/to/source/file-list.lst'

        def test_should_add_sciencebeam_judge_args(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'config': {
                    'evaluate': {
                        'sciencebeam_judge_args': 'arg1'
                    }
                }
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {})
            assert opt.remainder[-1:] == ['arg1']  # pylint: disable=no-member

        def test_should_use_default_container_requests(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--requests': str})
            assert getattr(opt, 'requests') == DEFAULT_JUDGE_CONTAINER_REQUESTS

        def test_should_be_able_to_override_container_requests(self, dag, airflow_context, dag_run):
            container_requests = 'cpu=123m,memory=123Mi'
            dag_run.conf = {
                **DEFAULT_CONF,
                'config': {
                    'evaluate': {
                        'container': {
                            'requests': container_requests
                        }
                    }
                }
            }
            rendered_bash_command = _create_and_render_evaluate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--requests': str})
            assert getattr(opt, 'requests') == container_requests

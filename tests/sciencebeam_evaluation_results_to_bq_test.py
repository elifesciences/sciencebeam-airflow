from unittest.mock import patch, MagicMock

import pytest

import dags.sciencebeam_evaluation_results_to_bq as sciencebeam_evaluation_results_to_bq_module
from dags.sciencebeam_evaluation_results_to_bq import (
    create_dag,
    create_evaluation_results_to_jsonl_op,
    create_upload_to_bq_op,
    create_remove_jsonl_file_op,
    ConfigProps
)

from .sciencebeam_test_utils import DEFAULT_CONF as _DEFAULT_CONF

from .test_utils import (
    parse_command_arg,
    create_and_render_command
)


URL_1 = 'gs://bucket/path'


DEFAULT_CONF = {
    **_DEFAULT_CONF,
    ConfigProps.EVAL_OUTPUT_PATH: 'eval_output_path1',
    ConfigProps.DATASET_NAME: 'dataset_name1',
    ConfigProps.MODEL_NAME: 'model1'
}


@pytest.fixture(name='convert_evaluation_results_to_jsonl_file_mock')
def _convert_evaluation_results_to_jsonl_file_mock():
    _module = sciencebeam_evaluation_results_to_bq_module
    with patch.object(_module, 'convert_evaluation_results_to_jsonl_file') as mock:
        yield mock


def _create_and_render_upload_to_bq_command(dag, airflow_context: dict) -> str:
    return create_and_render_command(
        create_upload_to_bq_op(dag=dag),
        airflow_context
    )


def _create_and_render_remove_jsonl_file_command(dag, airflow_context: dict) -> str:
    return create_and_render_command(
        create_remove_jsonl_file_op(dag=dag),
        airflow_context
    )


class TestScienceBeamEvaluationResultsToBq:
    class CreateDag:
        def test_should_be_able_to_create_dag(self):
            create_dag()

    class TestCreateEvaluationResultsToJsonlOperator:
        def test_should_call_convert_evaluation_results_to_jsonl_file_mock(
                self, dag, airflow_context, dag_run,
                convert_evaluation_results_to_jsonl_file_mock: MagicMock):
            dag_run.conf = DEFAULT_CONF
            operator = create_evaluation_results_to_jsonl_op(dag=dag)
            operator.execute(airflow_context)
            convert_evaluation_results_to_jsonl_file_mock.assert_called_with(
                eval_output_path=DEFAULT_CONF[ConfigProps.EVAL_OUTPUT_PATH],
                dataset_name=DEFAULT_CONF[ConfigProps.DATASET_NAME],
                model_name=DEFAULT_CONF[ConfigProps.MODEL_NAME]
            )

    class TestCreateUploadToBqOperator:
        def test_should_pass_bq_table_name(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_upload_to_bq_command(
                dag, airflow_context
            )
            opt = parse_command_arg(rendered_bash_command, {'--source_format': str})
            expected_table_name = f"{DEFAULT_CONF['namespace']}.document_field_evaluation"
            assert getattr(opt, 'remainder')[0] == expected_table_name

        def test_should_pass_source_url(self, dag, airflow_context, dag_run, task_instance):
            dag_run.conf = DEFAULT_CONF
            task_instance.xcom_pull.return_value = URL_1
            rendered_bash_command = _create_and_render_upload_to_bq_command(
                dag, airflow_context
            )
            opt = parse_command_arg(rendered_bash_command, {'--source_format': str})
            assert getattr(opt, 'remainder')[1] == URL_1

    class TestCreateRemoveJsonlFileOperator:
        def test_should_pass_source_url(self, dag, airflow_context, dag_run, task_instance):
            dag_run.conf = DEFAULT_CONF
            task_instance.xcom_pull.return_value = URL_1
            rendered_bash_command = _create_and_render_remove_jsonl_file_command(
                dag, airflow_context
            )
            opt = parse_command_arg(rendered_bash_command, {})
            assert getattr(opt, 'positional') == ['rm', URL_1]

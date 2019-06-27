from unittest.mock import patch, MagicMock

import pytest

import dags.sciencebeam_dag_utils as sciencebeam_dag_utils_module
from dags.sciencebeam_dag_utils import (
    create_watch_sensor,
    create_list_operator,
    create_trigger_next_task_dag_operator,
    get_app_config_value
)


@pytest.fixture(name='simple_trigger_dag_mock')
def _simple_trigger_dag_mock():
    with patch.object(sciencebeam_dag_utils_module, 'simple_trigger_dag') as mock:
        yield mock


class TestScienceBeamDagUtils:
    class TestCreateWatchSensor:
        def test_should_extract_bucket_and_prefix_from_url(self, dag):
            sensor = create_watch_sensor(dag, 'task1', 'gs://bucket/path/prefix')
            assert sensor.bucket == 'bucket'
            assert sensor.prefix == 'path/prefix'

        def test_should_throw_error_if_not_gs_scheme(self, dag):
            with pytest.raises(AssertionError):
                create_watch_sensor(dag, 'task1', 'other://bucket/path/prefix')

    class TestCreateListOperator:
        def test_should_extract_bucket_and_prefix_from_url(self, dag):
            operator = create_list_operator(dag, 'task1', 'gs://bucket/path/prefix')
            assert operator.bucket == 'bucket'
            assert operator.prefix == 'path/prefix'

        def test_should_throw_error_if_not_gs_scheme(self, dag):
            with pytest.raises(AssertionError):
                create_list_operator(dag, 'task1', 'other://bucket/path/prefix')

    class TestCreateTriggerNextTaskDagOperator:
        def test_should_not_trigger_without_tasks(
                self, dag, airflow_context, simple_trigger_dag_mock: MagicMock, dag_run):
            dag_run.conf = {'other': {}}
            operator = create_trigger_next_task_dag_operator(dag)
            operator.execute(airflow_context)
            simple_trigger_dag_mock.assert_not_called()

        def test_should_not_trigger_if_current_task_is_last(
                self, dag, airflow_context, simple_trigger_dag_mock: MagicMock, dag_run):
            dag_run.conf = {'tasks': ['previous', dag.dag_id]}
            operator = create_trigger_next_task_dag_operator(dag)
            operator.execute(airflow_context)
            simple_trigger_dag_mock.assert_not_called()

        def test_should_trigger_next_task(
                self, dag, airflow_context, simple_trigger_dag_mock: MagicMock, dag_run):
            dag_run.conf = {'tasks': [dag.dag_id, 'next']}
            operator = create_trigger_next_task_dag_operator(dag)
            operator.execute(airflow_context)
            simple_trigger_dag_mock.assert_called_with('next', dag_run.conf)

        def test_should_raise_exception_if_dag_id_not_found(
                self, dag, airflow_context, simple_trigger_dag_mock: MagicMock, dag_run):
            dag_run.conf = {'tasks': ['other']}
            operator = create_trigger_next_task_dag_operator(dag)
            with pytest.raises(ValueError):
                operator.execute(airflow_context)
            simple_trigger_dag_mock.assert_not_called()

    class TestGetAppConfig:
        @patch.dict('os.environ', {'KEY1': 'env value 1'})
        def test_should_return_config_value_if_present(self):
            assert get_app_config_value(
                'key1', default_value='default value 1', config={'key1': 'config value 1'}
            ) == 'config value 1'

        @patch.dict('os.environ', {'KEY1': 'env value 1'})
        def test_should_return_env_value_if_config_has_no_matching_key(self):
            assert get_app_config_value(
                'key1', default_value='default value 1', config={'other': 'config value 1'}
            ) == 'env value 1'

        @patch.dict('os.environ', {'KEY1': 'env value 1'})
        def test_should_return_env_value_if_config_is_none(self):
            assert get_app_config_value(
                'key1', default_value='default value 1', config=None
            ) == 'env value 1'

        @patch.dict('os.environ', {'OTHER': 'env value 1'})
        def test_should_return_default_value_otherwise(self):
            assert get_app_config_value(
                'key1', default_value='default value 1', config=None
            ) == 'default value 1'

        @patch.dict('os.environ', {'OTHER': 'env value 1'})
        def test_should_return_none_without_default_value(self):
            assert get_app_config_value('key1') is None

        @patch.dict('os.environ', {'OTHER': 'env value 1'})
        def test_should_raise_error_if_required(self):
            with pytest.raises(KeyError):
                get_app_config_value('key1', required=True)

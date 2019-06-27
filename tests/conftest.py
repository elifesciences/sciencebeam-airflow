import logging
from unittest.mock import MagicMock

import pytest

import airflow


@pytest.fixture(scope='session', autouse=True)
def setup_logging():
    logging.root.handlers = []
    logging.basicConfig(level='INFO')
    logging.getLogger('dags').setLevel('DEBUG')


@pytest.fixture(name='dag')
def _dag_mock():
    return airflow.models.DAG(
        dag_id='test',
        default_args={
            'start_date': airflow.utils.dates.days_ago(1)
        },
        schedule_interval=None
    )


@pytest.fixture(name='dag_run')
def _dag_run():
    dag_run = MagicMock(name='dag_run')
    dag_run.run_id = 'run_1'
    return dag_run


@pytest.fixture(name='task_instance')
def _task_instance():
    task_instance = MagicMock(name='task_instance')
    return task_instance


@pytest.fixture(name='airflow_context')
def _airflow_context(dag, dag_run, task_instance):
    return {
        'dag': dag,
        'dag_run': dag_run,
        'ti': task_instance
    }

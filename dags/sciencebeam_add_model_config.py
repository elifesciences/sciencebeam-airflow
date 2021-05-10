import json
import os
import logging

from airflow.operators.python import PythonOperator
from airflow.models import DAG, DagRun

from sciencebeam_airflow.dags.dag_ids import ScienceBeamDagIds

from sciencebeam_airflow.dags.utils import (
    get_default_args,
    get_config_data_path,
    create_validate_config_operation,
    create_trigger_next_task_dag_operator,
    set_file_content
)


LOGGER = logging.getLogger(__name__)


class ConfigProps:
    MODEL = 'model'


REQUIRED_PROPS = {
    ConfigProps.MODEL
}


DEFAULT_ARGS = get_default_args()


def get_models_path():
    return get_config_data_path('models')


def get_model_filename(model_config: dict) -> str:
    return '%s.json' % model_config['name']


def add_model_config(**kwargs):
    dag_run: DagRun = kwargs['dag_run']
    model_config: dict = dag_run.conf['model']
    model_json = json.dumps(model_config, indent=2).encode('utf-8')
    filename = get_model_filename(model_config)
    target_url = os.path.join(get_models_path(), filename)
    LOGGER.info('adding model config, url=%s, model_json=%s', target_url, model_json)
    set_file_content(target_url, model_json)


def add_model_config_operator(dag, task_id='add_model_config'):
    return PythonOperator(
        task_id=task_id,
        python_callable=add_model_config,
        dag=dag
    )


def create_dag():
    dag = DAG(
        dag_id=ScienceBeamDagIds.SCIENCEBEAM_ADD_MODEL_CONFIG,
        default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    _ = (
        create_validate_config_operation(dag=dag, required_props=REQUIRED_PROPS)
        >> add_model_config_operator(dag=dag)
        >> create_trigger_next_task_dag_operator(dag=dag)
    )

    return dag


MAIN_DAG = create_dag()

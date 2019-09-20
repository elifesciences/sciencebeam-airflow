import json
import os
import logging
import random
import string
from typing import List

from airflow.models import DAG

from airflow.operators.python_operator import PythonOperator

from sciencebeam_dag_ids import ScienceBeamDagIds

from sciencebeam_dag_ids import (
    DEFAULT_CONVERT_AND_EVALUATE_TASKS,
    DEFAULT_GROBID_TRAIN_TASKS,
    DEFAULT_AUTOCUT_TRAIN_TASKS
)

from sciencebeam_dag_conf import (
    get_output_data_path,
    get_file_list_path,
    get_eval_output_path
)

from sciencebeam_dag_utils import (
    get_default_args,
    get_config_data_path,
    parse_bool,
    create_watch_and_list_operator,
    file_exists,
    get_file_content,
    move_file,
    simple_trigger_dag,
    create_retrigger_operator
)


LOGGER = logging.getLogger(__name__)


DEFAULT_ARGS = get_default_args()


class InvalidConfigError(ValueError):
    pass


def _get_incoming_experiments_path():
    return get_config_data_path('experiments/incoming')


def _get_archive_experiments_path():
    return get_config_data_path('experiments/archive')


def _get_watch_interval():
    watch_interval = int(os.environ['SCIENCEBEAM_WATCH_INTERVAL'])
    if watch_interval <= 0:
        raise AssertionError('invalid watch interval: %d' % watch_interval)
    return watch_interval


def create_watch_and_list_experiments_op(dag, task_id_prefix='watch_experiments'):
    return create_watch_and_list_operator(
        task_id_prefix=task_id_prefix,
        dag=dag,
        url_prefix=_get_incoming_experiments_path(),
        poke_interval=_get_watch_interval()
    )


def log_files(**kwargs):
    LOGGER.info('files: %s', kwargs['ti'].xcom_pull(task_ids=None, key='return_value'))


def create_log_files_op(dag, task_id='log_files'):
    return PythonOperator(
        task_id=task_id,
        provide_context=True,
        python_callable=log_files,
        dag=dag
    )


def get_run_name(dataset: dict, model: dict) -> str:
    dataset_name = dataset['name']
    dataset_eval_name = dataset.get('eval_name')
    dataset_subset_name = dataset.get('subset_name')
    model_name = model['name']
    if dataset_eval_name:
        return f'{dataset_name}-{dataset_eval_name}_{model_name}'
    if dataset_subset_name:
        return f'{dataset_name}-{dataset_subset_name}_{model_name}'
    return f'{dataset_name}_{model_name}'


def _get_copied_experiment_data_props(experiment_data: dict) -> dict:
    return {
        key: value
        for key, value in experiment_data.items()
        if key.endswith('_image') or key in {'config'}
    }


def get_tasks_for_experiment_data(experiment_data: dict) -> List[str]:
    tasks: List[str] = experiment_data.get('tasks')
    if tasks:
        return tasks
    train_config: dict = experiment_data.get('train')
    if train_config and train_config.get('grobid'):
        return DEFAULT_GROBID_TRAIN_TASKS
    if train_config and train_config.get('autocut'):
        return DEFAULT_AUTOCUT_TRAIN_TASKS
    if train_config:
        raise InvalidConfigError('unrecognised train config: %s' % train_config)
    return DEFAULT_CONVERT_AND_EVALUATE_TASKS


def get_train_config_for_experiment_data(experiment_data: dict, namespace: str) -> List[str]:
    train_config: dict = experiment_data.get('train')
    if not train_config:
        return None
    autocut_config = train_config.get('autocut')
    if autocut_config and not autocut_config.get('input_source', {}).get('file_list'):
        train_limit = train_config.get('limit')
        model_name = autocut_config['input_model']['name']
        source_dataset = train_config['source_dataset']
        source_file_list = source_dataset['source_file_list']
        source_data_path = os.path.dirname(source_file_list)
        output_data_path = get_output_data_path(
            source_data_path=source_data_path, namespace=namespace, model_name=model_name
        )
        output_file_list = get_file_list_path(
            output_data_path, dataset=source_dataset, limit=train_limit
        )
        return {
            **train_config,
            'autocut': {
                **autocut_config,
                'input_source': {
                    **autocut_config.get('input_source', {}),
                    'file_list': output_file_list
                }
            }
        }
    return train_config


def get_namespace():
    return os.environ['SCIENCEBEAM_NAMESPACE']


def get_default_limit():
    return os.environ['SCIENCEBEAM_DEFAULT_LIMIT']


def generate_randstr(k):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=k))


def generate_release_name(prefix, namespace):
    return f'{prefix}-{generate_randstr(8)}--{namespace}'


def remove_none(dict_: dict) -> dict:
    return {key: value for key, value in dict_.items() if value is not None}


def get_conf_for_experiment_data(experiment_data):  # pylint: disable=too-many-locals
    namespace = get_namespace()
    default_limit = get_default_limit()
    model = experiment_data['model']
    dataset = experiment_data['dataset']
    sciencebeam_release_name = generate_release_name('sb', namespace=namespace)
    model_name = model['name']
    source_file_list = dataset['source_file_list']
    if not file_exists(source_file_list):
        raise AssertionError(f'file list not found: {source_file_list}')
    source_data_path = os.path.dirname(source_file_list)
    output_data_path = get_output_data_path(
        source_data_path=source_data_path, namespace=namespace, model_name=model_name
    )
    limit = experiment_data.get('limit', dataset.get('limit', default_limit))
    output_file_list = get_file_list_path(output_data_path, dataset=dataset, limit=limit)
    output_suffix = f'.xml.gz'
    eval_output_path = get_eval_output_path(output_data_path, dataset=dataset, limit=limit)
    resume = parse_bool(experiment_data.get('resume', True))
    return remove_none({
        **_get_copied_experiment_data_props(experiment_data),
        'tasks': get_tasks_for_experiment_data(experiment_data),
        'run_name': get_run_name(dataset=dataset, model=model),
        'experiment_timestamp': experiment_data['created'],
        'dataset_name': dataset['name'],
        'model_name': model['name'],
        'model': model,
        'sciencebeam_release_name': sciencebeam_release_name,
        'namespace': namespace,
        'dataset': dataset,
        'source_data_path': source_data_path,
        'source_file_list': os.path.basename(source_file_list),
        'output_data_path': output_data_path,
        'output_file_list': os.path.basename(output_file_list),
        'output_suffix': output_suffix,
        'eval_output_path': eval_output_path,
        'resume': resume,
        'limit': limit,
        'train': get_train_config_for_experiment_data(experiment_data, namespace=namespace)
    })


def for_each_file_in_file_list_trigger_dag_and_move_file(file_list):
    for file_url in file_list:
        LOGGER.info('file_url: %s', file_url)
        archive_file_url = os.path.join(
            _get_archive_experiments_path(),
            os.path.basename(file_url)
        )
        experiment_data = json.loads(get_file_content(file_url))
        LOGGER.info('experiment_data: %s', experiment_data)
        conf = get_conf_for_experiment_data(experiment_data)
        trigger_dag_id = conf['tasks'][0]
        LOGGER.info('trigger_dag_id: %s', trigger_dag_id)
        simple_trigger_dag(
            dag_id=trigger_dag_id,
            conf=conf
        )
        LOGGER.info('moving file to archive: %s', archive_file_url)
        move_file(file_url, archive_file_url)


def for_each_file_in_xcom_file_list_trigger_dag_and_move_file(**kwargs):
    file_list = kwargs['ti'].xcom_pull(task_ids=None, key='return_value')
    if not file_list:
        raise AssertionError('no file urls found')
    for_each_file_in_file_list_trigger_dag_and_move_file(file_list)


def create_for_each_file_in_file_list_trigger_dag_op(
        dag, task_id='for_each_file_in_file_list_trigger_dag_and_move_file'):
    return PythonOperator(
        task_id=task_id,
        provide_context=True,
        python_callable=for_each_file_in_xcom_file_list_trigger_dag_and_move_file,
        dag=dag
    )


def create_dag():
    dag = DAG(
        dag_id=ScienceBeamDagIds.SCIENCEBEAM_WATCH_EXPERIMENTS,
        default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    _ = (
        create_watch_and_list_experiments_op(dag=dag)
        >> create_log_files_op(dag=dag)
        >> create_for_each_file_in_file_list_trigger_dag_op(dag=dag)
        >> create_retrigger_operator(dag=dag)
    )

    return dag


MAIN_DAG = create_dag()

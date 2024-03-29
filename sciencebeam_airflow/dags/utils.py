import json
import logging
import os
from datetime import timedelta
from functools import partial
from pprint import pformat
from urllib.parse import urlparse
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Iterable, List

import airflow
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWtihPrefixExistenceSensor
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import DAG
from airflow.utils import timezone
from airflow.api.common.experimental.trigger_dag import trigger_dag


from sciencebeam_airflow.dags.dag_conf import ScienceBeamDagConf


LOGGER = logging.getLogger(__name__)


BOOL_MAP = {'true': True, 'false': False}


def parse_bool(value):
    return BOOL_MAP[str(value).lower()]


def get_default_args():
    return {
        'start_date': airflow.utils.dates.days_ago(1),
        'retries': 10,
        'retry_delay': timedelta(minutes=1),
        'retry_exponential_backoff': True
    }


def validate_config(required_props, is_config_valid: Callable[[dict], bool] = None, **kwargs):
    LOGGER.info('kwargs: %s', pformat(kwargs))
    conf = kwargs['dag_run'].conf
    LOGGER.info('dag_run.conf: %s', pformat(conf))
    missing_props = {
        key
        for key in required_props
        if not conf.get(key)
    }
    if missing_props:
        raise AssertionError('missing conf values: %s (got: %s)' % (missing_props, conf))
    if is_config_valid is not None and not is_config_valid(conf):
        raise AssertionError('invalid conf (is_config_valid)')
    LOGGER.info('all required conf values present')


def create_validate_config_operation(
        dag, required_props,
        is_config_valid: Callable[[dict], bool] = None,
        task_id='validate_config'):
    return PythonOperator(
        task_id=task_id,
        python_callable=partial(
            validate_config, required_props=required_props, is_config_valid=is_config_valid
        ),
        retries=0,
        dag=dag
    )


def parse_gs_url(url):
    parsed_url = urlparse(url)
    if parsed_url.scheme != 'gs':
        raise AssertionError('expected gs:// url, but got: %s' % url)
    if not parsed_url.hostname:
        raise AssertionError('url is missing bucket / hostname: %s' % url)
    return {
        'bucket': parsed_url.hostname,
        'object': parsed_url.path.lstrip('/')
    }


def create_watch_sensor(dag, task_id, url_prefix, **kwargs):
    parsed_url = parse_gs_url(url_prefix)
    return GCSObjectsWtihPrefixExistenceSensor(
        task_id=task_id,
        bucket=parsed_url['bucket'],
        prefix=parsed_url['object'],
        dag=dag,
        **kwargs
    )


def _to_absolute_urls(bucket: str, path_iterable: Iterable[str]):
    return [
        f'gs://{bucket}/{path}'
        for path in path_iterable
    ]


class AbsoluteUrlGoogleCloudStorageListOperator(GCSListObjectsOperator):
    def execute(self, context):
        return _to_absolute_urls(self.bucket, super().execute(context))


def create_list_operator(dag, task_id, url_prefix):
    parsed_url = parse_gs_url(url_prefix)
    return AbsoluteUrlGoogleCloudStorageListOperator(
        task_id=task_id,
        bucket=parsed_url['bucket'],
        prefix=parsed_url['object'],
        dag=dag
    )


def get_gs_hook(google_cloud_storage_conn_id='google_cloud_default', delegate_to=None):
    return GCSHook(
        google_cloud_storage_conn_id=google_cloud_storage_conn_id,
        delegate_to=delegate_to
    )


def list_files(url_prefix):
    parsed_url = parse_gs_url(url_prefix)
    LOGGER.info('listing files in %s (%s)', url_prefix, parsed_url)
    return _to_absolute_urls(
        parsed_url['bucket'],
        get_gs_hook().list(bucket_name=parsed_url['bucket'], prefix=parsed_url['object'])
    )


def require_list_files(url_prefix):
    file_list = list_files(url_prefix)
    if not file_list:
        raise AssertionError('no files found for %s' % url_prefix)
    return file_list


def create_watch_and_list_operator(dag, task_id_prefix, url_prefix, **kwargs):
    return (
        create_watch_sensor(
            dag=dag,
            task_id=f'{task_id_prefix}_watch',
            url_prefix=url_prefix,
            **kwargs
        ) >> create_list_operator(
            dag=dag,
            task_id=f'{task_id_prefix}_list',
            url_prefix=url_prefix
        )
    )


def file_exists(url, **kwargs):
    parsed_url = parse_gs_url(url)
    return get_gs_hook(**kwargs).exists(
        bucket_name=parsed_url['bucket'],
        object_name=parsed_url['object']
    )


def download_file(url, filename, **kwargs):
    LOGGER.info('downloading: %s', url)
    parsed_url = parse_gs_url(url)
    return get_gs_hook(**kwargs).download(
        bucket_name=parsed_url['bucket'],
        object_name=parsed_url['object'],
        filename=filename
    )


def upload_file(filename, url, **kwargs):
    LOGGER.info('uploading to: %s', url)
    parsed_url = parse_gs_url(url)
    return get_gs_hook(**kwargs).upload(
        bucket_name=parsed_url['bucket'],
        object_name=parsed_url['object'],
        filename=filename
    )


def set_file_content(url, data, mode='wb', **kwargs):
    with TemporaryDirectory() as temp_directory:
        temp_filename = os.path.join(temp_directory, os.path.basename(url))
        with open(temp_filename, mode) as temp_fp:
            temp_fp.write(data)
        upload_file(temp_filename, url, **kwargs)


def download_file_list(file_list, target_directory, **kwargs):
    for file_url in file_list:
        local_file_path = Path(target_directory).joinpath(Path(file_url).name)
        download_file(file_url, local_file_path, **kwargs)
        yield local_file_path


def get_file_content(url, **kwargs):
    return download_file(url, filename=None, **kwargs)


def _copy_or_move_file(
        source_url, target_url, is_move_file=False,
        google_cloud_storage_conn_id='google_cloud_default', delegate_to=None):
    parsed_source_url = parse_gs_url(source_url)
    parsed_target_url = parse_gs_url(target_url)
    hook = GCSHook(
        google_cloud_storage_conn_id=google_cloud_storage_conn_id,
        delegate_to=delegate_to
    )
    hook.rewrite(
        parsed_source_url['bucket'],
        parsed_source_url['object'],
        parsed_target_url['bucket'],
        parsed_target_url['object']
    )
    if is_move_file:
        hook.delete(
            parsed_source_url['bucket'],
            parsed_source_url['object']
        )


def copy_file(*args, **kwargs):
    _copy_or_move_file(*args, **kwargs, is_move_file=False)


def move_file(*args, **kwargs):
    _copy_or_move_file(*args, **kwargs, is_move_file=True)


def truncate_run_id(run_id: str) -> str:
    # maximum is 250
    return run_id[:250]


def _get_full_run_id(conf: dict, default_run_id: str) -> str:
    run_name = conf.get('run_name')
    if run_name:
        return truncate_run_id(f'{default_run_id}_{run_name}')
    return truncate_run_id(default_run_id)


def get_combined_run_name(base_run_name: str, sub_run_name: str) -> str:
    return f'{base_run_name}_{sub_run_name}'


def simple_trigger_dag(dag_id, conf, suffix=''):
    run_id = _get_full_run_id(
        conf=conf,
        default_run_id=f'trig__{timezone.utcnow().isoformat()}{suffix}'
    )
    trigger_dag(
        dag_id=dag_id,
        run_id=run_id,
        conf=json.dumps(conf),
        execution_date=None,
        replace_microseconds=False
    )


def create_retrigger_operator(dag, task_id=None):
    if not task_id:
        task_id = f'retrigger_{dag.dag_id}'
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag.dag_id,
        dag=dag
    )


def trigger_using_transform_conf(
        trigger_dag_id: str,
        transform_conf: Callable[[dict], dict] = None,
        **kwargs):
    dag_run = kwargs['dag_run']
    conf = transform_conf(dag_run.conf)
    LOGGER.info('triggering %s with: %s', trigger_dag_id, conf)
    simple_trigger_dag(dag_id=trigger_dag_id, conf=conf)


def create_trigger_operator(
        dag: DAG, trigger_dag_id: str, task_id: str = None,
        transform_conf: Callable[[dict], dict] = None):
    LOGGER.info('trigger_dag_id: %s', trigger_dag_id)
    if not task_id:
        task_id = f'trigger_{trigger_dag_id}'
    return PythonOperator(
        dag=dag,
        task_id=task_id,
        python_callable=partial(
            trigger_using_transform_conf,
            trigger_dag_id=trigger_dag_id,
            transform_conf=transform_conf
        )
    )


def _trigger_next_task_fn(**kwargs):
    dag: DAG = kwargs['dag']
    dag_id = dag.dag_id
    LOGGER.info('current dag id: %s', dag_id)
    conf: dict = kwargs['dag_run'].conf
    tasks: list = conf.get('tasks')
    LOGGER.info('tasks: %s', tasks)
    if not tasks:
        LOGGER.info('no tasks configured, skipping')
        return
    try:
        task_index = tasks.index(dag_id)
    except ValueError as exc:
        raise ValueError(
            'current dag not found in task list, "%s" not in  %s' % (dag_id, tasks)
        ) from exc
    LOGGER.info('current dag task index: %d', task_index)
    if task_index + 1 == len(tasks):
        LOGGER.info('last tasks, skipping')
        return
    next_task_id = tasks[task_index + 1]
    LOGGER.info('next_task_id: %s', next_task_id)
    simple_trigger_dag(next_task_id, conf)


def create_trigger_next_task_dag_operator(dag: DAG, task_id: str = 'trigger_next_task_dag'):
    return PythonOperator(
        dag=dag,
        task_id=task_id,
        python_callable=_trigger_next_task_fn
    )


def get_filtered_tasks(conf: dict, tasks: List[str]) -> List[str]:
    enabled_tasks = set(conf.get(ScienceBeamDagConf.TASKS) or tasks)
    filtered_tasks = [
        task
        for task in tasks
        if task in enabled_tasks
    ]
    assert tasks[0] in set(filtered_tasks)
    return filtered_tasks


def get_gcp_project_id():
    return os.environ['GOOGLE_CLOUD_PROJECT']


def get_config_data_path(relative_path):
    config_data_path = os.environ.get('SCIENCEBEAM_CONFIG_DATA_PATH')
    if not config_data_path:
        raise AssertionError('SCIENCEBEAM_CONFIG_DATA_PATH missing')
    return os.path.join(config_data_path, relative_path)


def get_app_config_value(
        key: str, default_value: str = None, required: bool = False,
        config: dict = None):
    value = (config or {}).get(key) or os.environ.get(key.upper()) or default_value
    if not value and required:
        raise KeyError('%s required' % key)
    LOGGER.debug('get_app_config_value: key=%s, value=%s', key, value)
    return value


DEFAULT_SCIENCEBEAM_IMAGE = (
    'elifesciences/sciencebeam:0.0.8'
)


def get_sciencebeam_image(config: dict = None):
    return get_app_config_value(
        'sciencebeam_image',
        config=config,
        default_value=DEFAULT_SCIENCEBEAM_IMAGE,
        required=True
    )


DEFAULT_SCIENCEBEAM_JUDGE_IMAGE = (
    'elifesciences/sciencebeam-judge:0.0.15'
)


def get_sciencebeam_judge_image(config: dict = None):
    return get_app_config_value(
        'sciencebeam_judge_image',
        config=config,
        default_value=DEFAULT_SCIENCEBEAM_JUDGE_IMAGE,
        required=True
    )


DEFAULT_SCIENCEBEAM_GYM_IMAGE = (
    'elifesciences/sciencebeam-gym:0.1.0'
)


def get_sciencebeam_gym_image(config: dict = None):
    return get_app_config_value(
        'sciencebeam_gym_image',
        config=config,
        default_value=DEFAULT_SCIENCEBEAM_GYM_IMAGE,
        required=True
    )

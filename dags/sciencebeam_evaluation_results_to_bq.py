import logging
import json
from datetime import datetime
from tempfile import TemporaryDirectory
from pathlib import Path
from csv import DictReader
from typing import Iterable

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG, DagRun

from sciencebeam_dag_ids import ScienceBeamDagIds

from sciencebeam_dag_utils import (
    get_default_args,
    create_validate_config_operation,
    create_trigger_next_task_dag_operator,
    require_list_files,
    download_file_list,
    upload_file
)


LOGGER = logging.getLogger(__name__)


class ConfigProps:
    DATASET_NAME = 'dataset_name'
    MODEL_NAME = 'model_name'
    NAMESPACE = 'namespace'
    EVAL_OUTPUT_PATH = 'eval_output_path'


REQUIRED_PROPS = {
    ConfigProps.DATASET_NAME,
    ConfigProps.MODEL_NAME,
    ConfigProps.NAMESPACE,
    ConfigProps.EVAL_OUTPUT_PATH
}


DEFAULT_ARGS = get_default_args()


UPLOAD_TO_BQ_TEMPLATE = (
    '''
    bq load \
        --source_format=NEWLINE_DELIMITED_JSON \
        "{{ dag_run.conf.namespace }}.document_field_evaluation" \
        "{{ ti.xcom_pull() }}"
    '''
)


GSUTIL_RM_TEMPLATE = (
    '''
    gsutil rm "{{ ti.xcom_pull() }}"
    '''
)


def _write_jsonl(jsonl_filename: str, iterable: Iterable[dict]):
    with Path(jsonl_filename).open(mode='w') as jsonl_fp:
        for row in iterable:
            jsonl_fp.write(json.dumps(row))
            jsonl_fp.write('\n')


def _iter_read_csv_as_dict(csv_filename_list: Iterable[str]) -> Iterable[dict]:
    for csv_filename in csv_filename_list:
        LOGGER.info('processing: %s', csv_filename)
        with Path(csv_filename).open() as csv_fp:
            for row in DictReader(csv_fp):
                yield row


def _add_common_props(iterable: Iterable[dict], common_props: dict) -> Iterable[dict]:
    return ({**common_props, **row} for row in iterable)


def convert_evaluation_results_to_jsonl_file(
        eval_output_path: str,
        dataset_name: str,
        model_name: str) -> str:
    LOGGER.info('eval_output_path: %s', eval_output_path)
    LOGGER.info('dataset_name: %s', dataset_name)
    LOGGER.info('model_name: %s', model_name)
    common_props = {
        'created_timestamp': datetime.utcnow().isoformat(),
        'dataset_name': dataset_name,
        'model_name': model_name
    }
    file_list = require_list_files(f'{eval_output_path}/results-')
    LOGGER.info('file_list: %s', file_list)
    jsonl_target_url = f'{eval_output_path}/results.jsonl'
    with TemporaryDirectory(prefix='airflowtemp_') as temp_dir:
        temp_path = Path(temp_dir)
        downloaded_file_list = download_file_list(file_list, temp_dir)
        jsonl_file_path = temp_path.joinpath('results.jsonl')
        _write_jsonl(
            jsonl_file_path,
            _add_common_props(
                _iter_read_csv_as_dict(downloaded_file_list),
                common_props
            )
        )
        upload_file(jsonl_file_path, jsonl_target_url)
    return jsonl_target_url


def evaluation_results_to_jsonl(dag_run: DagRun, **_):
    conf = dag_run.conf
    LOGGER.info('conf: %s', conf)
    return convert_evaluation_results_to_jsonl_file(
        eval_output_path=conf[ConfigProps.EVAL_OUTPUT_PATH],
        dataset_name=conf[ConfigProps.DATASET_NAME],
        model_name=conf[ConfigProps.MODEL_NAME]
    )


def create_evaluation_results_to_jsonl_op(dag, task_id='evaluation_results_to_jsonl'):
    return PythonOperator(
        task_id=task_id,
        provide_context=True,
        python_callable=evaluation_results_to_jsonl,
        dag=dag
    )


def create_upload_to_bq_op(dag, task_id='upload_to_bq'):
    return BashOperator(
        task_id=task_id,
        bash_command=UPLOAD_TO_BQ_TEMPLATE,
        dag=dag
    )


def create_remove_jsonl_file_op(dag, task_id='remove_jsonl_file'):
    return BashOperator(
        task_id=task_id,
        bash_command=GSUTIL_RM_TEMPLATE,
        dag=dag
    )


def create_dag():
    dag = DAG(
        dag_id=ScienceBeamDagIds.SCIENCEBEAM_EVALUATION_RESULTS_TO_BQ,
        default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    _ = (
        create_validate_config_operation(dag=dag, required_props=REQUIRED_PROPS)
        >> create_evaluation_results_to_jsonl_op(dag=dag)
        >> create_upload_to_bq_op(dag=dag)
        >> create_remove_jsonl_file_op(dag=dag)
        >> create_trigger_next_task_dag_operator(dag=dag)
    )

    return dag


MAIN_DAG = create_dag()

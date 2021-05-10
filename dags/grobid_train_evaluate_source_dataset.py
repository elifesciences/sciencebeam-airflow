import os

from airflow.models import DAG

from sciencebeam_airflow.dags.dag_ids import (
    ScienceBeamDagIds,
    DEFAULT_CONVERT_AND_EVALUATE_TASKS
)

from sciencebeam_airflow.dags.dag_conf import (
    ScienceBeamDagConf,
    ScienceBeamTrainDagConf,
    ScienceBeamDatasetDagConf,
    get_output_data_path,
    get_file_list_path,
    get_eval_output_path
)

from sciencebeam_airflow.dags.utils import (
    get_default_args,
    create_trigger_operator,
    create_trigger_next_task_dag_operator,
    get_filtered_tasks,
    get_combined_run_name
)

from grobid_train_utils import (
    create_grobid_train_validate_config_operation
)


DEFAULT_ARGS = get_default_args()

GROBID_TRAIN_DATASET_SUFFIX = '-grobid-train'
GROBID_TRAIN_MODEL_NAME = 'grobid-train'

SUB_RUN_NAME = 'source-dataset'
RELEASE_NAME_SUFFIX = '-source-ds'


def _prepare_convert_and_evaluate_source_dataset_conf(conf: dict):
    train_conf: dict = conf[ScienceBeamDagConf.TRAIN]
    source_dataset: dict = train_conf[ScienceBeamTrainDagConf.SOURCE_DATASET]
    source_dataset_name = source_dataset[ScienceBeamDatasetDagConf.NAME]
    source_file_list = source_dataset[ScienceBeamDatasetDagConf.SOURCE_FILE_LIST]
    train_limit = train_conf.get(ScienceBeamTrainDagConf.LIMIT)
    output_data_path = get_output_data_path(
        source_data_path=os.path.dirname(source_file_list),
        namespace=conf[ScienceBeamDagConf.NAMESPACE],
        model_name=conf[ScienceBeamDagConf.MODEL_NAME]
    )
    evaluate_conf = {
        **conf,
        ScienceBeamDagConf.SOURCE_DATA_PATH: os.path.dirname(source_file_list),
        ScienceBeamDagConf.SOURCE_FILE_LIST: os.path.basename(source_file_list),
        ScienceBeamDagConf.OUTPUT_DATA_PATH: output_data_path,
        ScienceBeamDagConf.OUTPUT_FILE_LIST: os.path.basename(get_file_list_path(
            output_data_path=output_data_path,
            dataset=source_dataset,
            limit=train_limit
        )),
        ScienceBeamDagConf.EVAL_OUTPUT_PATH: get_eval_output_path(
            output_data_path=output_data_path,
            dataset=source_dataset,
            limit=train_limit
        ),
        ScienceBeamDagConf.TASKS: get_filtered_tasks(conf, DEFAULT_CONVERT_AND_EVALUATE_TASKS),
        ScienceBeamDagConf.DATASET_NAME: source_dataset_name,
        ScienceBeamDagConf.RUN_NAME: get_combined_run_name(
            conf.get(ScienceBeamDagConf.RUN_NAME, ''), SUB_RUN_NAME
        ),
        ScienceBeamDagConf.SCIENCEBEAM_RELEASE_NAME: (
            conf.get(ScienceBeamDagConf.SCIENCEBEAM_RELEASE_NAME, '') + RELEASE_NAME_SUFFIX
        )
    }
    if train_limit:
        evaluate_conf[ScienceBeamDagConf.LIMIT] = train_limit
    return evaluate_conf


def create_trigger_sciencebeam_convert_and_evaluate_source_dataset_operator(
        dag, task_id='trigger_sciencebeam_convert_and_evaluate_source_dataset'):
    return create_trigger_operator(
        dag=dag,
        task_id=task_id,
        trigger_dag_id=DEFAULT_CONVERT_AND_EVALUATE_TASKS[0],
        transform_conf=_prepare_convert_and_evaluate_source_dataset_conf
    )


def create_dag():
    dag = DAG(
        dag_id=ScienceBeamDagIds.GROBID_TRAIN_EVALUATE_SOURCE_DATASET,
        default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    _ = (
        create_grobid_train_validate_config_operation(dag=dag)
        >> create_trigger_sciencebeam_convert_and_evaluate_source_dataset_operator(dag=dag)
        >> create_trigger_next_task_dag_operator(dag=dag)
    )

    return dag


MAIN_DAG = create_dag()

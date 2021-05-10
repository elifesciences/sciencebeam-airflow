from airflow.models import DAG

from sciencebeam_airflow.dags.dag_ids import (
    ScienceBeamDagIds,
    DEFAULT_EVALUATE_TASKS
)

from sciencebeam_airflow.dags.dag_conf import (
    ScienceBeamDagConf,
    ScienceBeamTrainDagConf,
    ScienceBeamDatasetDagConf,
    ScienceBeamTrainGrobidDagConf
)

from sciencebeam_dag_utils import (
    get_default_args,
    create_trigger_operator,
    create_trigger_next_task_dag_operator,
    get_filtered_tasks,
    get_combined_run_name
)

from grobid_train_utils import (
    create_grobid_train_validate_config_operation,
    DEFAULT_GROBID_TRAIN_FIELDS
)


DEFAULT_ARGS = get_default_args()

GROBID_TRAIN_DATASET_SUFFIX = '-grobid-train'
GROBID_TRAIN_MODEL_NAME = 'grobid-train'

SUB_RUN_NAME = 'grobid-train-tei'
RELEASE_NAME_SUFFIX = '-train-tei'


def _prepare_evaluate_grobid_train_tei_conf(conf: dict):
    train_conf: dict = conf[ScienceBeamDagConf.TRAIN]
    train_grobid_conf: dict = train_conf[ScienceBeamTrainDagConf.GROBID]
    source_dataset: dict = train_conf[ScienceBeamTrainDagConf.SOURCE_DATASET]
    source_dataset_name = source_dataset[ScienceBeamDatasetDagConf.NAME]
    grobid_dataset_path = train_grobid_conf[ScienceBeamTrainGrobidDagConf.DATASET]
    train_limit = train_conf.get(ScienceBeamTrainDagConf.LIMIT)
    train_fields = train_conf.get(ScienceBeamTrainDagConf.FIELDS) or DEFAULT_GROBID_TRAIN_FIELDS
    evaluate_conf = {
        **conf,
        ScienceBeamDagConf.SOURCE_DATA_PATH: grobid_dataset_path + '/xml',
        ScienceBeamDagConf.SOURCE_FILE_LIST: 'file-list.lst',
        ScienceBeamDagConf.OUTPUT_DATA_PATH: grobid_dataset_path + '/header/corpus/tei',
        ScienceBeamDagConf.OUTPUT_FILE_LIST: 'file-list.lst',
        ScienceBeamDagConf.EVAL_OUTPUT_PATH: grobid_dataset_path + '/evaluation-results',
        ScienceBeamDagConf.FIELDS: train_fields,
        ScienceBeamDagConf.TASKS: get_filtered_tasks(conf, DEFAULT_EVALUATE_TASKS),
        ScienceBeamDagConf.MODEL_NAME: GROBID_TRAIN_MODEL_NAME,
        ScienceBeamDagConf.DATASET_NAME: f'{source_dataset_name}{GROBID_TRAIN_DATASET_SUFFIX}',
        ScienceBeamDagConf.RUN_NAME: get_combined_run_name(
            conf.get(ScienceBeamDagConf.RUN_NAME, ''), SUB_RUN_NAME
        ),
        ScienceBeamDagConf.SCIENCEBEAM_RELEASE_NAME: (
            conf.get(ScienceBeamDagConf.SCIENCEBEAM_RELEASE_NAME, '') + RELEASE_NAME_SUFFIX
        )
    }
    if ScienceBeamDagConf.MODEL in evaluate_conf:
        del evaluate_conf[ScienceBeamDagConf.MODEL]
    if train_limit:
        evaluate_conf[ScienceBeamDagConf.LIMIT] = train_limit
    return evaluate_conf


def create_trigger_sciencebeam_evaluate_grobid_train_tei_operator(
        dag, task_id='trigger_sciencebeam_evaluate_grobid_train_tei'):
    return create_trigger_operator(
        dag=dag,
        task_id=task_id,
        trigger_dag_id=DEFAULT_EVALUATE_TASKS[0],
        transform_conf=_prepare_evaluate_grobid_train_tei_conf
    )


def create_dag():
    dag = DAG(
        dag_id=ScienceBeamDagIds.GROBID_TRAIN_EVALUATE_GROBID_TRAIN_TEI,
        default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    _ = (
        create_grobid_train_validate_config_operation(dag=dag)
        >> create_trigger_sciencebeam_evaluate_grobid_train_tei_operator(dag=dag)
        >> create_trigger_next_task_dag_operator(dag=dag)
    )

    return dag


MAIN_DAG = create_dag()

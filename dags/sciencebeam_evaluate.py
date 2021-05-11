import os
from typing import Optional

from airflow.models import DAG

from sciencebeam_airflow.utils.config import get_nested_prop
from sciencebeam_airflow.utils.airflow import add_dag_macros
from sciencebeam_airflow.utils.container_operators import ContainerRunOperator

from sciencebeam_airflow.dags.dag_ids import ScienceBeamDagIds

from sciencebeam_airflow.dags.utils import (
    get_default_args,
    create_validate_config_operation,
    create_trigger_next_task_dag_operator,
    get_sciencebeam_judge_image
)


class ConfigProps:
    SCIENCEBEAM_JUDGE_IMAGE = 'sciencebeam_judge_image'
    SCIENCEBEAM_RELEASE_NAME = 'sciencebeam_release_name'
    NAMESPACE = 'namespace'
    SOURCE_DATA_PATH = 'source_data_path'
    SOURCE_FILE_LIST = 'source_file_list'
    OUTPUT_DATA_PATH = 'output_data_path'
    OUTPUT_FILE_LIST = 'output_file_list'
    EVAL_OUTPUT_PATH = 'eval_output_path'
    LIMIT = 'limit'


REQUIRED_PROPS = {
    ConfigProps.SCIENCEBEAM_RELEASE_NAME,
    ConfigProps.NAMESPACE,
    ConfigProps.SOURCE_DATA_PATH,
    ConfigProps.SOURCE_FILE_LIST,
    ConfigProps.OUTPUT_DATA_PATH,
    ConfigProps.OUTPUT_FILE_LIST,
    ConfigProps.EVAL_OUTPUT_PATH,
    ConfigProps.LIMIT
}


DEFAULT_ARGS = get_default_args()


DEFAULT_JUDGE_CONTAINER_REQUESTS = 'cpu=1500m,memory=4096Mi'


SCIENCEBEAM_EVALUATE_TEMPLATE = (
    '''
    python -m sciencebeam_judge.evaluation_pipeline \
        --target-file-list \
        "{{ get_target_file_list(dag_run.conf) }}" \
        --target-file-column=xml_url \
        --prediction-file-list \
        "{{ dag_run.conf.output_data_path }}/{{ dag_run.conf.output_file_list }}" \
        --output-path="{{ dag_run.conf.eval_output_path }}" \
        {% if dag_run.conf.get('config', {}).get('evaluate', {}).get('fields') %} \
            --fields="{{ dag_run.conf.config.evaluate.fields }}" \
        {% endif %} \
        {% if dag_run.conf.get('config', {}).get('evaluate', {}).get('measures') %} \
            --measures="{{ dag_run.conf.config.evaluate.measures }}" \
        {% endif %} \
        {% if dag_run.conf.get('config', {}).get('evaluate', {}).get('scoring_type_overrides') %} \
            --scoring-type-overrides="{{ dag_run.conf.config.evaluate.scoring_type_overrides }}" \
        {% endif %} \
        --num_workers=10 \
        --skip-errors \
        --limit="{{ dag_run.conf.limit }}"
    '''
)


class ScienceBeamEvaluateMacros:
    def get_sciencebeam_judge_image(self, conf: dict) -> str:
        return get_sciencebeam_judge_image(conf)

    def get_sciencebeam_judge_container_kwargs(self, conf: dict) -> dict:
        return get_nested_prop(
            conf,
            ['config', 'evaluate', 'container'],
            {}
        )

    def get_dataset(self, conf: dict) -> dict:
        return conf.get('dataset')

    def get_target_file_list(self, conf: dict) -> dict:
        dataset = self.get_dataset(conf)
        if dataset:
            target_file_list = dataset.get('target_file_list')
            if target_file_list:
                return target_file_list
        return os.path.join(conf['source_data_path'], conf['source_file_list'])

    def is_config_valid(self, conf: dict) -> bool:
        return (
            self.get_target_file_list(conf)
            and True
        )


def add_sciencebeam_evaluate_dag_macros(
    dag: DAG,
    macros: Optional[ScienceBeamEvaluateMacros] = None
) -> ScienceBeamEvaluateMacros:
    if macros is None:
        macros = ScienceBeamEvaluateMacros()
    add_dag_macros(dag, macros)
    return macros


def create_sciencebeam_evaluate_op(
        dag, macros: Optional[ScienceBeamEvaluateMacros] = None,
        task_id: str = 'sciencebeam_evaluate'):
    _macros = add_sciencebeam_evaluate_dag_macros(dag, macros)
    return ContainerRunOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        image='{{ get_sciencebeam_judge_image(dag_run.conf) }}',
        name='{{ generate_run_name(dag_run.conf.sciencebeam_release_name, "judge") }}',
        preemptible=True,
        requests=DEFAULT_JUDGE_CONTAINER_REQUESTS,
        container_kwargs_fn=_macros.get_sciencebeam_judge_container_kwargs,
        command=SCIENCEBEAM_EVALUATE_TEMPLATE
    )


def create_dag(macros: ScienceBeamEvaluateMacros = None):
    if macros is None:
        macros = ScienceBeamEvaluateMacros()
    dag = DAG(
        dag_id=ScienceBeamDagIds.SCIENCEBEAM_EVALUATE,
        default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    _ = (
        create_validate_config_operation(
            dag=dag, required_props=REQUIRED_PROPS,
            is_config_valid=macros.is_config_valid
        )
        >> create_sciencebeam_evaluate_op(dag=dag)
        >> create_trigger_next_task_dag_operator(dag=dag)
    )

    return dag


MAIN_DAG = create_dag()

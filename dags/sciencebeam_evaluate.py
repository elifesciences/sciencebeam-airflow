from airflow.models import DAG

from sciencebeam_dag_ids import ScienceBeamDagIds

from sciencebeam_dag_utils import (
    get_default_args,
    create_validate_config_operation,
    create_trigger_next_task_dag_operator,
    add_dag_macro,
    get_sciencebeam_judge_image
)

from container_operators import ContainerRunOperator


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


SCIENCEBEAM_EVALUATE_TEMPLATE = (
    '''
    python -m sciencebeam_judge.evaluation_pipeline \
        --target-file-list \
        "{{ dag_run.conf.source_data_path }}/{{ dag_run.conf.source_file_list }}" \
        --target-file-column=xml_url \
        --prediction-file-list \
        "{{ dag_run.conf.output_data_path }}/{{ dag_run.conf.output_file_list }}" \
        --output-path "{{ dag_run.conf.eval_output_path }}" \
        {% if dag_run.conf.fields %} \
            --fields "{{ dag_run.conf.fields }}" \
        {% endif %} \
        --num_workers=10 \
        --skip-errors \
        --limit "{{ dag_run.conf.limit }}"
    '''
)


def create_sciencebeam_evaluate_op(dag, task_id='sciencebeam_evaluate'):
    add_dag_macro(dag, 'get_sciencebeam_judge_image', get_sciencebeam_judge_image)
    return ContainerRunOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        image='{{ get_sciencebeam_judge_image(dag_run.conf) }}',
        name='{{ generate_run_name(dag_run.conf.sciencebeam_release_name, "judge") }}',
        preemptible=True,
        requests='cpu=500m,memory=800Mi',
        command=SCIENCEBEAM_EVALUATE_TEMPLATE
    )


def create_dag():
    dag = DAG(
        dag_id=ScienceBeamDagIds.SCIENCEBEAM_EVALUATE,
        default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    _ = (
        create_validate_config_operation(dag=dag, required_props=REQUIRED_PROPS)
        >> create_sciencebeam_evaluate_op(dag=dag)
        >> create_trigger_next_task_dag_operator(dag=dag)
    )

    return dag


MAIN_DAG = create_dag()

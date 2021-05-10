from airflow.models import DAG

from sciencebeam_airflow.utils.airflow import add_dag_macro
from sciencebeam_airflow.utils.container_operators import ContainerRunOperator

from sciencebeam_dag_ids import ScienceBeamDagIds

from sciencebeam_dag_utils import (
    get_default_args,
    create_validate_config_operation,
    create_trigger_next_task_dag_operator,
    get_sciencebeam_gym_image
)


class ConfigProps:
    SCIENCEBEAM_RELEASE_NAME = 'sciencebeam_release_name'
    NAMESPACE = 'namespace'
    TRAIN = 'train'


REQUIRED_PROPS = {
    ConfigProps.SCIENCEBEAM_RELEASE_NAME,
    ConfigProps.NAMESPACE,
    ConfigProps.TRAIN
}


DEFAULT_ARGS = get_default_args()


TRAIN_AUTOCUT_MODEL_TEMPLATE = (
    '''
    python -m sciencebeam_gym.models.text.crf.autocut_training_pipeline \
        --input-file-list \
            "{{ dag_run.conf.train.autocut.input_source.file_list }}" \
        --input-xpath \
            "{{ dag_run.conf.train.autocut.input_source.xpath }}" \
        --target-file-list \
            "{{ dag_run.conf.train.source_dataset.source_file_list }}" \
        --target-file-column=xml_url \
        --target-xpath \
            "front/article-meta/title-group/article-title" \
        '--namespaces={"tei": "http://www.tei-c.org/ns/1.0"}' \
        --output-path="{{ dag_run.conf.train.autocut.model }}" \
        {% if dag_run.conf.train.limit | default(false) %} \
            --limit "{{ dag_run.conf.train.limit }}" \
        {% endif %} \
    '''
)


def create_train_autocut_model_operator(dag, task_id='train_autocut_model'):
    add_dag_macro(dag, 'get_sciencebeam_gym_image', get_sciencebeam_gym_image)
    return ContainerRunOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        image='{{ get_sciencebeam_gym_image(dag_run.conf) }}',
        name=(
            '{{ \
            generate_run_name(dag_run.conf.sciencebeam_release_name, "train-autocut-model") \
            }}'
        ),
        preemptible=True,
        requests='cpu=900m,memory=1Gi',
        command=TRAIN_AUTOCUT_MODEL_TEMPLATE,
    )


def create_dag():
    dag = DAG(
        dag_id=ScienceBeamDagIds.SCIENCEBEAM_AUTOCUT_TRAIN_MODEL,
        default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    _ = (
        create_validate_config_operation(dag=dag, required_props=REQUIRED_PROPS)
        >> create_train_autocut_model_operator(dag=dag)
        >> create_trigger_next_task_dag_operator(dag=dag)
    )

    return dag


MAIN_DAG = create_dag()

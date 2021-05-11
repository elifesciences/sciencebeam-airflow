from airflow.models import DAG
from airflow.operators.bash import BashOperator

from sciencebeam_airflow.utils.airflow import add_dag_macro

from sciencebeam_airflow.dags.dag_ids import ScienceBeamDagIds

from sciencebeam_airflow.dags.utils import (
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


BUILD_AUTOCUT_IMAGE_TEMPLATE = (
    '''
    AUTOCUT_MODEL_URL="{{ dag_run.conf.train.autocut.model }}" \
    SOURCE_AUTOCUT_IMAGE={{ get_sciencebeam_gym_image(dag_run.conf) }} \
    OUTPUT_AUTOCUT_IMAGE="{{ dag_run.conf.train.autocut.output_image }}" \
    GCP_PROJECT=elife-ml \
    $DOCKER_SCRIPTS_DIR/autocut-with-trained-model/build-docker-image.sh
    '''
)


def create_train_autocut_model_operator(dag, task_id='build_autocut_image'):
    add_dag_macro(dag, 'get_sciencebeam_gym_image', get_sciencebeam_gym_image)
    return BashOperator(
        dag=dag,
        task_id=task_id,
        bash_command=BUILD_AUTOCUT_IMAGE_TEMPLATE,
    )


def create_dag():
    dag = DAG(
        dag_id=ScienceBeamDagIds.SCIENCEBEAM_AUTOCUT_BUILD_IMAGE,
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

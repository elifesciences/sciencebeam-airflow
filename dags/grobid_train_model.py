from airflow.models import DAG

from sciencebeam_airflow.utils.airflow import add_dag_macro
from sciencebeam_airflow.utils.container_operators import ContainerRunOperator

from sciencebeam_airflow.dags.dag_ids import ScienceBeamDagIds

from sciencebeam_airflow.dags.utils import (
    get_default_args,
    create_trigger_next_task_dag_operator
)

from grobid_train_utils import (
    get_grobid_trainer_image,
    create_grobid_train_validate_config_operation
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


TRAIN_GROBID_MODEL_TEMPLATE = (
    '''
    train-header-model.sh \
        --dataset "{{ dag_run.conf.train.grobid.dataset }}" \
        --cloud-models-path "{{ dag_run.conf.train.grobid.models }}"
    '''
)


def create_train_grobid_model_operator(dag, task_id='train_grobid_model'):
    add_dag_macro(dag, 'get_grobid_trainer_image', get_grobid_trainer_image)
    return ContainerRunOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        image='{{ get_grobid_trainer_image(dag_run.conf) }}',
        name='{{ generate_run_name(dag_run.conf.sciencebeam_release_name, "train-grobid-model") }}',
        preemptible=True,
        requests='cpu=900m,memory=10Gi',
        command=TRAIN_GROBID_MODEL_TEMPLATE,
    )


def create_dag():
    dag = DAG(
        dag_id=ScienceBeamDagIds.GROBID_TRAIN_MODEL,
        default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    _ = (
        create_grobid_train_validate_config_operation(dag=dag)
        >> create_train_grobid_model_operator(dag=dag)
        >> create_trigger_next_task_dag_operator(dag=dag)
    )

    return dag


MAIN_DAG = create_dag()

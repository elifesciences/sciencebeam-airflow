from airflow.models import DAG

from sciencebeam_dag_ids import ScienceBeamDagIds

from sciencebeam_dag_utils import (
    get_default_args,
    add_dag_macro,
    create_trigger_next_task_dag_operator,
    get_gcp_project_id
)

from grobid_train_utils import (
    get_grobid_trainer_tools_image,
    create_grobid_train_validate_config_operation
)

from container_operators import ContainerRunOperator


DEFAULT_ARGS = get_default_args()


BUILD_GROBID_IMAGE_TEMPLATE = (
    '''
    build-grobid-docker-image.sh \
        "{{ dag_run.conf.train.grobid.models }}" \
        "{{ dag_run.conf.train.grobid.source_image }}" \
        "{{ dag_run.conf.train.grobid.output_image }}" \
        "{{ get_gcp_project_id() }}"
    '''
)


def create_build_grobid_image_operator(dag, task_id='build_grobid_image'):
    add_dag_macro(dag, 'get_grobid_trainer_tools_image', get_grobid_trainer_tools_image)
    add_dag_macro(dag, 'get_gcp_project_id', get_gcp_project_id)
    return ContainerRunOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        image='{{ get_grobid_trainer_tools_image(dag_run.conf) }}',
        name='{{ generate_run_name(dag_run.conf.sciencebeam_release_name, "build-image") }}',
        requests='cpu=100m,memory=256Mi',
        command=BUILD_GROBID_IMAGE_TEMPLATE,
    )


def create_dag():
    dag = DAG(
        dag_id=ScienceBeamDagIds.GROBID_BUILD_IMAGE,
        default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    _ = (
        create_grobid_train_validate_config_operation(dag=dag)
        >> create_build_grobid_image_operator(dag=dag)
        >> create_trigger_next_task_dag_operator(dag=dag)
    )

    return dag


MAIN_DAG = create_dag()

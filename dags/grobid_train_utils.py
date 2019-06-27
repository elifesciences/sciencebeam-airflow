from sciencebeam_dag_utils import (
    get_app_config_value,
    create_validate_config_operation
)


DEFAULT_GROBID_TRAINER_TOOLS_IMAGE = (
    'elifesciences/sciencebeam-trainer-grobid-tools_unstable'
    ':5b2ad616b5fdfb2fbde0aa9ec1930de511647a05'
)

DEFAULT_GROBID_TRAINER_IMAGE = (
    'elifesciences/sciencebeam-trainer-grobid_unstable'
    ':0.5.4-41ce96e225de2f8163b2255c7ad51270431ecc4e'
)


DEFAULT_GROBID_TRAIN_FIELDS = 'title,abstract'


class ConfigProps:
    SCIENCEBEAM_RELEASE_NAME = 'sciencebeam_release_name'
    NAMESPACE = 'namespace'
    TRAIN = 'train'


REQUIRED_PROPS = {
    ConfigProps.SCIENCEBEAM_RELEASE_NAME,
    ConfigProps.NAMESPACE,
    ConfigProps.TRAIN
}


def get_grobid_trainer_tools_image(config: dict = None):
    return get_app_config_value(
        'grobid_trainer_tools_image',
        config=config,
        default_value=DEFAULT_GROBID_TRAINER_TOOLS_IMAGE,
        required=True
    )


def get_grobid_trainer_image(config: dict = None):
    return get_app_config_value(
        'grobid_trainer_image',
        config=config,
        default_value=DEFAULT_GROBID_TRAINER_IMAGE,
        required=True
    )


def create_grobid_train_validate_config_operation(dag, task_id='validate_config'):
    return create_validate_config_operation(
        dag=dag,
        task_id=task_id,
        required_props=REQUIRED_PROPS
    )

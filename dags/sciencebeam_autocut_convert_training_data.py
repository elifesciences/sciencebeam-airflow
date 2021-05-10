import os

from airflow.models import DAG

from sciencebeam_airflow.dags.dag_ids import ScienceBeamDagIds

from sciencebeam_airflow.dags.utils import (
    get_default_args
)

from sciencebeam_convert import (
    create_dag as create_sciencebeam_convert_dag,
    ScienceBeamConvertMacros
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


class AutocutScienceBeamConvertMacros(ScienceBeamConvertMacros):
    def get_model(self, conf: dict) -> dict:
        return conf['train']['autocut']['input_model']

    def get_source_conf(self, conf: dict) -> dict:
        source_dataset: dict = conf['train']['source_dataset']
        file_list = source_dataset['source_file_list']
        return {
            'file_column': 'source_url',
            'data_path': os.path.dirname(file_list),
            'file_list': os.path.basename(file_list),
            'absolute_file_list': file_list
        }

    def get_output_conf(self, conf: dict) -> dict:
        input_source: dict = conf['train']['autocut']['input_source']
        file_list = input_source['file_list']
        return {
            'output_suffix': conf['output_suffix'],
            'data_path': os.path.dirname(file_list),
            'file_list': os.path.basename(file_list),
            'absolute_file_list': file_list
        }

    def get_limit(self, conf: dict) -> str:
        return conf['train']['limit']


def create_dag() -> DAG:
    return create_sciencebeam_convert_dag(
        dag_id=ScienceBeamDagIds.SCIENCEBEAM_AUTOCUT_CONVERT_TRAINING_DATA,
        default_args=DEFAULT_ARGS,
        macros=AutocutScienceBeamConvertMacros()
    )


MAIN_DAG = create_dag()

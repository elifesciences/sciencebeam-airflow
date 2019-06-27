from unittest.mock import patch

import pytest

from dags.sciencebeam_dag_ids import (
    DEFAULT_CONVERT_AND_EVALUATE_TASKS,
    DEFAULT_GROBID_TRAIN_TASKS,
    DEFAULT_AUTOCUT_TRAIN_TASKS
)

import dags.sciencebeam_watch_experiments as sciencebeam_watch_experiments_module
from dags.sciencebeam_watch_experiments import (
    create_dag,
    get_namespace,
    get_default_limit,
    get_conf_for_experiment_data,
    InvalidConfigError
)


DEFAULT_SOURCE_DATA_PATH = 'xy://host/path/to/source'
DEFAULT_FILE_LIST = 'file-list.tsv'
DEFAULT_MODEL_NAME = 'model1'

DEFAULT_DATASET_NAME = 'dataset1'
DATASET_SUBSET_NAME_1 = 'subset1'
DATASET_EVAL_NAME_1 = 'eval1'

IMAGE_1 = 'group/name:tag1'

NAMESPACE_1 = 'namespace1'

LIMIT_1 = 123

DEFAULT_EXPERIMENT_DATA = {
    'created': '2019-01-02',
    'model': {
        'name': DEFAULT_MODEL_NAME,
        'sciencebeam_image': 'test/sciencebeam_image:0.0.1',
        'grobid_image': 'test/grobid_image:0.0.1'
    },
    'dataset': {
        'name': DEFAULT_DATASET_NAME,
        'source_file_list': f'{DEFAULT_SOURCE_DATA_PATH}/{DEFAULT_FILE_LIST}'
    }
}


TRAIN_SOURCE_DATASET = {
    'name': DEFAULT_DATASET_NAME + '-train',
    'source_file_list': f'{DEFAULT_SOURCE_DATA_PATH}/file-list-train.tsv'
}

GROBID_TRAIN_CONFIG_1 = {'grobid': {'dummy_train_config': {}}}
AUTOCUT_TRAIN_CONFIG_1 = {
    'source_dataset': TRAIN_SOURCE_DATASET,
    'autocut': {
        "input_model": {
            "name": "grobid-tei-0.5.3"
        },
        "input_source": {
            "xpath": "tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title",
        }
    }
}
UNKNOWN_TRAIN_CONFIG_1 = {'xyz': {'dummy_train_config': {}}}
TRAIN_CONFIG_1 = GROBID_TRAIN_CONFIG_1


@pytest.fixture(name='file_exists', autouse=True)
def _file_exists_mock():
    with patch.object(sciencebeam_watch_experiments_module, 'file_exists') as mock:
        yield mock


@pytest.fixture(name='get_namespace_mock', autouse=True)
def _get_namespace_mock():
    with patch.object(sciencebeam_watch_experiments_module, 'get_namespace') as mock:
        mock.return_value = NAMESPACE_1
        yield mock


@pytest.fixture(name='get_default_limit_mock', autouse=True)
def _get_default_limit_mock():
    with patch.object(sciencebeam_watch_experiments_module, 'get_default_limit') as mock:
        mock.return_value = None
        yield mock


class TestScienceBeamWatchExperiments:
    class TestCreateDag:
        def test_should_be_able_to_create_dag(self):
            create_dag()

    class TestGetNamespace:
        @patch('os.environ')
        def test_should_return_environ_namespace(self, os_mock):
            os_mock.__getitem__.side_effect = {
                'SCIENCEBEAM_NAMESPACE': NAMESPACE_1
            }.get
            assert get_namespace() == NAMESPACE_1

    class TestGetDefaultLimit:
        @patch('os.environ')
        def test_should_return_environ_default_limit(self, os_mock):
            os_mock.__getitem__.side_effect = {
                'SCIENCEBEAM_DEFAULT_LIMIT': '123'
            }.get
            assert get_default_limit() == '123'

    @pytest.mark.usefixture('get_default_limit_mock', 'get_namespace_mock')
    class TestGetConfForExperimentData:
        def test_should_pass_through_model(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA
            })['model'] == DEFAULT_EXPERIMENT_DATA['model']

        def test_should_determine_source_data_path(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA
            })['source_data_path'] == DEFAULT_SOURCE_DATA_PATH

        def test_should_determine_source_file_list(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA
            })['source_file_list'] == DEFAULT_FILE_LIST

        def test_should_determine_output_data_path(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA
            })['output_data_path'] == (
                f'{DEFAULT_SOURCE_DATA_PATH}-results/{NAMESPACE_1}/{DEFAULT_MODEL_NAME}'
            )

        def test_should_determine_output_file_list(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA
            })['output_file_list'] == (
                f'file-list.lst'
            )

        def test_should_include_dataset_subset_name_in_output_file_list(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'dataset': {
                    **DEFAULT_EXPERIMENT_DATA['dataset'],
                    'subset_name': DATASET_SUBSET_NAME_1
                }
            })['output_file_list'] == (
                f'file-list-{DATASET_SUBSET_NAME_1}.lst'
            )

        def test_should_include_limit_in_output_file_list(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'limit': f'{LIMIT_1}'
            })['output_file_list'] == (
                f'file-list-{LIMIT_1}.lst'
            )

        def test_should_determine_output_suffix(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA
            })['output_suffix'] == f'.xml.gz'

        def test_should_determine_eval_output_path(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA
            })['eval_output_path'] == (
                f'{DEFAULT_SOURCE_DATA_PATH}-results/{NAMESPACE_1}/'
                f'{DEFAULT_MODEL_NAME}/evaluation-results/all'
            )

        def test_should_include_subset_name_in_eval_output_path(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'dataset': {
                    **DEFAULT_EXPERIMENT_DATA['dataset'],
                    'subset_name': DATASET_SUBSET_NAME_1
                }
            })['eval_output_path'] == (
                f'{DEFAULT_SOURCE_DATA_PATH}-results/{NAMESPACE_1}/'
                f'{DEFAULT_MODEL_NAME}/evaluation-results/{DATASET_SUBSET_NAME_1}'
            )

        def test_should_include_eval_name_in_eval_output_path(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'dataset': {
                    **DEFAULT_EXPERIMENT_DATA['dataset'],
                    'eval_name': DATASET_EVAL_NAME_1
                }
            })['eval_output_path'] == (
                f'{DEFAULT_SOURCE_DATA_PATH}-results/{NAMESPACE_1}/'
                f'{DEFAULT_MODEL_NAME}/evaluation-results/{DATASET_EVAL_NAME_1}'
            )

        def test_should_include_subset_name_and_limit_in_eval_output_path(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'limit': LIMIT_1,
                'dataset': {
                    **DEFAULT_EXPERIMENT_DATA['dataset'],
                    'subset_name': DATASET_SUBSET_NAME_1
                }
            })['eval_output_path'] == (
                f'{DEFAULT_SOURCE_DATA_PATH}-results/{NAMESPACE_1}/'
                f'{DEFAULT_MODEL_NAME}/evaluation-results/{DATASET_SUBSET_NAME_1}-{LIMIT_1}'
            )

        def test_should_determine_run_name(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA
            })['run_name'] == (
                f'{DEFAULT_DATASET_NAME}_{DEFAULT_MODEL_NAME}'
            )

        def test_should_include_subset_name_in_run_name(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'dataset': {
                    **DEFAULT_EXPERIMENT_DATA['dataset'],
                    'subset_name': DATASET_SUBSET_NAME_1
                }
            })['run_name'] == (
                f'{DEFAULT_DATASET_NAME}-{DATASET_SUBSET_NAME_1}_{DEFAULT_MODEL_NAME}'
            )

        def test_should_include_eval_name_in_run_name(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'dataset': {
                    **DEFAULT_EXPERIMENT_DATA['dataset'],
                    'eval_name': DATASET_EVAL_NAME_1
                }
            })['run_name'] == (
                f'{DEFAULT_DATASET_NAME}-{DATASET_EVAL_NAME_1}_{DEFAULT_MODEL_NAME}'
            )

        def test_should_use_limit_from_experiment(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'limit': '123'
            })['limit'] == '123'

        def test_should_fallback_to_dataset_limit(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'dataset': {
                    **DEFAULT_EXPERIMENT_DATA['dataset'],
                    'limit': '123'
                }
            })['limit'] == '123'

        def test_should_fallback_to_environ_default_limit(self, get_default_limit_mock):
            get_default_limit_mock.return_value = '123'
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA
            })['limit'] == '123'

        def test_should_enable_resume_if_enabled_by_experiment(self):
            assert get_conf_for_experiment_data({  # pylint: disable=singleton-comparison
                **DEFAULT_EXPERIMENT_DATA,
                'resume': 'true'
            })['resume'] is True

        def test_should_disable_resume_if_disabled_by_experiment(self):
            assert get_conf_for_experiment_data({  # pylint: disable=singleton-comparison
                **DEFAULT_EXPERIMENT_DATA,
                'resume': 'false'
            })['resume'] is False

        def test_should_enable_resume_by_default(self):
            assert get_conf_for_experiment_data({  # pylint: disable=singleton-comparison
                **DEFAULT_EXPERIMENT_DATA
            })['resume'] is True

        def test_should_include_train_config(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'train': TRAIN_CONFIG_1
            })['train'] == TRAIN_CONFIG_1

        def test_should_include_image_config(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'xyz_image': IMAGE_1
            })['xyz_image'] == IMAGE_1

        def test_should_include_tasks(self):
            tasks = ['task1', 'task2']
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'tasks': tasks
            })['tasks'] == tasks

        def test_should_use_default_to_convert_tasks_without_train_config(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA
            })['tasks'] == DEFAULT_CONVERT_AND_EVALUATE_TASKS

        def test_should_use_default_to_grobid_train_tasks_with_grobid_train_config(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'train': GROBID_TRAIN_CONFIG_1
            })['tasks'] == DEFAULT_GROBID_TRAIN_TASKS

        def test_should_use_default_to_autocut_train_tasks_with_autocut_train_config(self):
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'train': AUTOCUT_TRAIN_CONFIG_1
            })['tasks'] == DEFAULT_AUTOCUT_TRAIN_TASKS

        def test_should_raise_exception_with_unknown_config(self):
            with pytest.raises(InvalidConfigError):
                get_conf_for_experiment_data({
                    **DEFAULT_EXPERIMENT_DATA,
                    'train': UNKNOWN_TRAIN_CONFIG_1
                })

        def test_should_determine_autocut_train_input_source_file_list(self):
            input_model = AUTOCUT_TRAIN_CONFIG_1['autocut']['input_model']
            input_model_name = input_model['name']
            assert get_conf_for_experiment_data({
                **DEFAULT_EXPERIMENT_DATA,
                'train': AUTOCUT_TRAIN_CONFIG_1
            })['train']['autocut']['input_source'].get('file_list') == (
                f'{DEFAULT_SOURCE_DATA_PATH}-results/{NAMESPACE_1}/'
                f'{input_model_name}/file-list.lst'
            )

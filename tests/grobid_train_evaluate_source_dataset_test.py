import os


from sciencebeam_airflow.dags.dag_ids import (
    ScienceBeamDagIds,
    DEFAULT_CONVERT_AND_EVALUATE_TASKS
)

from dags.sciencebeam_dag_conf import (
    ScienceBeamDagConf,
    ScienceBeamTrainDagConf,
    ScienceBeamDatasetDagConf,
    get_output_data_path,
    get_file_list_path,
    get_eval_output_path
)

from dags.sciencebeam_dag_utils import (
    get_combined_run_name
)

from dags.grobid_train_evaluate_source_dataset import (
    create_dag,
    _prepare_convert_and_evaluate_source_dataset_conf,
    SUB_RUN_NAME,
    RELEASE_NAME_SUFFIX
)

from .grobid_train_test_utils import DEFAULT_CONF


SCIENCEBEAM_JUDGE_IMAGE_1 = 'judge:0.0.1'


FIELD_1 = 'field1'


class TestScienceBeamEvaluate:
    class TestCreateDag:
        def test_should_be_able_to_create_dag(self):
            create_dag()

    class TestPrepareConvertAndEvaluateSourceDatasetConf:
        def test_should_include_dataset_paths(self):
            train_limit = '123'
            source_conf = {
                **DEFAULT_CONF,
                ScienceBeamDagConf.TRAIN: {
                    **DEFAULT_CONF[ScienceBeamDagConf.TRAIN],
                    'limit': train_limit
                }
            }
            train_conf: dict = source_conf[ScienceBeamDagConf.TRAIN]
            source_dataset: dict = train_conf[ScienceBeamTrainDagConf.SOURCE_DATASET]
            source_file_list = source_dataset[ScienceBeamDatasetDagConf.SOURCE_FILE_LIST]
            conf = _prepare_convert_and_evaluate_source_dataset_conf(source_conf)
            assert conf[ScienceBeamDagConf.SOURCE_DATA_PATH] == os.path.dirname(source_file_list)
            assert conf[ScienceBeamDagConf.SOURCE_FILE_LIST] == os.path.basename(source_file_list)
            assert conf[ScienceBeamDagConf.OUTPUT_DATA_PATH] == get_output_data_path(
                source_data_path=os.path.dirname(source_file_list),
                namespace=DEFAULT_CONF[ScienceBeamDagConf.NAMESPACE],
                model_name=DEFAULT_CONF[ScienceBeamDagConf.MODEL_NAME]
            )
            assert conf[ScienceBeamDagConf.OUTPUT_FILE_LIST] == os.path.basename(get_file_list_path(
                output_data_path=conf[ScienceBeamDagConf.OUTPUT_DATA_PATH],
                dataset=source_dataset,
                limit=train_limit
            ))
            assert conf[ScienceBeamDagConf.EVAL_OUTPUT_PATH] == get_eval_output_path(
                output_data_path=conf[ScienceBeamDagConf.OUTPUT_DATA_PATH],
                dataset=source_dataset,
                limit=train_limit
            )

        def test_should_include_train_limit(self):
            conf = _prepare_convert_and_evaluate_source_dataset_conf({
                **DEFAULT_CONF,
                'train': {
                    **DEFAULT_CONF[ScienceBeamDagConf.TRAIN],
                    ScienceBeamTrainDagConf.LIMIT: '123'
                }
            })
            assert conf[ScienceBeamDagConf.LIMIT] == '123'

        def test_should_not_include_limit_if_no_train_limit_specified(self):
            conf = _prepare_convert_and_evaluate_source_dataset_conf({
                **DEFAULT_CONF,
                'train': {
                    **DEFAULT_CONF[ScienceBeamDagConf.TRAIN],
                    ScienceBeamTrainDagConf.LIMIT: ''
                }
            })
            assert ScienceBeamDagConf.LIMIT not in conf

        def test_should_include_default_evaluate_tasks(self):
            conf = _prepare_convert_and_evaluate_source_dataset_conf({
                **DEFAULT_CONF,
                'tasks': None
            })
            assert conf[ScienceBeamDagConf.TASKS] == DEFAULT_CONVERT_AND_EVALUATE_TASKS

        def test_should_filter_tasks(self):
            expected_tasks = [
                ScienceBeamDagIds.SCIENCEBEAM_CONVERT
            ]
            tasks = ['previous tasks'] + expected_tasks + ['other tasks']
            conf = _prepare_convert_and_evaluate_source_dataset_conf({
                **DEFAULT_CONF,
                'tasks': tasks
            })
            assert conf[ScienceBeamDagConf.TASKS] == expected_tasks

        def test_should_include_source_dataset_name(self):
            train_conf: dict = DEFAULT_CONF[ScienceBeamDagConf.TRAIN]
            source_dataset: dict = train_conf[ScienceBeamTrainDagConf.SOURCE_DATASET]
            source_dataset_name = source_dataset[ScienceBeamDatasetDagConf.NAME]
            conf = _prepare_convert_and_evaluate_source_dataset_conf(DEFAULT_CONF)
            assert conf[ScienceBeamDagConf.DATASET_NAME] == source_dataset_name

        def test_should_include_run_name_suffix(self):
            conf = _prepare_convert_and_evaluate_source_dataset_conf({
                **DEFAULT_CONF,
                ScienceBeamDagConf.RUN_NAME: 'run1'
            })
            assert conf[ScienceBeamDagConf.RUN_NAME] == get_combined_run_name(
                'run1', SUB_RUN_NAME
            )

        def test_should_include_release_name_suffix(self):
            conf = _prepare_convert_and_evaluate_source_dataset_conf({
                **DEFAULT_CONF,
                ScienceBeamDagConf.SCIENCEBEAM_RELEASE_NAME: 'release1'
            })
            assert conf[ScienceBeamDagConf.SCIENCEBEAM_RELEASE_NAME] == (
                'release1' + RELEASE_NAME_SUFFIX
            )

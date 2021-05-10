from sciencebeam_airflow.dags.dag_ids import (
    ScienceBeamDagIds,
    DEFAULT_EVALUATE_TASKS
)

from sciencebeam_airflow.dags.dag_conf import (
    ScienceBeamDagConf,
    ScienceBeamTrainDagConf,
    ScienceBeamDatasetDagConf,
    ScienceBeamTrainGrobidDagConf
)

from sciencebeam_airflow.dags.utils import (
    get_combined_run_name
)

from dags.grobid_train_evaluate_grobid_train_tei import (
    create_dag,
    _prepare_evaluate_grobid_train_tei_conf,
    DEFAULT_GROBID_TRAIN_FIELDS,
    GROBID_TRAIN_MODEL_NAME,
    GROBID_TRAIN_DATASET_SUFFIX,
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

    class TestPrepareEvaluateGrobidTrainTeiConf:
        def test_should_include_dataset_paths(self):
            train_conf: dict = DEFAULT_CONF[ScienceBeamDagConf.TRAIN]
            train_grobid_conf: dict = train_conf[ScienceBeamTrainDagConf.GROBID]
            grobid_dataset_path: str = train_grobid_conf[ScienceBeamTrainGrobidDagConf.DATASET]
            conf = _prepare_evaluate_grobid_train_tei_conf(DEFAULT_CONF)
            assert conf[ScienceBeamDagConf.SOURCE_DATA_PATH] == grobid_dataset_path + '/xml'
            assert conf[ScienceBeamDagConf.SOURCE_FILE_LIST] == 'file-list.lst'
            assert conf[ScienceBeamDagConf.OUTPUT_DATA_PATH] == (
                grobid_dataset_path + '/header/corpus/tei'
            )
            assert conf[ScienceBeamDagConf.OUTPUT_FILE_LIST] == 'file-list.lst'
            assert conf[ScienceBeamDagConf.EVAL_OUTPUT_PATH] == (
                grobid_dataset_path + '/evaluation-results'
            )

        def test_should_include_train_limit(self):
            conf = _prepare_evaluate_grobid_train_tei_conf({
                **DEFAULT_CONF,
                'train': {
                    **DEFAULT_CONF[ScienceBeamDagConf.TRAIN],
                    ScienceBeamTrainDagConf.LIMIT: '123'
                }
            })
            assert conf[ScienceBeamDagConf.LIMIT] == '123'

        def test_should_not_include_limit_if_no_train_limit_specified(self):
            conf = _prepare_evaluate_grobid_train_tei_conf({
                **DEFAULT_CONF,
                'train': {
                    **DEFAULT_CONF[ScienceBeamDagConf.TRAIN],
                    ScienceBeamTrainDagConf.LIMIT: ''
                }
            })
            assert ScienceBeamDagConf.LIMIT not in conf

        def test_should_include_train_fields(self):
            conf = _prepare_evaluate_grobid_train_tei_conf({
                **DEFAULT_CONF,
                'train': {
                    **DEFAULT_CONF[ScienceBeamDagConf.TRAIN],
                    ScienceBeamTrainDagConf.FIELDS: FIELD_1
                }
            })
            assert conf[ScienceBeamDagConf.FIELDS] == FIELD_1

        def test_should_include_default_train_fields_if_not_specified(self):
            conf = _prepare_evaluate_grobid_train_tei_conf({
                **DEFAULT_CONF,
                'train': {
                    **DEFAULT_CONF[ScienceBeamDagConf.TRAIN],
                    ScienceBeamTrainDagConf.FIELDS: ''
                }
            })
            assert conf[ScienceBeamDagConf.FIELDS] == DEFAULT_GROBID_TRAIN_FIELDS

        def test_should_include_default_evaluate_tasks(self):
            conf = _prepare_evaluate_grobid_train_tei_conf({
                **DEFAULT_CONF,
                'tasks': None
            })
            assert conf[ScienceBeamDagConf.TASKS] == DEFAULT_EVALUATE_TASKS

        def test_should_filter_tasks(self):
            expected_tasks = [
                ScienceBeamDagIds.SCIENCEBEAM_EVALUATE
            ]
            tasks = ['previous tasks'] + expected_tasks + ['other tasks']
            conf = _prepare_evaluate_grobid_train_tei_conf({
                **DEFAULT_CONF,
                'tasks': tasks
            })
            assert conf[ScienceBeamDagConf.TASKS] == expected_tasks

        def test_should_include_grobid_train_model_name(self):
            conf = _prepare_evaluate_grobid_train_tei_conf(DEFAULT_CONF)
            assert conf[ScienceBeamDagConf.MODEL_NAME] == GROBID_TRAIN_MODEL_NAME

        def test_should_not_include_model_props(self):
            conf = _prepare_evaluate_grobid_train_tei_conf(DEFAULT_CONF)
            assert conf.get(ScienceBeamDagConf.MODEL) is None

        def test_should_include_grobid_train_dataset_name(self):
            train_conf: dict = DEFAULT_CONF[ScienceBeamDagConf.TRAIN]
            source_dataset: dict = train_conf[ScienceBeamTrainDagConf.SOURCE_DATASET]
            source_dataset_name = source_dataset[ScienceBeamDatasetDagConf.NAME]
            conf = _prepare_evaluate_grobid_train_tei_conf(DEFAULT_CONF)
            assert conf[ScienceBeamDagConf.DATASET_NAME] == (
                f'{source_dataset_name}{GROBID_TRAIN_DATASET_SUFFIX}'
            )

        def test_should_include_run_name_suffix(self):
            conf = _prepare_evaluate_grobid_train_tei_conf({
                **DEFAULT_CONF,
                ScienceBeamDagConf.RUN_NAME: 'run1'
            })
            assert conf[ScienceBeamDagConf.RUN_NAME] == get_combined_run_name(
                'run1', SUB_RUN_NAME
            )

        def test_should_include_release_name_suffix(self):
            conf = _prepare_evaluate_grobid_train_tei_conf({
                **DEFAULT_CONF,
                ScienceBeamDagConf.SCIENCEBEAM_RELEASE_NAME: 'release1'
            })
            assert conf[ScienceBeamDagConf.SCIENCEBEAM_RELEASE_NAME] == (
                'release1' + RELEASE_NAME_SUFFIX
            )

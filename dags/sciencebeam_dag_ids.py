class ScienceBeamDagIds:
    SCIENCEBEAM_WATCH_EXPERIMENTS = 'sciencebeam_watch_experiments'
    GROBID_TRAIN_PREPARE = 'grobid_train_prepare'
    GROBID_TRAIN_EVALUATE_GROBID_TRAIN_TEI = 'grobid_train_evaluate_grobid_train_tei'
    GROBID_TRAIN_MODEL = 'grobid_train_model'
    GROBID_BUILD_IMAGE = 'grobid_build_image'
    GROBID_TRAIN_EVALUATE_SOURCE_DATASET = 'grobid_train_evaluate_source_dataset'
    SCIENCEBEAM_ADD_MODEL_CONFIG = 'sciencebeam_add_model_config'
    SCIENCEBEAM_CONVERT = 'sciencebeam_convert'
    SCIENCEBEAM_EVALUATE = 'sciencebeam_evaluate'
    SCIENCEBEAM_EVALUATION_RESULTS_TO_BQ = 'sciencebeam_evaluation_results_to_bq'
    SCIENCEBEAM_AUTOCUT_CONVERT_TRAINING_DATA = 'sciencebeam_autocut_convert_training_data'
    SCIENCEBEAM_AUTOCUT_TRAIN_MODEL = 'sciencebeam_autocut_train_model'
    SCIENCEBEAM_AUTOCUT_BUILD_IMAGE = 'sciencebeam_autocut_build_image'


DEFAULT_EVALUATE_TASKS = [
    ScienceBeamDagIds.SCIENCEBEAM_EVALUATE,
    ScienceBeamDagIds.SCIENCEBEAM_EVALUATION_RESULTS_TO_BQ
]


DEFAULT_CONVERT_AND_EVALUATE_TASKS = [
    ScienceBeamDagIds.SCIENCEBEAM_CONVERT
] + DEFAULT_EVALUATE_TASKS


DEFAULT_GROBID_TRAIN_TASKS = [
    ScienceBeamDagIds.GROBID_TRAIN_PREPARE,
    ScienceBeamDagIds.GROBID_TRAIN_EVALUATE_GROBID_TRAIN_TEI,
    ScienceBeamDagIds.GROBID_TRAIN_MODEL,
    ScienceBeamDagIds.GROBID_BUILD_IMAGE,
    ScienceBeamDagIds.SCIENCEBEAM_ADD_MODEL_CONFIG,
] + DEFAULT_CONVERT_AND_EVALUATE_TASKS + [
    ScienceBeamDagIds.GROBID_TRAIN_EVALUATE_SOURCE_DATASET
]


DEFAULT_AUTOCUT_TRAIN_TASKS = [
    ScienceBeamDagIds.SCIENCEBEAM_AUTOCUT_CONVERT_TRAINING_DATA,
    ScienceBeamDagIds.SCIENCEBEAM_AUTOCUT_TRAIN_MODEL,
    ScienceBeamDagIds.SCIENCEBEAM_ADD_MODEL_CONFIG
] + DEFAULT_CONVERT_AND_EVALUATE_TASKS

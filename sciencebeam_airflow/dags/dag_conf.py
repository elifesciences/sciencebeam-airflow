import os


class ScienceBeamDagConf:
    RUN_NAME = 'run_name'
    NAMESPACE = 'namespace'
    SCIENCEBEAM_RELEASE_NAME = 'sciencebeam_release_name'
    SOURCE_DATA_PATH = 'source_data_path'
    SOURCE_FILE_LIST = 'source_file_list'
    OUTPUT_DATA_PATH = 'output_data_path'
    OUTPUT_FILE_LIST = 'output_file_list'
    OUTPUT_SUFFIX = 'output_suffix'
    EVAL_OUTPUT_PATH = 'eval_output_path'
    DATASET_NAME = 'dataset_name'
    MODEL_NAME = 'model_name'
    MODEL = 'model'
    FIELDS = 'fields'
    TRAIN = 'train'
    RESUME = 'resume'
    LIMIT = 'limit'
    TASKS = 'tasks'


class ScienceBeamTrainDagConf:
    SOURCE_DATASET = 'source_dataset'
    GROBID = 'grobid'
    FIELDS = 'fields'
    LIMIT = 'limit'


class ScienceBeamTrainGrobidDagConf:
    DATASET = 'dataset'


class ScienceBeamDatasetDagConf:
    NAME = 'name'
    SOURCE_FILE_LIST = 'source_file_list'
    LIMIT = 'limit'


def get_output_data_path(source_data_path: str, namespace: str, model_name: str) -> str:
    return f'{source_data_path}-results/{namespace}/{model_name}'


def get_file_list_path(output_data_path: str, dataset: dict, limit: int) -> str:
    dataset_subset_name = dataset.get('subset_name')
    if dataset_subset_name:
        suffix = f'-{dataset_subset_name}'
    else:
        suffix = ''
    if limit:
        suffix += f'-{limit}'
    return os.path.join(output_data_path, f'file-list{suffix}.lst')


def get_eval_output_path(
        output_data_path: str,
        dataset: dict,
        limit: int,
        eval_name: str = None) -> str:
    dataset_eval_name = eval_name or dataset.get('eval_name')
    dataset_subset_name = dataset.get('subset_name')
    if dataset_eval_name:
        folder_name = f'{dataset_eval_name}'
    elif dataset_subset_name:
        folder_name = f'{dataset_subset_name}'
    else:
        folder_name = 'all'
    if limit:
        folder_name += f'-{limit}'
    return os.path.join(output_data_path, f'evaluation-results/{folder_name}')

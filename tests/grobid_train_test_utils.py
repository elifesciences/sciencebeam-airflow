NAMESPACE_1 = 'namespace1'

MODEL_NAME_1 = 'model1'

MODEL_1 = {
    'name': MODEL_NAME_1
}

DEFAULT_CONF = {
    'namespace': NAMESPACE_1,
    'sciencebeam_release_name': 'sciencebeam_release_name_1',
    'model_name': MODEL_NAME_1,
    'model': MODEL_1,
    'train': {
        'source_dataset': {
            'name': 'source_dataset_name_1',
            'source_file_list': '/document/file_list_1',
            'source_file_column': 'document_file_column',
            'target_file_list': '/xml/file_list_1',
            'target_file_column': 'xml_file_column'
        },
        'grobid': {
            'dataset': 'grobid_dataset_1',
            'models': 'grobid_model_1',
            'source_image': 'grobid_image_1',
            'output_image': 'output_image_1'
        }
    }
}

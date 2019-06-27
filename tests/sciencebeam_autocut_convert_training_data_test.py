from dags.sciencebeam_autocut_convert_training_data import (
    create_dag,
    AutocutScienceBeamConvertMacros
)

NAMESPACE_1 = 'namespace1'

DEFAULT_CONF = {
    'namespace': NAMESPACE_1,
    'train': {
        'source_dataset': {
            'source_file_list': 'source_file_list1'
        },
        'limit': '100',
        "autocut": {
            "input_model": {
                "name": "model1",
            },
            "input_source": {
                "xpath": "tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title",
                "file_list": "file_list2"
            },
            "convert": True,
            "model": "$(AUTOCUT_MODEL_PATH)"
        }
    },
    'source_data_path': 'source_data_path1',
    'source_file_list': 'source_file_list1',
    'output_data_path': 'output_data_path1',
    'output_file_list': 'output_file_list1',
    'output_suffix': '.xml.gz',
    'limit': '100'
}


class TestScienceBeamAutocutTrainModel:
    class TestCreateDag:
        def test_should_be_able_to_create_dag(self):
            assert create_dag()

    class TestAutocutScienceBeamConvertMacros:
        def test_default_config_should_be_valid(self):
            assert AutocutScienceBeamConvertMacros().is_config_valid(DEFAULT_CONF)

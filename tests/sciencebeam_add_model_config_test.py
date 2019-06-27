import logging
import json
import os
from unittest.mock import patch, MagicMock

import pytest

import dags.sciencebeam_add_model_config as sciencebeam_add_model_config_module
from dags.sciencebeam_add_model_config import (
    create_dag,
    add_model_config,
    get_models_path,
    get_model_filename
)


LOGGER = logging.getLogger(__name__)


MODEL_CONFIG_1 = {
    'name': 'model_name_1'
}


@pytest.fixture(name='set_file_content_mock')
def _set_file_content_mock():
    with patch.object(sciencebeam_add_model_config_module, 'set_file_content') as mock:
        yield mock


class TestGrobidAddModelConfig:
    class TestCreateDag:
        def test_should_be_able_to_create_dag(self):
            create_dag()

    class TestCreateAddModelConfig:
        def test_should_call_set_file_content_with_model_json(
                self, dag_run, set_file_content_mock: MagicMock):
            dag_run.conf = {
                'model': MODEL_CONFIG_1
            }
            add_model_config(dag_run=dag_run)
            set_file_content_mock.assert_called_with(
                os.path.join(get_models_path(), get_model_filename(MODEL_CONFIG_1)),
                json.dumps(MODEL_CONFIG_1, indent=2).encode('utf-8')
            )

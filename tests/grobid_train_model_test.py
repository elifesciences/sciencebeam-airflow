import logging

from dags.grobid_train_model import (
    create_dag,
    create_train_grobid_model_operator
)

from .test_utils import (
    parse_command_arg,
    create_and_render_command
)

from .grobid_train_test_utils import (
    DEFAULT_CONF
)


LOGGER = logging.getLogger(__name__)


def _create_and_render_train_grobid_model_command(dag, airflow_context: dict) -> str:
    return create_and_render_command(
        create_train_grobid_model_operator(dag=dag),
        airflow_context
    )


class TestGrobidTrainModel:
    class TestCreateDag:
        def test_should_be_able_to_create_dag(self):
            create_dag()

    class TestCreateTrainGrobidModelOperator:
        def test_should_include_namespace(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_train_grobid_model_command(
                dag, airflow_context
            )
            opt = parse_command_arg(rendered_bash_command, {'--namespace': str})
            assert getattr(opt, 'namespace') == DEFAULT_CONF['namespace']

        def test_should_include_dataset(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_train_grobid_model_command(
                dag, airflow_context
            )
            opt = parse_command_arg(rendered_bash_command, {'--dataset': str})
            assert (
                getattr(opt, 'dataset')
                == DEFAULT_CONF['train']['grobid']['dataset']
            )

        def test_should_include_models_output_path(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_train_grobid_model_command(
                dag, airflow_context
            )
            opt = parse_command_arg(rendered_bash_command, {'--cloud-models-path': str})
            assert (
                getattr(opt, 'cloud_models_path')
                == DEFAULT_CONF['train']['grobid']['models']
            )

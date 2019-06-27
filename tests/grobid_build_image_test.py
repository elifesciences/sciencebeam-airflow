import logging

from dags.grobid_build_image import (
    create_dag,
    create_build_grobid_image_operator
)

from .test_utils import (
    parse_command_arg,
    create_and_render_command
)

from .grobid_train_test_utils import (
    DEFAULT_CONF
)


LOGGER = logging.getLogger(__name__)


def _create_and_render_build_grobid_image_command(dag, airflow_context: dict) -> str:
    return create_and_render_command(
        create_build_grobid_image_operator(dag=dag),
        airflow_context
    )


class TestGrobidBuildImage:
    class TestCreateDag:
        def test_should_be_able_to_create_dag(self):
            create_dag()

    class TestCreateTrainGrobidModelOperator:
        def test_should_include_namespace(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_build_grobid_image_command(
                dag, airflow_context
            )
            opt = parse_command_arg(rendered_bash_command, {'--namespace': str})
            assert getattr(opt, 'namespace') == DEFAULT_CONF['namespace']

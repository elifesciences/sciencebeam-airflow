import logging

from dags.grobid_train_prepare import (
    create_dag,
    get_source_dataset_args,
    create_get_data_operator,
    create_generate_training_data_operator,
    create_auto_annotate_header_operator,
    DEFAULT_GROBID_TRAIN_FIELDS
)

from .test_utils import (
    parse_command_arg,
    create_and_render_command
)

from .grobid_train_test_utils import (
    DEFAULT_CONF
)


LOGGER = logging.getLogger(__name__)


FIELD_1 = 'field1'


def _create_and_render_get_data_command(dag, airflow_context: dict) -> str:
    return create_and_render_command(
        create_get_data_operator(dag=dag),
        airflow_context
    )


def _create_and_render_generate_training_data_command(dag, airflow_context: dict) -> str:
    return create_and_render_command(
        create_generate_training_data_operator(dag=dag),
        airflow_context
    )


def _create_and_render_auto_annotate_command(dag, airflow_context: dict) -> str:
    return create_and_render_command(
        create_auto_annotate_header_operator(dag=dag),
        airflow_context
    )


class TestGrobidTrainPrepare:
    class TestCreateDag:
        def test_should_be_able_to_create_dag(self):
            create_dag()

    class TestGetSourceDatasetArgs:
        def test_should_use_defaults(self):
            source_file_list = 'source_file_list_1'
            assert get_source_dataset_args({
                'source_file_list': source_file_list
            }) == {
                'document-file-list': source_file_list,
                'document-file-column': 'source_url',
                'target-file-list': source_file_list,
                'target-file-column': 'xml_url'
            }

    class TestCreateGetDataOperator:
        def test_should_include_namespace(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_get_data_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--namespace': str})
            assert getattr(opt, 'namespace') == DEFAULT_CONF['namespace']

        def test_should_include_document_source(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_get_data_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {
                '--document-file-list': str,
                '--document-file-column': str
            })
            assert (
                getattr(opt, 'document_file_list')
                == DEFAULT_CONF['train']['source_dataset']['source_file_list']
            )
            assert (
                getattr(opt, 'document_file_column')
                == DEFAULT_CONF['train']['source_dataset']['source_file_column']
            )

        def test_should_include_xml_source(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_get_data_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {
                '--target-file-list': str,
                '--target-file-column': str
            })
            assert (
                getattr(opt, 'target_file_list')
                == DEFAULT_CONF['train']['source_dataset']['target_file_list']
            )
            assert (
                getattr(opt, 'target_file_column')
                == DEFAULT_CONF['train']['source_dataset']['target_file_column']
            )

        def test_should_include_document_output(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_get_data_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {
                '--document-output-path': str,
                '--document-output-filename-pattern': str
            })
            assert (
                getattr(opt, 'document_output_path')
                == DEFAULT_CONF['train']['grobid']['dataset'] + '/pdf'
            )
            assert (
                getattr(opt, 'document_output_filename_pattern')
                == r'{name}.pdf.gz'
            )

        def test_should_include_xml_output(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_get_data_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {
                '--target-output-path': str,
                '--target-output-filename-pattern': str
            })
            assert (
                getattr(opt, 'target_output_path')
                == DEFAULT_CONF['train']['grobid']['dataset'] + '/xml'
            )
            assert (
                getattr(opt, 'target_output_filename_pattern')
                == r'{document.name}.xml.gz'
            )

        def test_should_include_limit_if_specified(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'train': {
                    **DEFAULT_CONF['train'],
                    'limit': '123'
                }
            }
            rendered_bash_command = _create_and_render_get_data_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--limit': str})
            assert getattr(opt, 'limit') == '123'

        def test_should_not_include_limit_if_not_specified(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_get_data_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--limit': str})
            assert getattr(opt, 'limit') is None

    class TestCreateGenerateTrainingDataOperator:
        def test_should_include_namespace(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_generate_training_data_command(
                dag, airflow_context
            )
            opt = parse_command_arg(rendered_bash_command, {'--namespace': str})
            assert getattr(opt, 'namespace') == DEFAULT_CONF['namespace']

    class TestCreateAutoAnnotateHeaderOperator:
        def test_should_include_namespace(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_auto_annotate_command(
                dag, airflow_context
            )
            opt = parse_command_arg(rendered_bash_command, {'--namespace': str})
            assert getattr(opt, 'namespace') == DEFAULT_CONF['namespace']

        def test_should_include_dataset_paths(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_auto_annotate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {
                '--source-base-path': str,
                '--output-path': str,
                '--xml-path': str
            })
            assert (
                getattr(opt, 'source_base_path')
                == DEFAULT_CONF['train']['grobid']['dataset'] + '/header/corpus/tei-raw'
            )
            assert (
                getattr(opt, 'output_path')
                == DEFAULT_CONF['train']['grobid']['dataset'] + '/header/corpus/tei-auto'
            )
            assert (
                getattr(opt, 'xml_path')
                == DEFAULT_CONF['train']['grobid']['dataset'] + '/xml'
            )

        def test_should_include_xml_filename_regex(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_auto_annotate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--xml-filename-regex': str})
            assert (
                getattr(opt, 'xml_filename_regex')
                == r'/(.*).header.tei.xml/\1.xml/'
            )

        def test_should_include_default_fields(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_auto_annotate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--fields': str})
            assert getattr(opt, 'fields') == DEFAULT_GROBID_TRAIN_FIELDS

        def test_should_include_configured_fields(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'train': {
                    **DEFAULT_CONF['train'],
                    'fields': FIELD_1
                }
            }
            rendered_bash_command = _create_and_render_auto_annotate_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--fields': str})
            assert getattr(opt, 'fields') == FIELD_1

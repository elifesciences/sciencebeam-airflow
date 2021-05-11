import logging
from typing import List

import pytest

from sciencebeam_airflow.dags.utils import get_sciencebeam_image

from dags.sciencebeam_convert import (
    create_dag,
    get_model_sciencebeam_image,
    get_sciencebeam_child_chart_names_for_helm_args,
    create_delete_sciencebeam_op,
    create_deploy_sciencebeam_op,
    create_sciencebeam_convert_op,
    ScienceBeamConvertMacros,
    DEFAULT_WORKER_COUNT,
    DEFAULT_CONVERT_CONTAINER_REQUESTS
)

from .test_utils import (
    parse_command_arg,
    create_and_render_command
)


LOGGER = logging.getLogger(__name__)


NAMESPACE_1 = 'namespace1'

SCIENCEBEAM_IMAGE_REPO_1 = 'sciencebeam_image_repo1'
SCIENCEBEAM_IMAGE_TAG_1 = 'sciencebeam_image_tag1'
SCIENCEBEAM_ARGS_1 = 'sciencebeam_args1'
CERMINE_IMAGE_REPO_1 = 'cermine_image_repo1'
CERMINE_IMAGE_TAG_1 = 'cermine_image_tag1'
GROBID_IMAGE_REPO_1 = 'grobid_image_repo1'
GROBID_IMAGE_TAG_1 = 'grobid_image_tag1'
MODEL_1 = {
    'sciencebeam_image': f'{SCIENCEBEAM_IMAGE_REPO_1}:{SCIENCEBEAM_IMAGE_TAG_1}',
    'sciencebeam_args': SCIENCEBEAM_ARGS_1,
    'grobid_image': f'{GROBID_IMAGE_REPO_1}:{GROBID_IMAGE_TAG_1}'
}
CERMINE_MODEL_1 = {
    'chart_args': {
        'image.repository': SCIENCEBEAM_IMAGE_REPO_1,
        'image.tag': SCIENCEBEAM_IMAGE_TAG_1,
        'sciencebeam.pipeline': 'cermine',
        'grobid.enabled': 'false',
        'cermine.enabled': 'true',
        'cermine.image.repository': CERMINE_IMAGE_REPO_1,
        'cermine.image.tag': CERMINE_IMAGE_TAG_1
    }
}

DEFAULT_CONF = {
    'namespace': NAMESPACE_1,
    'model': MODEL_1,
    'sciencebeam_release_name': 'sb-release1',
    'source_data_path': 'source_data_path1',
    'source_file_list': 'source_file_list1',
    'output_data_path': 'output_data_path1',
    'output_file_list': 'output_file_list1',
    'output_suffix': '.xml.gz',
    'limit': '100'
}

FULL_CHART_NAME = DEFAULT_CONF['sciencebeam_release_name'] + '-sb'


def _create_and_render_delete_command(dag, airflow_context: dict) -> str:
    return create_and_render_command(
        create_delete_sciencebeam_op(dag=dag),
        airflow_context
    )


def _create_and_render_deploy_command(dag, airflow_context: dict) -> str:
    return create_and_render_command(
        create_deploy_sciencebeam_op(dag=dag),
        airflow_context
    )


def _create_and_render_convert_command(dag, airflow_context: dict) -> str:
    return create_and_render_command(
        create_sciencebeam_convert_op(dag=dag),
        airflow_context
    )


def _parse_set_string_list(set_string_list: List[str]) -> dict:
    return dict(
        set_string_line.split('=')
        for set_string_line in set_string_list
    )


class TestScienceBeamConvert:
    class TestCreateDag:
        def test_should_be_able_to_create_dag(self):
            create_dag()

    class TestGetModelScienceBeamImage:
        def test_should_return_model_sciencebeam_image(self):
            assert get_model_sciencebeam_image({
                'sciencebeam_image': 'image1'
            }) == 'image1'

        def test_should_return_default_sciencebeam_image_if_not_specified(self):
            assert get_model_sciencebeam_image({
            }) == get_sciencebeam_image({})

    class TestGetScienceBeamChildChartNamesForHelmArgs:
        def test_should_include_enabled_child_chart(self):
            assert get_sciencebeam_child_chart_names_for_helm_args({
                'child.enabled': 'true'
            }) == ['child']

        def test_should_not_include_disabled_child_chart(self):
            assert get_sciencebeam_child_chart_names_for_helm_args({
                'child.enabled': 'false'
            }) == []

        def test_should_not_include_sub_property_child_chart(self):
            assert get_sciencebeam_child_chart_names_for_helm_args({
                'child.prop.enabled': 'true'
            }) == []

    class TestScienceBeamConvertMacros:
        def test_default_conf_should_be_valid(self):
            assert ScienceBeamConvertMacros().is_config_valid(DEFAULT_CONF)

        def test_should_use_default_worker_count(self):
            assert (
                ScienceBeamConvertMacros().get_worker_count(DEFAULT_CONF)
                == DEFAULT_WORKER_COUNT
            )

        def test_should_use_configured_worker_count(self):
            assert ScienceBeamConvertMacros().get_worker_count({
                **DEFAULT_CONF,
                'config': {
                    'convert': {
                        'worker_count': '123'
                    }
                }
            }) == 123

        def test_should_reject_invalid_worker_count(self):
            with pytest.raises(ValueError):
                ScienceBeamConvertMacros().is_config_valid({
                    **DEFAULT_CONF,
                    'config': {
                        'convert': {
                            'worker_count': 'not a number'
                        }
                    }
                })

    class TestCreateScienceBeamDeleteOp:
        def test_should_include_namespace(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_delete_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--namespace': str})
            assert getattr(opt, 'namespace') == 'namespace1'

    class TestCreateScienceBeamDeployOp:
        def test_should_include_namespace(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_deploy_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--namespace': str})
            assert getattr(opt, 'namespace') == 'namespace1'

        def test_should_include_timeout(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_deploy_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--timeout': str})
            assert getattr(opt, 'timeout') == '600s'

        def test_should_set_string_options_for_default_grobid_deployment(
                self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_deploy_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--set': [str]})
            assert _parse_set_string_list(getattr(opt, 'set')) == {
                'image.repository': SCIENCEBEAM_IMAGE_REPO_1,
                'image.tag': SCIENCEBEAM_IMAGE_TAG_1,
                'sciencebeam.args': SCIENCEBEAM_ARGS_1,
                'grobid.enabled': 'true',
                'grobid.image.repository': GROBID_IMAGE_REPO_1,
                'grobid.image.tag': GROBID_IMAGE_TAG_1,
                'grobid.warmup.enabled': 'true',
                'grobid.crossref.enabled': 'false',
                'fullnameOverride': FULL_CHART_NAME
            }

        def test_should_set_options_for_cermine_deployment(
                self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'model': CERMINE_MODEL_1
            }
            rendered_bash_command = _create_and_render_deploy_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--set': [str]})
            set_string_map = _parse_set_string_list(getattr(opt, 'set'))
            assert set_string_map == {
                **CERMINE_MODEL_1['chart_args'],
                'fullnameOverride': FULL_CHART_NAME
            }

        def test_should_escape_set_options_for_deployment(
                self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'model': {
                    'chart_args': {
                        'extra': 'a,b'
                    }
                }
            }
            rendered_bash_command = _create_and_render_deploy_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--set': [str]})
            set_string_map = _parse_set_string_list(getattr(opt, 'set'))
            assert set_string_map.get('extra') == r'a\,b'

        def test_should_set_replica_count_if_configured(
                self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'config': {
                    'convert': {
                        'replica_count': 42
                    }
                }
            }
            rendered_bash_command = _create_and_render_deploy_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--set': [str]})
            set_props = _parse_set_string_list(getattr(opt, 'set'))
            assert set_props.get('replicaCount') == '42'
            assert set_props.get('grobid.replicaCount') == '42'

        def test_should_only_include_a_single_line(
                self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_deploy_command(dag, airflow_context)
            lines = rendered_bash_command.splitlines()
            LOGGER.info('lines: %s', lines)
            assert len(lines) == 1

    class TestCreateScienceBeamConvertOp:
        def test_should_include_limit(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'limit': 123
            }
            rendered_bash_command = _create_and_render_convert_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--limit': str})
            assert getattr(opt, 'limit') == '123'

        def test_should_include_resume_flag_if_enabled(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'resume': True
            }
            rendered_bash_command = _create_and_render_convert_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--resume': bool})
            assert getattr(opt, 'resume') is True  # pylint: disable=singleton-comparison

        def test_should_not_include_resume_flag_if_disabled(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'resume': False
            }
            rendered_bash_command = _create_and_render_convert_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--resume': bool})
            assert getattr(opt, 'resume') is False  # pylint: disable=singleton-comparison

        def test_should_only_include_a_single_line_with_resume_enabled(
                self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'resume': True
            }
            rendered_bash_command = _create_and_render_convert_command(dag, airflow_context)
            lines = rendered_bash_command.splitlines()
            LOGGER.info('lines: %s', lines)
            assert len(lines) == 1

        def test_should_only_include_a_single_line_with_resume_disabled(
                self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'resume': False
            }
            rendered_bash_command = _create_and_render_convert_command(dag, airflow_context)
            lines = rendered_bash_command.splitlines()
            LOGGER.info('lines: %s', lines)
            assert len(lines) == 1

        def test_should_include_worker_count(self, dag, airflow_context, dag_run):
            dag_run.conf = {
                **DEFAULT_CONF,
                'config': {
                    'convert': {
                        'worker_count': 123
                    }
                }
            }
            rendered_bash_command = _create_and_render_convert_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--num-workers': str})
            assert getattr(opt, 'num_workers') == '123'

        def test_should_use_default_container_requests(self, dag, airflow_context, dag_run):
            dag_run.conf = DEFAULT_CONF
            rendered_bash_command = _create_and_render_convert_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--requests': str})
            assert getattr(opt, 'requests') == DEFAULT_CONVERT_CONTAINER_REQUESTS

        def test_should_be_able_to_override_container_requests(self, dag, airflow_context, dag_run):
            container_requests = 'cpu=123m,memory=123Mi'
            dag_run.conf = {
                **DEFAULT_CONF,
                'config': {
                    'convert': {
                        'container': {
                            'requests': container_requests
                        }
                    }
                }
            }
            rendered_bash_command = _create_and_render_convert_command(dag, airflow_context)
            opt = parse_command_arg(rendered_bash_command, {'--requests': str})
            assert getattr(opt, 'requests') == container_requests

import re

from sciencebeam_airflow.utils.container import (
    KUBECTL_RUN_COMMAND_PREFIX,
    escape_helm_set_value,
    get_helm_delete_command,
    get_container_run_command,
    _get_prefer_preemptible_json,
    _get_select_preemptible_json
)

from ..test_utils import parse_command_arg


class TestEscapeHelmSetValue:
    def test_should_escape_comma(self):
        assert escape_helm_set_value('a,b') == r'a\,b'

    def test_should_convert_int_to_str(self):
        assert escape_helm_set_value(123) == '123'


class TestGetHelmDeleteCommand:
    def test_should_return_helm_uninstall_with_namespace(self):
        assert get_helm_delete_command(
            namespace='namespace1',
            release_name='release1',
            keep_history=False
        ) == 'helm uninstall "release1" --namespace="namespace1"'

    def test_should_add_keey_history_arg(self):
        assert get_helm_delete_command(
            namespace='namespace1',
            release_name='release1',
            keep_history=True
        ) == 'helm uninstall --keep-history "release1" --namespace="namespace1"'


def _normalize_command(command: str) -> str:
    return re.sub(r'\s+', ' ', command)


class TestGetContainerRunCommand:
    def test_should_generate_simle_command(self):
        assert _normalize_command(get_container_run_command(
            namespace='namespace1',
            image='image1',
            name='name1',
            command='command1'
        )) == (
            f'{KUBECTL_RUN_COMMAND_PREFIX}'
            ' --namespace="namespace1"'
            ' --image="image1"'
            ' "name1" -- '
            'command1'
        )

    def test_should_add_requests_if_specified(self):
        command = get_container_run_command(
            namespace='namespace1',
            image='image1',
            name='name1',
            command='command1',
            requests='requests1'
        )
        args = parse_command_arg(command, {'--requests': str})
        assert args.requests == 'requests1'

    def test_should_add_prefer_preemptible(self):
        command = get_container_run_command(
            namespace='namespace1',
            image='image1',
            name='name1',
            command='command1',
            prefer_preemptible=True
        )
        args = parse_command_arg(command, {'--overrides': str})
        assert args.overrides == _get_prefer_preemptible_json()

    def test_should_add_select_preemptible(self):
        command = get_container_run_command(
            namespace='namespace1',
            image='image1',
            name='name1',
            command='command1',
            preemptible=True
        )
        args = parse_command_arg(command, {'--overrides': str})
        assert args.overrides == _get_select_preemptible_json()

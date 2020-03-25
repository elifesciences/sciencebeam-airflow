from sciencebeam_airflow.utils.container import (
    escape_helm_set_value,
    get_helm_delete_command
)


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

import logging
import json
from pathlib import Path
from tempfile import mkdtemp
from shutil import rmtree
from typing import Callable, Dict, List

import yaml


LOGGER = logging.getLogger(__name__)


def _get_preemptible_affinity():
    return {
        "nodeAffinity": {
            "preferredDuringSchedulingIgnoredDuringExecution": [{
                "weight": 1,
                "preference": {
                    "matchExpressions": [{
                        "key": "cloud.google.com/gke-preemptible",
                        "operator": "In",
                        "values": ["true"]
                    }]
                }
            }]
        }
    }


def _get_preemptible_toleration():
    return {
        "key": "cloud.google.com/gke-preemptible",
        "operator": "Equal",
        "value": "true",
        "effect": "NoSchedule"
    }


def _get_preemptible_spec():
    return {
        "affinity": _get_preemptible_affinity(),
        "tolerations": [_get_preemptible_toleration()]
    }


def _get_preemptible_json():
    return json.dumps({
        "spec": _get_preemptible_spec()
    })


def _get_helm_preemptible_values(child_chart_names: List[str] = None) -> dict:
    values = _get_preemptible_spec().copy()
    for child_chart_name in (child_chart_names or []):
        values[child_chart_name] = _get_preemptible_spec()
    return values


def generate_run_name(name: str, suffix: str = '', other_suffix: str = '') -> str:
    name = '-'.join(s for s in [name, suffix, other_suffix] if s)
    return name[:63]


def escape_helm_set_value(helm_value: str) -> str:
    return str(helm_value).replace(',', r'\,')


def format_helm_values_as_set_args(helm_values: Dict[str, str]) -> str:
    return ' '.join([
        '--set "{key}={value}"'.format(key=key, value=escape_helm_set_value(value))
        for key, value in helm_values.items()
    ])


class GeneratedHelmDeployArgs:
    def __init__(  # pylint: disable=too-many-arguments
            self,
            preemptible: bool = False,
            child_chart_names: List[str] = None,
            get_child_chart_names: Callable[[], List[str]] = None):
        self._temp_dir = None
        self.preemptible = preemptible
        self.child_chart_names = child_chart_names or []
        self.get_child_chart_names = get_child_chart_names

    def __enter__(self):
        return self.get_helm_args()

    def __exit__(self, exception_type, value, traceback):
        self.cleanup()

    def _get_temp_dir(self):
        if not self._temp_dir:
            self._temp_dir = Path(mkdtemp(prefix='helm-deploy-'))
        return self._temp_dir

    def cleanup(self):
        if self._temp_dir and self._temp_dir.is_dir():
            rmtree(self._temp_dir)

    def get_helm_args(self) -> str:
        if self.preemptible:
            # we are already in a temporary directory created by BashOperator
            values_file = self._get_temp_dir().joinpath('helm-values.yaml')
            child_chart_names = self.child_chart_names
            if self.get_child_chart_names:
                child_chart_names = self.get_child_chart_names()
            values_file.write_text(yaml.safe_dump(_get_helm_preemptible_values(
                child_chart_names
            )))
            LOGGER.info('helm values (%s): %s', values_file, values_file.read_text())
            return f'--values {values_file.absolute()}'
        return ''


def get_helm_deploy_command(
        namespace: str,
        release_name: str,
        chart_name: str,
        helm_args: str) -> str:
    return (
        '''
        helm upgrade --install --wait "{release_name}" \
            --namespace "{namespace}" \
            {helm_args} \
            {chart_name}
        '''.format(
            namespace=namespace,
            release_name=release_name,
            chart_name=chart_name,
            helm_args=helm_args.strip()
        ).strip()
    )


def get_helm_delete_command(
        namespace: str,
        release_name: str,
        keep_history: bool = False,
        helm_args: str = '') -> str:
    helm_args = helm_args.strip()
    return (
        '''
        helm uninstall{keep_history_arg}{helm_args} "{release_name}" --namespace="{namespace}"
        '''.format(
            namespace=namespace,
            release_name=release_name,
            keep_history_arg=' --keep-history' if keep_history else '',
            helm_args=' ' + helm_args if helm_args else ''
        ).strip()
    )

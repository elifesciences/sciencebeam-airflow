import json
import logging
from pathlib import Path
from tempfile import mkdtemp
from typing import List

import yaml

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

from sciencebeam_airflow.utils.container import (
    get_helm_delete_command
)

from sciencebeam_dag_utils import add_dag_macro


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


def _get_prefer_preemptible_spec():
    return {
        "affinity": _get_preemptible_affinity(),
        "tolerations": [_get_preemptible_toleration()]
    }


def _get_select_preemptible_spec():
    return {
        "nodeSelector": {
            "cloud.google.com/gke-preemptible": "true"
        },
        "tolerations": [_get_preemptible_toleration()]
    }


def _get_prefer_preemptible_json():
    return json.dumps({
        "spec": _get_prefer_preemptible_spec()
    })


def _get_select_preemptible_json():
    return json.dumps({
        "spec": _get_select_preemptible_spec()
    })


def _get_helm_preemptible_values(child_chart_names: List[str] = None) -> dict:
    values = _get_select_preemptible_spec().copy()
    for child_chart_name in (child_chart_names or []):
        values[child_chart_name] = _get_select_preemptible_spec()
    return values


def generate_run_name(name: str, suffix: str = '', other_suffix: str = '') -> str:
    name = '-'.join(s for s in [name, suffix, other_suffix] if s)
    return name[:63]


class ContainerRunOperator(BashOperator):
    def __init__(  # pylint: disable=too-many-arguments
            self,
            dag,
            namespace,
            image,
            name,
            command,
            preemptible: bool = False,
            prefer_preemptible: bool = False,
            requests='',
            **kwargs):
        kubectl_args = ''
        if preemptible:
            kubectl_args = "--overrides '{json}'".format(json=_get_select_preemptible_json())
        elif prefer_preemptible:
            kubectl_args = "--overrides '{json}'".format(json=_get_prefer_preemptible_json())
        if requests:
            kubectl_args += " --requests '{requests}'".format(requests=requests)
        bash_command = (
            '''
            kubectl run --rm --attach --restart=Never --generator=run-pod/v1 \
                --namespace "{namespace}" \
                {kubectl_args} \
                --image={image} \
                "{name}" -- \
            {command}
            '''.format(
                namespace=namespace,
                kubectl_args=kubectl_args,
                image=image,
                name=name,
                command=command.strip()
            )
        ).strip()
        add_dag_macro(dag, 'generate_run_name', generate_run_name)
        super().__init__(dag=dag, bash_command=bash_command, **kwargs)


class HelmDeployOperator(BashOperator):
    def __init__(  # pylint: disable=too-many-arguments
            self,
            dag,
            namespace,
            release_name,
            chart_name,
            helm_args,
            preemptible=False,
            get_child_chart_names=None,
            **kwargs):
        self._temp_dir = None
        add_dag_macro(dag, 'get_helm_args', self.get_helm_args)
        self.preemptible = preemptible
        self.get_child_chart_names = get_child_chart_names
        bash_command = (
            '''
            helm upgrade --install --wait "{release_name}" \
                --namespace "{namespace}" \
                {get_helm_args} \
                {helm_args} \
                {chart_name}
            '''.format(
                namespace=namespace,
                release_name=release_name,
                chart_name=chart_name,
                get_helm_args='{{ get_helm_args(dag_run) }}',
                helm_args=helm_args.strip()
            ).strip()
        )
        super().__init__(dag=dag, bash_command=bash_command, **kwargs)

    def _get_temp_dir(self):
        if not self._temp_dir:
            self._temp_dir = Path(mkdtemp(prefix='helm-deploy-'))
        return self._temp_dir

    def _cleanup(self):
        if self._temp_dir and self._temp_dir.is_dir():
            self._temp_dir.rmdir()

    def get_helm_args(self, dag_run):
        if self.preemptible:
            # we are already in a temporary directory created by BashOperator
            values_file = self._get_temp_dir().joinpath('helm-values.yaml')
            child_chart_names = []
            if self.get_child_chart_names:
                child_chart_names = self.get_child_chart_names(dag_run=dag_run)
            values_file.write_text(yaml.safe_dump(_get_helm_preemptible_values(
                child_chart_names
            )))
            LOGGER.info('helm values (%s): %s', values_file, values_file.read_text())
            return f'--values {values_file.absolute()}'
        return ''

    def post_execute(self, *args, **kwargs):  # pylint: disable=arguments-differ
        self._cleanup()
        super().post_execute(*args, **kwargs)

    def on_kill(self, *args, **kwargs):  # pylint: disable=arguments-differ
        self._cleanup()
        super().on_kill(*args, **kwargs)


class HelmDeleteOperator(BashOperator):
    def __init__(  # pylint: disable=too-many-arguments
            self,
            dag: DAG,
            namespace: str,
            release_name: str,
            keep_history: bool = False,
            **kwargs):
        bash_command = get_helm_delete_command(
            namespace=namespace,
            release_name=release_name,
            keep_history=keep_history
        )
        super().__init__(dag=dag, bash_command=bash_command, **kwargs)

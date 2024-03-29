import logging
from pathlib import Path
from tempfile import mkdtemp
from typing import Callable, Dict, Optional, Union

import yaml

from airflow.models import DAG
from airflow.operators.bash import BashOperator

from sciencebeam_airflow.utils.container import (
    _get_helm_select_preemptible_values,
    get_helm_delete_command,
    get_container_run_command
)

from sciencebeam_airflow.utils.airflow import add_dag_macro


LOGGER = logging.getLogger(__name__)


def generate_run_name(name: str, suffix: str = '', other_suffix: str = '') -> str:
    name = '-'.join(s for s in [name, suffix, other_suffix] if s)
    return name[:63]


class ContainerRunOperator(BashOperator):
    def __init__(  # pylint: disable=too-many-arguments
            self,
            dag,
            namespace: str,
            image: str,
            name: str,
            command: str,
            preemptible: Union[str, bool] = False,
            prefer_preemptible: Union[str, bool] = False,
            highcpu: Union[str, bool] = False,
            requests='',
            container_overrides_fn: Optional[Callable[[dict], dict]] = None,
            **kwargs):
        add_dag_macro(dag, 'get_container_run_command', self.get_container_run_command)
        add_dag_macro(dag, 'generate_run_name', generate_run_name)
        self.container_args = dict(
            namespace=namespace,
            image=image,
            name=name,
            command=command,
            preemptible=preemptible,
            prefer_preemptible=prefer_preemptible,
            highcpu=highcpu,
            requests=requests
        )
        self.container_overrides: Optional[dict] = None
        self.container_overrides_fn = container_overrides_fn
        bash_command = '{{ get_container_run_command() }}'
        super().__init__(dag=dag, bash_command=bash_command, **kwargs)
        # Note: we are processing the command separately
        self.template_fields = ('env',)

    def fix_boolean_container_args(self):
        # currently render template is converting the booleans to a string
        for name in ['preemptible', 'prefer_preemptible', 'highcpu']:
            value = self.container_args[name]
            if isinstance(value, str):
                self.container_args[name] = (value.lower() == 'true')

    def render_template_fields(self, context: Dict, *args, **kwargs) -> None:
        LOGGER.info('render_template_fields: self=%r', id(self))
        LOGGER.info(
            'render_template_fields: template_fields=%r, context=%r',
            self.template_fields, context
        )
        self.container_args = self.render_template(self.container_args, context)
        if self.container_overrides_fn:
            dag_run = context['dag_run']
            self.container_overrides = self.container_overrides_fn(dag_run.conf)
        # Note: using a bash command template, seem to result in a separate instance of this class
        self.bash_command = self.get_container_run_command()
        LOGGER.info(
            'render_template_fields: rendered container_args: %r', self.container_args
        )
        LOGGER.info(
            'render_template_fields: rendered container_overrides: %r', self.container_overrides
        )
        super().render_template_fields(context, *args, **kwargs)
        LOGGER.info(
            'render_template_fields: rendered bash_command: %r', self.bash_command
        )

    def get_container_run_command(self):
        self.fix_boolean_container_args()
        LOGGER.info('get_container_run_command: self=%r', id(self))
        LOGGER.info(
            'get_container_run_command: container_args: %r', self.container_args
        )
        LOGGER.info(
            'get_container_run_command: container_overrides: %r', self.container_overrides
        )
        return get_container_run_command(
            **{
                **self.container_args,
                **(self.container_overrides or {})
            }
        )


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
            values_file.write_text(yaml.safe_dump(_get_helm_select_preemptible_values(
                child_chart_names
            )))
            LOGGER.info('helm values (%s): %s', values_file, values_file.read_text())
            return f'--values {values_file.absolute()}'
        return ''

    def post_execute(self, *args, **kwargs):  # pylint: disable=arguments-differ, signature-differs
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

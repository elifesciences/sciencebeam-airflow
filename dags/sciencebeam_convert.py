import logging
import os
from typing import Dict, List, Optional

from airflow.operators.bash import BashOperator
from airflow.models import DAG, DagRun

from sciencebeam_airflow.utils.container import escape_helm_set_value
from sciencebeam_airflow.utils.airflow import add_dag_macros

from sciencebeam_airflow.utils.container_operators import (
    ContainerRunOperator,
    HelmDeployOperator,
    HelmDeleteOperator
)

from sciencebeam_airflow.dags.dag_ids import ScienceBeamDagIds

from sciencebeam_airflow.dags.utils import (
    get_default_args,
    create_validate_config_operation,
    create_trigger_next_task_dag_operator,
    get_sciencebeam_image
)


LOGGER = logging.getLogger(__name__)


class ConfigProps:
    SCIENCEBEAM_RELEASE_NAME = 'sciencebeam_release_name'
    MODEL = 'model'
    NAMESPACE = 'namespace'
    SOURCE_DATA_PATH = 'source_data_path'
    SOURCE_FILE_LIST = 'source_file_list'
    OUTPUT_DATA_PATH = 'output_data_path'
    OUTPUT_FILE_LIST = 'output_file_list'
    OUTPUT_SUFFIX = 'output_suffix'
    RESUME = 'resume'
    LIMIT = 'limit'


REQUIRED_PROPS = {
    ConfigProps.SCIENCEBEAM_RELEASE_NAME,
    ConfigProps.NAMESPACE,
    ConfigProps.LIMIT
}


DEFAULT_ARGS = get_default_args()


DEFAULT_WORKER_COUNT = 10
DEFAULT_REPLICA_COUNT = 0  # don't set replica by default


DEPLOY_SCIENCEBEAM_ARGS_TEMPLATE = (
    '''
    --timeout 600s \
    --set "fullnameOverride={{ dag_run.conf.sciencebeam_release_name }}-sb" \
    {% for key, value in get_sciencebeam_deploy_args(dag_run.conf).items() %} \
        --set "{{ key }}={{ escape_helm_set_value(value) }}" \
    {% endfor %}
    '''
)


SCIENCEBEAM_CONVERT_TEMPLATE = (
    '''
    python -m sciencebeam.pipeline_runners.local_pipeline_runner \
        --data-path "{{ get_source_conf(dag_run.conf).data_path }}" \
        --source-file-list "{{ get_source_conf(dag_run.conf).file_list }}" \
        --source-file-column "{{ get_source_conf(dag_run.conf).file_column }}" \
        --output-path "{{ get_output_conf(dag_run.conf).data_path }}" \
        --output-suffix "{{ get_output_conf(dag_run.conf).output_suffix }}" \
        --pipeline=api \
        --api-url=http://{{ dag_run.conf.sciencebeam_release_name }}-sb:8075/api/convert \
        {% if dag_run.conf.resume | default(false) %} \
            --resume \
        {% endif %} \
        --limit "{{ get_limit(dag_run.conf) }}" \
        --num-workers "{{ get_worker_count(dag_run.conf) }}"
    '''
)


SCIENCEBEAM_GET_OUTPUT_FILE_LIST_TEMPLATE = (
    '''
    python -m sciencebeam_utils.tools.get_output_files \
        --source-base-path "{{ get_source_conf(dag_run.conf).data_path }}" \
        --source-file-list "{{ get_source_conf(dag_run.conf).file_list }}" \
        --source-file-column "{{ get_source_conf(dag_run.conf).file_column }}" \
        --output-file-list "{{ get_output_conf(dag_run.conf).absolute_file_list }}" \
        --output-file-suffix "{{ get_output_conf(dag_run.conf).output_suffix }}" \
        --output-base-path "{{ get_output_conf(dag_run.conf).data_path }}" \
        --use-relative-path \
        --limit "{{ get_limit(dag_run.conf) }}" \
        --check
    '''
)

DEFAULT_CONVERT_CONTAINER_REQUESTS = 'cpu=500m,memory=2048Mi'


def parse_image_name_tag(image):
    return image.split(':')


def get_model_sciencebeam_image(model: dict) -> dict:
    return get_sciencebeam_image(model)


def get_model_sciencebeam_deploy_args(model: dict) -> dict:
    if 'chart_args' in model:
        return model['chart_args']
    sciencebeam_image_repo, sciencebeam_image_tag = parse_image_name_tag(
        get_model_sciencebeam_image(model)
    )
    grobid_image_repo, grobid_image_tag = parse_image_name_tag(
        model['grobid_image']
    )
    return {
        'image.repository': sciencebeam_image_repo,
        'image.tag': sciencebeam_image_tag,
        'sciencebeam.args': model.get('sciencebeam_args', ''),
        'grobid.enabled': 'true',
        'grobid.image.repository': grobid_image_repo,
        'grobid.image.tag': grobid_image_tag,
        'grobid.warmup.enabled': 'true',
        'grobid.crossref.enabled': model.get('grobid_crossref_enabled', 'false')
    }


def get_sciencebeam_child_chart_names_for_helm_args(helm_args: Dict[str, str]) -> List[str]:
    return [
        key.split('.')[0]
        for key, value in helm_args.items()
        if key.endswith('.enabled') and len(key.split('.')) == 2 and value == 'true'
    ]


class ScienceBeamConvertMacros:
    def get_model(self, conf: dict) -> dict:
        return conf['model']

    def get_source_conf(self, conf: dict) -> dict:
        return {
            'file_column': 'source_url',
            'data_path': conf['source_data_path'],
            'file_list': conf['source_file_list'],
            'absolute_file_list': os.path.join(conf['source_data_path'], conf['source_file_list'])
        }

    def get_output_conf(self, conf: dict) -> dict:
        return {
            'output_suffix': conf['output_suffix'],
            'data_path': conf['output_data_path'],
            'file_list': conf['output_file_list'],
            'absolute_file_list': os.path.join(conf['output_data_path'], conf['output_file_list'])
        }

    def get_limit(self, conf: dict) -> str:
        return conf['limit']

    def get_convert_config(self, conf: dict) -> dict:
        return conf.get('config', {}).get('convert', {})

    def get_sciencebeam_convert_container_kwargs(self, conf: dict) -> dict:
        return self.get_convert_config(conf).get('container', {})

    def get_worker_count(self, conf: dict) -> str:
        return int(self.get_convert_config(conf).get('worker_count', DEFAULT_WORKER_COUNT))

    def get_replica_count(self, conf: dict) -> str:
        return int(self.get_convert_config(conf).get('replica_count', DEFAULT_REPLICA_COUNT))

    def get_base_sciencebeam_deploy_args(self, conf: dict) -> dict:
        return get_model_sciencebeam_deploy_args(self.get_model(conf))

    def get_sciencebeam_deploy_args(self, conf: dict) -> dict:
        LOGGER.debug('conf: %s', conf)
        helm_args = self.get_base_sciencebeam_deploy_args(conf)
        replica_count = self.get_replica_count(conf)
        if replica_count:
            child_chart_names = list(get_sciencebeam_child_chart_names_for_helm_args(helm_args))
            helm_args['replicaCount'] = replica_count
            for child_chart_name in child_chart_names:
                helm_args['%s.replicaCount' % child_chart_name] = replica_count
        return helm_args

    def escape_helm_set_value(self, helm_value: str) -> str:
        return escape_helm_set_value(helm_value)

    def get_sciencebeam_child_chart_names(
            self, dag_run: DagRun, **_) -> List[str]:
        conf: dict = dag_run.conf
        helm_args = self.get_base_sciencebeam_deploy_args(conf)
        return get_sciencebeam_child_chart_names_for_helm_args(helm_args)

    def get_sciencebeam_image(self, conf: dict) -> str:
        return get_sciencebeam_image(conf)

    def is_config_valid(self, conf: dict) -> bool:
        return (
            self.get_model(conf)
            and self.get_source_conf(conf)
            and self.get_output_conf(conf)
            and self.get_limit(conf)
            and self.get_worker_count(conf)
            and True
        )


def add_sciencebeam_convert_dag_macros(
    dag: DAG,
    macros: Optional[ScienceBeamConvertMacros] = None
) -> ScienceBeamConvertMacros:
    if macros is None:
        macros = ScienceBeamConvertMacros()
    add_dag_macros(dag, macros)
    return macros


def create_deploy_sciencebeam_op(
        dag: DAG, macros: ScienceBeamConvertMacros = None,
        task_id='deploy_sciencebeam'):
    if macros is None:
        macros = ScienceBeamConvertMacros()
    add_sciencebeam_convert_dag_macros(dag, macros)
    return HelmDeployOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        release_name='{{ dag_run.conf.sciencebeam_release_name }}',
        chart_name='$HELM_CHARTS_DIR/sciencebeam',
        get_child_chart_names=macros.get_sciencebeam_child_chart_names,
        preemptible=True,
        helm_args=DEPLOY_SCIENCEBEAM_ARGS_TEMPLATE
    )


def create_delete_sciencebeam_op(dag, task_id='delete_sciencebeam'):
    return HelmDeleteOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        release_name='{{ dag_run.conf.sciencebeam_release_name }}',
        keep_history=False,
        trigger_rule='all_done'
    )


def create_sciencebeam_convert_op(
        dag, macros: ScienceBeamConvertMacros = None,
        task_id='sciencebeam_convert') -> BashOperator:
    _macros = add_sciencebeam_convert_dag_macros(dag, macros)
    return ContainerRunOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        image='{{ get_sciencebeam_image(dag_run.conf) }}',
        name='{{ generate_run_name(dag_run.conf.sciencebeam_release_name, "convert") }}',
        preemptible=True,
        requests=DEFAULT_CONVERT_CONTAINER_REQUESTS,
        container_overrides_fn=_macros.get_sciencebeam_convert_container_kwargs,
        command=SCIENCEBEAM_CONVERT_TEMPLATE
    )


def create_get_output_file_list_op(
        dag, macros: ScienceBeamConvertMacros = None, task_id='get_output_file_list'):
    add_sciencebeam_convert_dag_macros(dag, macros)
    return ContainerRunOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        image='{{ get_sciencebeam_image(dag_run.conf) }}',
        name='{{ generate_run_name(dag_run.conf.sciencebeam_release_name, "get-output-list") }}',
        preemptible=True,
        requests='cpu=100m,memory=256Mi',
        command=SCIENCEBEAM_GET_OUTPUT_FILE_LIST_TEMPLATE,
    )


def create_dag(
        dag_id: str = ScienceBeamDagIds.SCIENCEBEAM_CONVERT,
        default_args: dict = None,
        schedule_interval=None,
        macros: ScienceBeamConvertMacros = None,
        trigger_next: bool = True):
    if default_args is None:
        default_args = DEFAULT_ARGS
    if macros is None:
        macros = ScienceBeamConvertMacros()

    dag = DAG(dag_id=dag_id, default_args=default_args, schedule_interval=schedule_interval)

    convert_results = (
        create_validate_config_operation(
            dag=dag, required_props=REQUIRED_PROPS,
            is_config_valid=macros.is_config_valid
        ) >> create_deploy_sciencebeam_op(dag=dag, macros=macros)
        >> create_sciencebeam_convert_op(dag=dag, macros=macros)
    )

    _ = convert_results >> create_delete_sciencebeam_op(dag=dag)

    get_output_file_list_results = (
        convert_results
        >> create_get_output_file_list_op(dag=dag, macros=macros)
    )

    if trigger_next:
        _ = get_output_file_list_results >> create_trigger_next_task_dag_operator(dag=dag)

    return dag


MAIN_DAG = create_dag()

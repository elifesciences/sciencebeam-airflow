import argparse
import logging
import json
from typing import Dict, List

from sciencebeam_airflow.utils.io import (
    get_file_content,
    STANDALONE_GOOGLE_CLOUD_CONNECTION_ID
)
from sciencebeam_airflow.utils.container import (
    GeneratedHelmDeployArgs,
    get_helm_deploy_command,
    get_helm_delete_command
)
from sciencebeam_airflow.utils.subprocess import run_command
from sciencebeam_airflow.utils.sciencebeam_env import get_namespace


LOGGER = logging.getLogger(__name__)


def parse_image_name_tag(image):
    return image.split(':')


def get_base_model_sciencebeam_deploy_args(model: dict) -> Dict[str, str]:
    if 'chart_args' in model:
        return model['chart_args']
    sciencebeam_image_repo, sciencebeam_image_tag = parse_image_name_tag(
        model['sciencebeam_image']
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


def add_replica_helm_args(helm_args: Dict[str, str], replica_count: int) -> Dict[str, str]:
    if not replica_count:
        return helm_args
    helm_args = helm_args.copy()
    child_chart_names = list(get_sciencebeam_child_chart_names_for_helm_args(helm_args))
    helm_args['replicaCount'] = replica_count
    for child_chart_name in child_chart_names:
        helm_args['%s.replicaCount' % child_chart_name] = replica_count
    return helm_args


def get_model_sciencebeam_deploy_args(
        model: dict, replica_count: int, timeout: int) -> Dict[str, str]:
    helm_args = get_base_model_sciencebeam_deploy_args(model)
    helm_args = add_replica_helm_args(helm_args, replica_count=replica_count)
    if timeout:
        helm_args['timeout'] = str(timeout) + 's'
    return helm_args


def parse_args(argv: List[str] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy ScienceBeam Model"
    )
    parser.add_argument(
        "--model-url",
        type=str,
        required=True,
        help="URL to model config"
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default=get_namespace(),
        help="The namespace to use"
    )
    parser.add_argument(
        "--release-name",
        type=str,
        default='sb-adhoc-1',
        help="The helm release name"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="Helm deploy timeout in seconds"
    )
    parser.add_argument(
        "--replica-count",
        type=int,
        default=0,
        help="Number of replicas (0, for keeping replica count from model definition)"
    )
    parser.add_argument(
        "--delete",
        action='store_true',
        help="Rather than deploying, delete helm chart"
    )
    args = parser.parse_args(argv)
    return args


def _load_model_config(model_url: str) -> dict:
    LOGGER.info('loading model config: %s', model_url)
    model_config = json.loads(get_file_content(
        model_url,
        google_cloud_storage_conn_id=STANDALONE_GOOGLE_CLOUD_CONNECTION_ID
    ))
    LOGGER.info('model config: %s', model_config)
    return model_config


def format_helm_args(helm_args: Dict[str, str]) -> str:
    return ' '.join([
        '--set "{key}={value}"'.format(key=key, value=value)
        for key, value in helm_args.items()
    ])



def run(args: argparse.Namespace):
    model_config = _load_model_config(args.model_url)
    helm_args = get_model_sciencebeam_deploy_args(
        model_config, replica_count=args.replica_count, timeout=args.timeout
    )
    LOGGER.info('helm args: %s', helm_args)
    child_chart_names = get_sciencebeam_child_chart_names_for_helm_args(helm_args)
    with GeneratedHelmDeployArgs(
            preemptible=True,
            child_chart_names=child_chart_names) as generated_helm_args:
        LOGGER.info('generated_helm_args: %s', generated_helm_args)
        release_name = args.release_name
        if args.delete:
            command = get_helm_delete_command(
                namespace=args.namespace,
                release_name=release_name,
                keep_history=False
            )
        else:
            command = get_helm_deploy_command(
                namespace=args.namespace,
                release_name=release_name,
                chart_name='$HELM_CHARTS_DIR/sciencebeam',
                helm_args=' '.join([generated_helm_args, format_helm_args(helm_args)])
            )
        run_command(command, shell=True)


def main(argv: List[str] = None):
    args = parse_args(argv)
    LOGGER.info('main, args=%s', args)
    run(args)


if __name__ == '__main__':
    logging.basicConfig(level='INFO')

    main()

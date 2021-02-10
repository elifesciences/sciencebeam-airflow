import logging
from urllib.parse import urlparse

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Connection


LOGGER = logging.getLogger(__name__)


DEFAULT_GOOGLE_CLOUD_CONNECTION_ID = 'google_cloud_default'
STANDALONE_GOOGLE_CLOUD_CONNECTION_ID = 'standalone'


def parse_gs_url(url):
    parsed_url = urlparse(url)
    if parsed_url.scheme != 'gs':
        raise AssertionError('expected gs:// url, but got: %s' % url)
    if not parsed_url.hostname:
        raise AssertionError('url is missing bucket / hostname: %s' % url)
    return {
        'bucket': parsed_url.hostname,
        'object': parsed_url.path.lstrip('/')
    }


class StandaloneGoogleCloudStorageHook(GCSHook):  # pylint: disable=abstract-method
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_connection(self, conn_id):  # type: (str) -> Connection
        if conn_id != STANDALONE_GOOGLE_CLOUD_CONNECTION_ID:
            return super().get_connection(self)
        # using standalone connection, not requiring the Airflow database
        connection = Connection(conn_id=conn_id)
        return connection


def get_gs_hook(
        google_cloud_storage_conn_id=DEFAULT_GOOGLE_CLOUD_CONNECTION_ID,
        delegate_to=None):
    return StandaloneGoogleCloudStorageHook(
        google_cloud_storage_conn_id=google_cloud_storage_conn_id,
        delegate_to=delegate_to
    )


def download_file(url, filename, **kwargs):
    LOGGER.info('downloading: %s', url)
    parsed_url = parse_gs_url(url)
    return get_gs_hook(**kwargs).download(
        bucket=parsed_url['bucket'],
        object=parsed_url['object'],
        filename=filename
    )


def get_file_content(url, **kwargs):
    return download_file(url, filename=None, **kwargs)

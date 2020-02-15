from dags.sciencebeam_dag_utils import get_sciencebeam_image

from sciencebeam_airflow.tools.deploy_sciencebeam import (
    get_model_sciencebeam_image,
    get_model_sciencebeam_deploy_args
)

class TestGetModelScienceBeamImage:
    def test_should_return_model_sciencebeam_image(self):
        assert get_model_sciencebeam_image({
            'sciencebeam_image': 'image1'
        }) == 'image1'

    def test_should_return_default_sciencebeam_image_if_not_specified(self):
        assert get_model_sciencebeam_image({
        }) == get_sciencebeam_image({})


class TestGetModelScienceBeamDeployArgs:
    def test_should_format_timeout(self):
        args = get_model_sciencebeam_deploy_args({
            'sciencebeam_image': 'sciencebeam_repo1:tag1',
            'grobid_image': 'grobid_repo1:tag1'
        }, replica_count=1, timeout=123)
        assert args['timeout'] == '123s'

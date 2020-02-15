from sciencebeam_airflow.tools.deploy_sciencebeam import (
    get_model_sciencebeam_deploy_args
)


class TestGetModelScienceBeamDeployArgs:
    def test_should_format_timeout(self):
        args = get_model_sciencebeam_deploy_args({
            'sciencebeam_image': 'sciencebeam_repo1:tag1',
            'grobid_image': 'grobid_repo1:tag1'
        }, replica_count=1, timeout=123)
        assert args['timeout'] == '123s'

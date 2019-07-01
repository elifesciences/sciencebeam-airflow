from dags.sciencebeam_autocut_build_image import (
    create_dag
)


class TestScienceBeamAutocutBuildImage:
    class TestCreateDag:
        def test_should_be_able_to_create_dag(self):
            assert create_dag()

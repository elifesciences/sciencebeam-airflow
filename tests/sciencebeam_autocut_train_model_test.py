from dags.sciencebeam_autocut_train_model import (
    create_dag
)


class TestScienceBeamAutocutTrainModel:
    class TestCreateDag:
        def test_should_be_able_to_create_dag(self):
            assert create_dag()

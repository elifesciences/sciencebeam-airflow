from dags.kube_list_pods import create_dag


class TestKubeListPods:
    def test_should_be_able_to_create_dag(self):
        create_dag()

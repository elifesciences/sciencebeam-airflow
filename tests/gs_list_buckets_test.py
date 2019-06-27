from dags.gs_list_buckets import create_dag


class TestGsListBuckets:
    def test_should_be_able_to_create_dag(self):
        create_dag()

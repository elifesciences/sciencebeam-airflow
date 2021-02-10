import airflow
from airflow.operators.python import PythonOperator
from airflow.models import DAG

from gcloud import storage


DEFAULT_ARGS = {
    'start_date': airflow.utils.dates.days_ago(2)
}


def gs_list_buckets(*_, **__):
    client = storage.Client()
    items = list(client.list_buckets())
    print('buckets:', items)
    return 'buckets: %s' % (items)


def create_dag():
    dag = DAG(
        dag_id='gs_list_buckets', default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    PythonOperator(
        task_id='gs_list_buckets',
        python_callable=gs_list_buckets,
        dag=dag)

    return dag


MAIN_DAG = create_dag()

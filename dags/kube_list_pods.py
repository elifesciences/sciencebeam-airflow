import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

import kubernetes


DEFAULT_ARGS = {
    'start_date': airflow.utils.dates.days_ago(2)
}


def kube_list_pods(**_):
    kubernetes.config.load_kube_config(persist_config=False)

    client = kubernetes.client.CoreV1Api()
    print("Listing pods with their IPs:")
    ret = client.list_pod_for_all_namespaces(watch=False)
    for i in ret.items:
        print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))


def create_dag():
    dag = DAG(
        dag_id='kube_list_pods', default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    PythonOperator(
        task_id='kube_list_pods',
        provide_context=False,
        python_callable=kube_list_pods,
        dag=dag)

    return dag


MAIN_DAG = create_dag()

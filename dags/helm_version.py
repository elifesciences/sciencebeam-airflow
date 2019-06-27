import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG


DEFAULT_ARGS = {
    'start_date': airflow.utils.dates.days_ago(2)
}


def create_dag():
    dag = DAG(
        dag_id='helm_version', default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    BashOperator(
        task_id='helm_version',
        bash_command='helm version',
        dag=dag)

    return dag


MAIN_DAG = create_dag()

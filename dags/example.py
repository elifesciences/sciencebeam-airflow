# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import time
from pprint import pprint

import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG


DEFAULT_ARGS = {
    'start_date': airflow.utils.dates.days_ago(2)
}


def create_dag():

    dag = DAG(
        dag_id='example_python_operator', default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    def print_context(**kwargs):
        pprint(kwargs)
        return 'Whatever you return gets printed in the logs'

    run_this = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
        dag=dag)

    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(random_base)

    for i in range(5):
        task = PythonOperator(
            task_id='sleep_for_' + str(i),
            python_callable=my_sleeping_function,
            op_kwargs={'random_base': float(i) / 10},
            dag=dag)

        task.set_upstream(run_this)

    return dag


MAIN_DAG = create_dag()

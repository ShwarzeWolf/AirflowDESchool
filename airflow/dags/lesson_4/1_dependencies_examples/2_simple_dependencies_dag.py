from time import sleep

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _sleep():
    sleep(5)


with DAG(
    dag_id='2_simple_dependencies_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson4']
) as dag:
    upstream_op = PythonOperator(
        task_id='start',
        python_callable=_sleep
    )

    downstream_op = PythonOperator(
        task_id='finish',
        python_callable=_sleep
    )

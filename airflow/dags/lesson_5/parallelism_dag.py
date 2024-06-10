import time

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

def _sleep():
    time.sleep(10)

with DAG(
    dag_id='parallelism_demo_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson5'],
    max_active_tasks=4,
    max_active_runs=1
) as dag:
    task_1_op = PythonOperator(
        task_id='task_1',
        python_callable=_sleep,
        pool='api_call_pool',
        priority_weight=1
    )
    task_2_op = PythonOperator(
        task_id='task_2',
        python_callable=_sleep,
        pool='api_call_pool',
        priority_weight=2
    )
    task_3_op = PythonOperator(
        task_id='task_3',
        python_callable=_sleep,
        priority_weight=1
    )
    task_4_op = PythonOperator(
        task_id='task_4',
        python_callable=_sleep
    )
    # task_5_op = PythonOperator(
    #     task_id='task_5',
    #     python_callable=_sleep
    # )
    # task_6_op = PythonOperator(
    #     task_id='task_6',
    #     python_callable=_sleep
    # )
import random

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor

def _random_condition():
    random_value = random.choice([True, False])
    return random_value



with DAG(
    dag_id='3_sensor_demo_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson2']
) as dag:
    start_op = EmptyOperator(task_id='start')

    python_sensor_op = PythonSensor(
        task_id='wait',
        python_callable=_random_condition,
        poke_interval=60,
        timeout=7*24*60*60
    )

    finish_op = EmptyOperator(task_id='finish')

    start_op >> python_sensor_op >> finish_op

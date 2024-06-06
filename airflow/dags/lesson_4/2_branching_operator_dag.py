import random

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


def _choose_api():
    available_apis = ['fetch_json', 'fetch_csv']
    return random.choice(available_apis)


with DAG(
    dag_id='branching_operator_demo_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson4']
) as dag:
    start_op = EmptyOperator(task_id='start')

    choose_api_op = BranchPythonOperator(
        task_id='choose_api',
        python_callable=_choose_api
    )

    fetch_json_data_op = EmptyOperator(task_id='fetch_json')
    fetch_csv_data_op = EmptyOperator(task_id='fetch_csv')

    finish_op = EmptyOperator(
        task_id='finish',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    start_op >> choose_api_op
    choose_api_op >> fetch_json_data_op >> finish_op
    choose_api_op >> fetch_csv_data_op >> finish_op

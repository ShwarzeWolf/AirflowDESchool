import json

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator

with DAG(
    dag_id='1_http_demo_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson2']
) as dag:
    start_op = EmptyOperator(task_id='start')

    launch_processing_op = HttpOperator(
        task_id='launch_processing',
        http_conn_id='webserver_conn_id',
        endpoint='datasets',
        method='POST',
        data=json.dumps({'dataset_id': 1}),
        headers={'Content-Type': 'application/json'},
        response_check=lambda response: response.status_code == 200
    )

    finish_op = EmptyOperator(task_id='finish')

    start_op >> launch_processing_op >> finish_op

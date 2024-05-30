import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id='0_bash_demo_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson2']
) as dag:
    start_op = EmptyOperator(task_id='start')

    process_zones_op = BashOperator(
        task_id='process_zones',
        bash_command='process_zones.sh',
        env={'message': 'Starting a new DAG'}
    )

    finish_op = EmptyOperator(task_id='finish')

    start_op >> process_zones_op >> finish_op

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain


with DAG(
    dag_id='5_dependency_functions_not_working_chain_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson4']
) as dag:
    start_op = EmptyOperator(task_id='start')
    transform_1_op = EmptyOperator(task_id='transform_1')
    transform_2_op = EmptyOperator(task_id='transform_2')
    transform_3_op = EmptyOperator(task_id='transform_3')
    check_op = EmptyOperator(task_id='check')
    insert_1_op = EmptyOperator(task_id='insert_1')
    insert_2_op = EmptyOperator(task_id='insert_2')
    finish_op = EmptyOperator(task_id='finish')

    chain(start_op, [transform_1_op, transform_2_op, transform_3_op], check_op, [insert_1_op, insert_2_op], finish_op)

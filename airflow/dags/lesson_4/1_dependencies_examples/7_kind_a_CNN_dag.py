import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain_linear
from airflow.utils.helpers import cross_downstream

with DAG(
    dag_id='7_linear_chain_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson4']
) as dag:
    start_op = EmptyOperator(task_id='start')
    transform_1_op = EmptyOperator(task_id='transform_1')
    transform_2_op = EmptyOperator(task_id='transform_2')
    transform_3_op = EmptyOperator(task_id='transform_3')
    insert_1_op = EmptyOperator(task_id='insert_1')
    insert_2_op = EmptyOperator(task_id='insert_2')
    finish_op = EmptyOperator(task_id='finish')

    chain_linear(
        start_op,
        [transform_1_op, transform_2_op, transform_3_op],
        [insert_1_op, insert_2_op],
        finish_op
    )

    cross_downstream(
        from_tasks=[transform_1_op, transform_2_op, transform_3_op],
        to_tasks=[insert_1_op, insert_2_op],
    )

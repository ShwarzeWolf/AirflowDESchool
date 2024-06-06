import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator



with DAG(
    dag_id='3_many_streams_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson4']
) as dag:
    start_op = EmptyOperator(task_id='start')
    downstream_1_op = EmptyOperator(task_id='transform_1')
    downstream_2_op = EmptyOperator(task_id='transform_2')
    finish_op = EmptyOperator(task_id='finish')

    start_op >> [downstream_1_op, downstream_2_op] >> finish_op

    # start_op.set_downstream(downstream_1_op)
    # start_op.set_downstream(downstream_2_op)
    # downstream_1_op.set_downstream(finish_op)
    # downstream_2_op.set_downstream(finish_op)
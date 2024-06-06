import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain


with DAG(
    dag_id='6_meaningful_chain_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson4']
) as dag:
    start_op = EmptyOperator(task_id='start')

    tasks_list = [start_op]
    for op in ('transform', 'insert'):
        _tasks_list = []
        for i in range(2):
            op_i = EmptyOperator(task_id=f"{op}_{i}")
            _tasks_list.append(op_i)

        tasks_list.append(_tasks_list)

    finish_op = EmptyOperator(task_id='finish')
    tasks_list.append(finish_op)

    chain(*tasks_list)

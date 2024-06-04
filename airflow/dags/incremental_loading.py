import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def _copy_images(device_id, data_interval_start):
    current_month = data_interval_start.strftime('%Y_%m')
    print(f'processing for {current_month} for device {device_id} has started')

with DAG(
    dag_id='incremental_processing_demo_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson3']
) as dag:
    start_op = EmptyOperator(task_id='start')

    some_bash_op = BashOperator(
        task_id='bash_demo',
        bash_command='echo Today is {{ execution_date.format("dddd") }}'
                     '{{ var.value.biogrid_url }} {{ task_instance.xcom_pull(task_ids="start", key="some_key")}}'
    )

    process_zones_op = PythonOperator(
        task_id='process_zones',
        python_callable=_copy_images,
        op_kwargs={'device_id': '1'},
    )

    finish_op = EmptyOperator(task_id='finish')

    start_op >> some_bash_op >> process_zones_op >> finish_op

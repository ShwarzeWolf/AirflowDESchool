from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum


with DAG(
    dag_id='biogrid_loading_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson1', 'biogrid'],
    discrition='A DAG to load biogrid from website into Postgres database',
    catchup=False
) as dag:
    load_data_op = EmptyOperator(
        task_id='load_data'
    )

    ingest_data_op = EmptyOperator(
        task_id='ingest_data'
    )

    load_data_op >> ingest_data_op

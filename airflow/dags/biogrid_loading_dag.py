import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import pandas as pd
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

BASE_URL = 'https://downloads.thebiogrid.org/Download/BioGRID/Release-Archive/BIOGRID-{version}/BIOGRID-ALL-{version}.tab3.zip'

def load_biogrid():
    logging.info('Loading biogrid file...')
    response = requests.get(
        BASE_URL.format(version='4.4.200'),
        params={'downloadformat': 'zip'}
    )

    if response.status_code == 200:
        local_file_name = 'biogrid.tab3.zip'
        with open(local_file_name, 'wb') as f:
            f.write(response.content)
    else:
        logging.error("The specified version is not found")
        raise Exception()

    logging.info('Biogrid file has loaded')
    logging.info('Starting biogrid processing')

def ingest_data():
    local_file_name = 'biogrid.tab3.zip'
    df = pd.read_csv(local_file_name, delimiter='\t', compression='zip', nrows=100)

    df = df.rename(
        lambda column_name: column_name.lower().replace(' ', '_').replace('#', '_').strip('_'),
        axis='columns'
    )

    df = df[[
        'biogrid_interaction_id',
        'biogrid_id_interactor_a',
        'biogrid_id_interactor_b',
    ]]

    df['version'] = '4.4.200'

    logging.info('Biogrid file has been transformed')
    logging.info('Starting ingestion into database...')

    postgres_hook = PostgresHook(postgres_conn_id='postgres_local')
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql('biogrid_data', engine, if_exists='replace')
    logging.info('Data successfully ingested')


with DAG(
    dag_id='biogrid_loading_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson1', 'biogrid'],
    description='A DAG to load biogrid from website into Postgres database',
    catchup=False
) as dag:
    load_data_op = PythonOperator(
        task_id='load_data',
        python_callable=load_biogrid
    )

    ingest_data_op = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data
    )

    trigger_function_op = PostgresOperator(
        task_id='trigger_function',
        sql='SELECT get_biogrid_interactors();',
        postgres_conn_id='postgres_local'
    )

    load_data_op >> ingest_data_op >> trigger_function_op

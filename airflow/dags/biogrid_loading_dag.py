import logging

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.models import (
    Param,
    Variable,
)
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule


def _check_version_existence(params):
    biogrid_version = params['version']

    postgres_hook = PostgresHook(postgres_conn_id='postgres_local')
    connection = postgres_hook.get_conn()
    with connection.cursor() as cursor:
        cursor.execute('SELECT DISTINCT(bd."version") FROM biogrid_data bd;') # [(value,)]
        loaded_versions = [version[0] for version in cursor.fetchall()]

        if biogrid_version not in loaded_versions:
            return 'load_data'
        else:
            return 'finish'


def load_biogrid(params, ti):
    biogrid_version = params['version']
    biogrid_url = Variable.get('biogrid_url')
    local_file_name = f'biogrid_v{biogrid_version.replace(".", "_")}.tab3.zip'
    ti.xcom_push(
        key='local_file_name',
        value=local_file_name
    )

    logging.info('Loading biogrid file...')
    response = requests.get(
        biogrid_url.format(version=biogrid_version),
        params={'downloadformat': 'zip'}
    )

    if response.status_code == 200:
        with open(local_file_name, 'wb') as f:
            f.write(response.content)
    else:
        logging.error('The specified version is not found')
        raise Exception()

    logging.info('Biogrid file has loaded')
    logging.info('Starting biogrid processing')

def ingest_data(params, ti):
    biogrid_version = params['version']
    local_file_name = ti.xcom_pull(task_ids='load_data', key='local_file_name')

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

    df['version'] = biogrid_version

    logging.info('Biogrid file has been transformed')
    logging.info('Starting ingestion into database...')

    postgres_hook = PostgresHook(postgres_conn_id='postgres_local')
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql('biogrid_data', engine, if_exists='append')
    logging.info('Data successfully ingested')


with DAG(
    dag_id='biogrid_loading_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=['lesson3', 'biogrid'],
    description='A DAG to load biogrid from website into Postgres database',
    catchup=False,
    params={
        'version': Param('4.4.200', type='string')
    }
) as dag:
    start_op = EmptyOperator(task_id='start')

    check_version_existence_op = BranchPythonOperator(
        task_id='check_version_existence',
        python_callable=_check_version_existence
    )

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

    finish_op = EmptyOperator(
        task_id='finish',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    start_op >> check_version_existence_op >> load_data_op >> ingest_data_op >> trigger_function_op >> finish_op
    start_op >> check_version_existence_op >> finish_op

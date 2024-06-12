from airflow_client import client
from airflow_client.client.api import dag_run_api, dag_api
from airflow_client.client.model.dag_run import DAGRun

AIRFLOW_HOST = 'http://localhost:8080'
AIRFLOW_USERNAME = 'admin'
AIRFLOW_PASSWORD = 'admin'

class AirflowService:
    def __init__(self):
        self._configuration = client.Configuration(
            host=f'{AIRFLOW_HOST}/api/v1',
            username=AIRFLOW_USERNAME,
            password=AIRFLOW_PASSWORD
        )

    def _get_internal_data(self):
        pass


    def trigger_biogrid_loading_dag(self, dag_params):
        with client.ApiClient(self._configuration) as api_client:
            dag_id='biogrid_loading_dag'
            dag_run = DAGRun(conf=dag_params)

            api_instance = dag_run_api.DAGRunApi(api_client)
            api_instance.post_dag_run(dag_id, dag_run)


    def get_dags(self):
        with client.ApiClient(self._configuration) as api_client:
            dag_api_instance = dag_api.DAGApi(api_client)

            dags = dag_api_instance.get_dags()
            return dags

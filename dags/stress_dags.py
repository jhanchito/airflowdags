from airflow import DAG
from airflow.providers.teradata.operators.teradata import TeradataOperator
from datetime import datetime

with DAG('stress_test_teradata_load', start_date=datetime(2023,1,1), schedule='@once') as dag:
    for i in range(50):  # Genera 50 tareas paralelas
        TeradataOperator(
            task_id=f'query_teradata_{i}',
            teradata_conn_id='teradata_default',
            sql="CALL PE_PROD_ARQ_DATA.demosp_airflow();" 
        )
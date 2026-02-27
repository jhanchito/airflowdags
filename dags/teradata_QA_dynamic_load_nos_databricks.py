from airflow import DAG
from airflow.providers.teradata.operators.teradata import TeradataOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import random

# Configuraciones fijas (Sin lógica aleatoria aquí)
TABLE_CONFIG = [
    {
        "ID_FUENTE": "M_PlantaClientes",
        "script_table": "select * from pe_prod_lz_data.aldm_subscriber where SUBSCRIBER_STATUS_KEY = 2266",
        "base_location": "/AZ/scacanaliticadev.BLOB.core.windows.net/lakehouse/landing/m_suscrip"
    }
]

DATABRICKS_CONN_ID = 'databricksDev'
DATABRICKS_JOB_ID = 922393484091429

# Función para generar el número una sola vez por ejecución
def generate_random_id():
    return random.randint(1, 100)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'teradata_QA_dynamic_load_nos_databricks_v2',
    default_args=default_args,
    description='Carga dinámica con XCom para consistencia de RANDOM_VAL',
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['teradata', 'databricks', 'nos', 'fix'],
) as dag:

    # 1. Tarea que genera el valor único para esta corrida
    get_random_val = PythonOperator(
        task_id='generate_id',
        python_callable=generate_random_id
    )

    # 2. Databricks task (usando Jinja para jalar el valor de XCom)
    run_databricks_job = DatabricksRunNowOperator(
        task_id='run_databricks_now',
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
        notebook_params={
            "identificadores": "valor1",
            "ruta": "{{ task_instance.xcom_pull(task_ids='generate_id') }}"
        }
    )

    # 3. Generación dinámica de tareas de Teradata
    for config in TABLE_CONFIG:
        # Usamos Macros de Jinja dentro del SQL para que Airflow lo renderice al ejecutar
        # La ruta final será: base_location + numero_xcom + /
        load_task = TeradataOperator(
            task_id=config['ID_FUENTE'],
            teradata_conn_id='teradata_dev',
            sql=f"""
                SELECT * FROM WRITE_NOS (
                  ON ({config['script_table']})
                  USING
                    LOCATION('{config['base_location']}{{{{ task_instance.xcom_pull(task_ids='generate_id') }}}}/')
                    AUTHORIZATION(nos_usr.token_datalab_landing)
                    STOREDAS('parquet')
                ) AS dt;
            """
        )

        # Flujo: Generar ID -> Teradata -> Databricks
        get_random_val >> load_task >> run_databricks_job
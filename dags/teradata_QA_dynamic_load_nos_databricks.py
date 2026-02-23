from airflow import DAG
from airflow.providers.teradata.operators.teradata import TeradataOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime
import random
# Table and location mapping configuration

RANDOM_VAL = random.randint(1, 100)
TABLE_CONFIG = [
    {
        "ID_FUENTE" : "M_PlantaClientes",
        "script_table": "select * from pe_prod_lz_data.aldm_subscriber limit 777",
        "target_location": f"/AZ/scacanaliticadev.BLOB.core.windows.net/lakehouse/landing/m_suscrip{RANDOM_VAL}/"
    },
    # Add more tables and locations here as needed
]

DATABRICKS_CONN_ID = 'databricksDev'
DATABRICKS_JOB_ID = 922393484091429

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'teradata_QA_dynamic_load_nos_databricks',
    default_args=default_args,
    description='Dynamic Teradata WRITE_NOS load followed by Databricks job',
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['teradata', 'databricks', 'nos'],
) as dag:

    # Databricks task to run after all Teradata loads are complete
    run_databricks_job = DatabricksRunNowOperator(
        task_id='run_databricks_now',
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
        notebook_params={
            "identificadores": "valor1",
            "ruta": f"{RANDOM_VAL}"
        }
    )

    # Dynamically generate Teradata load tasks
    for i, config in enumerate(TABLE_CONFIG):
        script_table = config['script_table']
        location = config['target_location']
        load_task = TeradataOperator(
            task_id=config['ID_FUENTE'],
            teradata_conn_id='teradata_dev',
            sql=f"""
                SELECT * FROM WRITE_NOS (
                  ON ({script_table} )
                  USING
                    LOCATION('{location}')
                    AUTHORIZATION(nos_usr.token_datalab_landing)
                    STOREDAS('parquet')
                ) AS dt;
            """
        )

        # Set dependency: each load task must finish before the Databricks job runs
        load_task >> run_databricks_job

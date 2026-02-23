from airflow import DAG
from airflow.providers.teradata.operators.teradata import TeradataOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime
import random
# Table and location mapping configuration

random = random.randint(1, 100)
TABLE_CONFIG = [
    {
        "script_table": "select * from pe_prod_lz_data.aldm_subscriber limit 777",
        "target_location": "/AZ/scacanaliticadev.BLOB.core.windows.net/lakehouse/landing/m_suscrip{random}/"
    },
    # Add more tables and locations here as needed
    # {
    #     "script_table": "select * from another_schema.another_table limit 777",
    #     "target_location": "/AZ/scacanaliticadev.BLOB.core.windows.net/lakehouse/landing/another_path/"
    # }
]

DATABRICKS_CONN_ID = 'databricksDev'
DATABRICKS_JOB_ID = 828525461039791

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'teradata_dynamic_load_nos_databricks',
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
    )

    # Dynamically generate Teradata load tasks
    for i, config in enumerate(TABLE_CONFIG):
        script_table = config['script_table']
        location = config['target_location']
        # Extract meaningful task ID part (last part of table name)
        table_name = script_table.split('.')[-1]
        
        load_task = TeradataOperator(
            task_id=f'write_nos_{table_name}',
            teradata_conn_id='teradata',
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

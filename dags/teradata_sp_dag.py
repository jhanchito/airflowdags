from datetime import datetime
from airflow import DAG
try:
    from airflow.providers.teradata.operators.teradata import TeradataOperator
except ImportError:
    # Fallback or user might need to install apache-airflow-providers-teradata
    # Defining a dummy operator or just letting it fail on import if provider is missing
    # But usually in a DAG file we expect imports to work on the airflow server.
    pass

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'teradata_sp_execution',
    default_args=default_args,
    description='A DAG to execute a Teradata Stored Procedure',
    schedule_interval=None, # Set to None for manual trigger, adjust as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['teradata', 'example'],
) as dag:

    # IMPORTANT: Ensure the 'apache-airflow-providers-teradata' package is installed in your Airflow environment.
    # IMPORTANT: Update 'teradata' to your actual Connection ID.
    # IMPORTANT: Update the SQL to your actual Stored Procedure call.
    call_stored_procedure = TeradataOperator(
        task_id='call_stored_procedure',
        teradata_conn_id='teradata',
        sql='CALL my_db.my_sp();',
    )

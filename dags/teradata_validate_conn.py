from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.teradata.hooks.teradata import TeradataHook
from datetime import datetime

def test_teradata_connection():
    # Usamos el ID que ya tienes creado: 'teradata'
    hook = TeradataHook(teradata_conn_id='teradata')
    try:
        # Intentamos obtener la conexión y ejecutar una consulta simple
        conn = hook.get_conn()
        print("¡Conexión exitosa al motor Teradata!")
        
        # Opcional: Validar versión o una tabla dual
        df = hook.get_pandas_df(sql="SELECT DATABASE;")
        print(f"Base de datos actual: {df.iloc[0,0]}")
        
    except Exception as e:
        print(f"Error detectado durante la validación: {e}")
        raise e

with DAG(
    dag_id='z_test_teradata_connection',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id='validate_conn',
        python_callable=test_teradata_connection
    )
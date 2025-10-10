from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor # 💡 Sensor clave

# Define las constantes
AZURE_CONN_ID = "azure_blob_default"  # El ID de tu conexión en Airflow
AZURE_CONTAINER = "datos-entrada"
AZURE_BLOB_FILE = "archivo_para_procesar.csv"

with DAG(
    dag_id="azure_blob_trigger_dag_sensor",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=pendulum.duration(minutes=5),  # El DAG se ejecuta cada 5 minutos
    catchup=False,
    tags=["azure", "sensor", "data"],
) as dag:
    # 1. Tarea Sensor: Espera la existencia del archivo
    wait_for_file = WasbBlobSensor(
        task_id="wait_for_azure_blob_file",
        container_name=AZURE_CONTAINER,
        blob_name=AZURE_BLOB_FILE,
        wasb_conn_id=AZURE_CONN_ID,
        # Tiempo de espera (timeout) en segundos antes de que la tarea falle
        # Esto evita que el Sensor espere indefinidamente
        timeout=60 * 60 * 24,  # Espera hasta 24 horas
        poke_interval=60,       # Revisa el archivo cada 60 segundos (1 minuto)
        mode="poke",            # Modo tradicional de polling
    )

    # 2. Tarea de Procesamiento: Se ejecuta inmediatamente después de que el Sensor tiene éxito
    process_data = BashOperator(
        task_id="process_uploaded_file",
        bash_command=f"echo 'Archivo {AZURE_BLOB_FILE} encontrado. Iniciando procesamiento...'",
    )

    # Definir el orden del flujo
    wait_for_file >> process_data
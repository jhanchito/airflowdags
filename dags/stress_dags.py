from airflow import DAG
from airflow.providers.teradata.operators.teradata import TeradataOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import math

def cpu_load():
    """Function to generate some CPU load"""
    # Calculate primes up to 1000 to generate CPU usage
    primes = []
    for num in range(2, 1000):
        if all(num % i != 0 for i in range(2, int(math.sqrt(num)) + 1)):
            primes.append(num)
    time.sleep(1) # Ensure it takes at least a bit of time

with DAG('stress_test_teradata_load', start_date=datetime(2023,1,1), schedule='@once', catchup=False) as dag:
    
    # 1. Teradata Tasks (Database Load) - Increased to 100
    for i in range(100):
        TeradataOperator(
            task_id=f'query_teradata_{i}',
            teradata_conn_id='teradata',
            sql="CALL PE_PROD_ARQ_DATA.demosp_airflow(1);" 
        )

    # 2. Bash Tasks (Scheduler/Executor/Pod Load) - 100 Tasks
    for j in range(100):
        BashOperator(
            task_id=f'bash_sleep_{j}',
            bash_command='sleep 30'
        )

    # 3. Python Tasks (Worker CPU Load) - 50 Tasks
    for k in range(50):
        PythonOperator(
            task_id=f'python_cpu_{k}',
            python_callable=cpu_load
        )
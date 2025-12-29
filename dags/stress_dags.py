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
    
    # Combined Loop for Chained Tasks
    for i in range(100):
        # 1. Teradata Task
        t1 = TeradataOperator(
            task_id=f'query_teradata_{i}',
            teradata_conn_id='teradata',
            sql="CALL PE_PROD_ARQ_DATA.demosp_airflow(1);" 
        )

        # 2. Bash Task
        t2 = BashOperator(
            task_id=f'bash_sleep_{i}',
            bash_command='sleep 30'
        )

        # 3. Python Task
        t3 = PythonOperator(
            task_id=f'python_cpu_{i}',
            python_callable=cpu_load
        )

        # Chain the tasks
        t1 >> t2 >> t3
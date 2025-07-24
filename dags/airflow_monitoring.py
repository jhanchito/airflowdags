from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a simple Python function that our task will execute
def my_simple_python_function():
    print("Hello from a Python function in Airflow!")
    # You could add any Python logic here, e.g.,
    # import pandas as pd
    # df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    # print(df)

with DAG(
    dag_id='simple_python_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # This DAG will only run manually
    catchup=False,
    tags=['example', 'python'],
) as dag:
    # Task 1: A simple PythonOperator that calls our function
    run_python_task = PythonOperator(
        task_id='execute_my_function',
        python_callable=my_simple_python_function,
    )
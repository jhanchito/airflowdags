from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

my_dag = DAG(
     dag_id="my_dag_name",
     start_date=datetime(2021, 1, 1),
     schedule="@daily",
 )
EmptyOperator(task_id="task", dag=my_dag)
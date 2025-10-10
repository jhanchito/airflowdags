from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime
default_args = {
  'owner': 'airflow'
}
with DAG('databricksJob_poc',
  start_date = datetime(2021, 1, 1),
  schedule=None,
  default_args = default_args
  ) as dag:
  opr_run_now = DatabricksRunNowOperator(
    task_id = 'run_now',
    databricks_conn_id = 'databricksDev',
    job_id = 49300933223328
  )
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
default_args = {
  'owner': 'airflow'
}
with DAG('databricksJob_poc',
  start_date = days_ago(2),
  schedule_interval = None,
  default_args = default_args
  ) as dag:
  opr_run_now = DatabricksRunNowOperator(
    task_id = 'run_now',
    databricks_conn_id = 'databricksDev',
    job_id = 49300933223328
  )
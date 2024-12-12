from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'qdvn',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id="qdvn_dag_catchup_backfill_v2",
        default_args=default_args,
        description="This is the 4th dag I write",
        start_date=datetime(2024, 11, 1, 5),
        schedule_interval='@daily',
        catchup=False  # False = No run past schedule
) as dag:
  task1 = BashOperator(
      task_id='qdvn_first_task_with_catchup',
      bash_command="echo This is task1"
  )
  task1

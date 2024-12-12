from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'qdvn',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def get_sklearn():
  import sklearn
  print(123)
  print(f"scikit-learn with version: {sklearn.__version__}")


def get_numpy():
  import numpy as np
  print(np.__version__)


def get_greeting_v2(ti):
  ti.xcom_push(key='greeting_first', value='Quang')
  ti.xcom_push(key='greeting_last', value='Dang')


def get_age():
  return 23


with DAG(
    default_args=default_args,
    dag_id='qdvn_dag_with_sklearn_v2',
    start_date=datetime(2024, 12, 1),
    schedule_interval='@daily'
) as dag:
  task_sklearn = PythonOperator(
      task_id='get_sklearn',
      python_callable=get_sklearn
  )
  task_numpy = PythonOperator(
      task_id='get_numpy',
      python_callable=get_numpy
  )
  task_sklearn >> task_numpy

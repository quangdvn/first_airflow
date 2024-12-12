from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'qdvn',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    default_args=default_args,
    dag_id='qdvn_dag_minio_v1',
    start_date=datetime(2024, 12, 6),
    schedule_interval='@daily'
) as dag:

  task1 = S3KeySensor(
      task_id='qdvn_dag_with_minio',
      # bucket_name='airflow',
      bucket_key='s3://airflow/data.csv',
      aws_conn_id='minio',
      mode='poke',
      verify=False,
      timeout=60,
      poke_interval=5,
      # dag=dag
  )

  task1

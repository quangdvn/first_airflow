import csv
import logging
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'qdvn',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def postgres_to_s3(ds_nodash):  # Airflow will generate macro during execution
  # now = datetime.now()
  # hour = now.hour
  # minute = now.minute
  # second = now.second

  # Step 1: Query data from Postgres
  hook = PostgresHook(postgres_conn_id='postgres_localhost')
  connection = hook.get_conn()
  cursor = connection.cursor()
  cursor.execute("select * from orders where date <= '2022-03-01' order by date")

  # output_file_path = f'dags/output/get_orders_{ds_nodash}_H{hour}_M{minute}_S{second}.txt'
  # logging.info(output_file_path)
  # logging.info(cursor.fetchall())
  # with open(output_file_path, 'w') as f:

  # Step 2: Save data to text file
  with NamedTemporaryFile(mode="w", suffix=f"{ds_nodash}") as f:
    csv_writer = csv.writer(f)
    csv_writer.writerow([i[0] for i in cursor.description])
    csv_writer.writerows(cursor)
    f.flush()
    cursor.close()
    connection.close()
    logging.info(f"Saved orders data in text file: dags/get_orders_{ds_nodash}.txt")
    # f.write(str(cursor.fetchall()))

    # Step 3: Upload text file into S3
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_hook.load_file(
        filename=f.name,
        key=f"orders/{ds_nodash}.txt",
        bucket_name='airflow',
        replace=True
      )
    logging.info(f"Uploaded orders data into S3: {f.name}")


with DAG(
    default_args=default_args,
    dag_id='qdvn_dag_postgres_hooks_v03',
    start_date=datetime(2024, 12, 6),
    schedule_interval='@daily'
) as dag:
  task1 = PythonOperator(
     task_id='postgres_to_s3',
     python_callable=postgres_to_s3
   )

  task1

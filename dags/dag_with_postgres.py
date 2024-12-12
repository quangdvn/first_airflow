from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'qdvn',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id="qdvn_dag_postgres_v05",
        default_args=default_args,
        description="This is the 5th dag I write",
        start_date=datetime(2024, 12, 1),
        schedule_interval='@daily',
        catchup=False
) as dag:
  task1 = PostgresOperator(
      task_id='create_postgres_table',
      postgres_conn_id='postgres_localhost',
      sql="""
        create table if not exists qdvn_dag_runs (
            dt date,
            dag_id character varying,
            primary key (dt, dag_id)
        )
      """
  )

  task2 = PostgresOperator(
      task_id='delete_postgres_table',
      postgres_conn_id='postgres_localhost',
      sql="""
        delete from qdvn_dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}'
      """
  )

  task3 = PostgresOperator(
      task_id='insert_into_table',
      postgres_conn_id='postgres_localhost',
      sql="""
        insert into qdvn_dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
      """
  )

  task1 >> task2 >> task3

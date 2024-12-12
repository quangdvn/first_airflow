from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'qdvn',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet(name, age):
  print(f"Hello World, my name is {name} and I am {age} years old")


def greet_v2(ti):
  name = ti.xcom_pull(task_ids='get_greeting')
  print(f"Hello World, my name is {name}")


def greet_v3(ti):
  name = ti.xcom_pull(key='greeting_first', task_ids='get_greeting_v2')
  last_name = ti.xcom_pull(key='greeting_last', task_ids='get_greeting_v2')
  age = ti.xcom_pull(task_ids='get_age')
  print(f"Hello World, my name is {name} {last_name}, I'm {age} years old")


def get_greeting():
  return "Quang"

# !! NOTE: MAX XCOM Size is 48KB


def get_greeting_v2(ti):
  ti.xcom_push(key='greeting_first', value='Quang')
  ti.xcom_push(key='greeting_last', value='Dang')


def get_age():
  return 23


with DAG(
    default_args=default_args,
    dag_id='qdvn_dag_python_v1',
    start_date=datetime(2024, 12, 1),
    schedule_interval='@daily'
) as dag:
  task1 = PythonOperator(
      task_id='greet',
      python_callable=greet,
      op_kwargs={'name': 'Quang Dang', 'age': 23}
  )
  task2 = PythonOperator(
      task_id='get_greeting',
      python_callable=get_greeting
  )

  task1

with DAG(
    default_args=default_args,
    dag_id='qdvn_dag_python_v2',
    start_date=datetime(2024, 12, 1),
    schedule_interval='@daily'
) as dag:
  task1 = PythonOperator(
      task_id='greet_v2',
      python_callable=greet_v2,
  )
  task2 = PythonOperator(
      task_id='get_greeting',
      python_callable=get_greeting
  )

  task2 >> task1

with DAG(
    default_args=default_args,
    dag_id='qdvn_dag_python_v3',
    start_date=datetime(2024, 12, 1),
    schedule_interval='@daily'
) as dag:
  task1 = PythonOperator(
      task_id='greet_v3',
      python_callable=greet_v3,
  )
  task2 = PythonOperator(
      task_id='get_greeting_v2',
      python_callable=get_greeting_v2
  )
  task3 = PythonOperator(
      task_id='get_age',
      python_callable=get_age
  )

  [task2, task3] >> task1

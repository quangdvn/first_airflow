from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'qdvn',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id="qdvn_first_dag_v1",
         default_args=default_args,
         description="This is the 1st dag I write",
         start_date=datetime(2024, 12, 1, 5),
         schedule_interval='@daily'
         ) as dag:
  task1 = BashOperator(
    task_id='qdvn_first_task',
    bash_command="echo This is task1"
  )
  task1

with DAG(dag_id="qdvn_first_dag_v2",
         default_args=default_args,
         description="This is the 2nd dag I write",
         start_date=datetime(2024, 12, 1, 5),
         schedule_interval='@daily'
         ) as dag:
  task1 = BashOperator(
    task_id='qdvn_first_task',
    bash_command="echo This is task1"
  )
  task2 = BashOperator(
    task_id='qdvn_second_task',
    bash_command="echo This is task2"
  )
  # task3 = BashOperator(
  #   task_id='qdvn_third_task',
  #   bash_command="echo This is task3, run at the same time as task2"
  # )

  task1.set_downstream(task2)
  # task1.set_downstream(task3)

with DAG(dag_id="qdvn_first_dag_v3",
         default_args=default_args,
         description="This is the 3rd dag I write",
         start_date=datetime(2024, 12, 1, 5),
         schedule_interval='@daily'
         ) as dag:
  task1 = BashOperator(
    task_id='qdvn_first_task',
    bash_command="echo This is task1"
  )
  task2 = BashOperator(
    task_id='qdvn_second_task',
    bash_command="echo This is task2"
  )
  task3 = BashOperator(
    task_id='qdvn_third_task',
    bash_command="echo This is task3, run at the same time as task2"
  )

  # task1 >> task2
  # task1 >> task3
  task1 >> [task2, task3]

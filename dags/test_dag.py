from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import os
import csv

def read_file(dir, filename):
    with open(os.path.join(dir, filename), 'r') as file:
        data = list(csv.reader(file))
    return data

def print_content(s):
    print(s)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

# Create the DAG
dag = DAG(
    'my_test_pipeline',
    default_args=default_args,
    description='test pipeline',
    schedule_interval="@once",
    catchup=False
)

start_task = DummyOperator(task_id='start_task', dag=dag)

# Define the extract_task operator
read_task = PythonOperator(
    task_id='read_task',
    python_callable=read_file,
    op_args=[r'/opt/airflow/dags/files', 'test.csv'],
    dag=dag
)

print_task = PythonOperator(
    task_id='print_task',
    python_callable=print_content,
    op_args=[read_task.output],
    dag=dag
)

start_task >> read_task >> print_task
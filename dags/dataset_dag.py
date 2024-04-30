from datetime import datetime, timedelta
import os
import csv
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

def read_file(dir, filename):
    with open(os.path.join(dir, filename), 'r') as file:
        data = list(csv.reader(file))
    for row in data:
        row[0]= datetime.strptime(row[0], '%m/%d/%Y').strftime("%Y-%m-%d")
    return data

def print_content(s):
    print(s)

def insert_hook(data):
    trunc_request = 'truncate dags.prog_langs;'
    request = 'insert into dags.prog_langs (week, python_trend, java_trend, c_trend) VALUES'
    for row in data:
        request += "\n('{0}', {1}, {2}, {3}),".format(row[0], row[1], row[2], row[3])
    request = request[:-1] + ';'
    print(request)
    pg_hook = PostgresHook(postgres_conn_id='postgre_sql', schema='airflow')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(trunc_request)
    cursor.execute(request)
    connection.commit()

def process():
    request = 'select * from dags.prog_langs order by week;'
    pg_hook = PostgresHook(postgres_conn_id='postgre_sql', schema='airflow')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    rows = cursor.fetchall()
    
    df = pd.DataFrame(rows, columns=['id', 'week', 'python_trend', 'java_trend', 'c_trend'])
    corr_matrix = df.corr()

    start_week = rows[0][1]
    print(start_week)
    end_week = rows[-1][1]
    print(end_week)

    save_request = "insert into dags.prog_langs_results (start_week, end_week, java_c_corr, python_java_coor, python_c_corr) VALUES ('{0}', '{1}', {2}, {3}, {4});"
    save_request = save_request.format(start_week, end_week, corr_matrix['java_trend']['c_trend'], corr_matrix['python_trend']['java_trend'], corr_matrix['python_trend']['c_trend'])

    print(save_request)
    cursor.execute(save_request)
    print(cursor.rowcount)
    connection.commit()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG('dataset_dag',
         description='dataset dag',
         default_args = default_args,
         schedule_interval="@once",
         catchup=False) as dag:
    start_task = DummyOperator(task_id='start_task_ds', dag=dag)
    read_task = PythonOperator(task_id='read_task_ds', dag=dag, python_callable=read_file, op_args=[r'/opt/airflow/dags/files', 'dataset.csv'])
    
    print_task = PythonOperator(task_id='print_task_ds', python_callable=print_content, op_args=[read_task.output], dag=dag)
    insert_task = PythonOperator(task_id='insert_task_ds', dag=dag, python_callable=insert_hook, op_args=[read_task.output])
    process_task = PythonOperator(task_id='process_task_ds', dag=dag, python_callable=process)

    start_task >> read_task >> insert_task >> process_task
    read_task >> print_task
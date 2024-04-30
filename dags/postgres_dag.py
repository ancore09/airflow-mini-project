from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

def test_hook():
    request = 'select * from dags.test_table;'
    pg_hook = PostgresHook(postgres_conn_id='postgre_sql', schema='airflow')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    records = cursor.fetchall()
    return records

def print_records(records):
    for record in records:
        print('Id: {0} - Text: {1}'.format(record[0], record[1]))


with DAG('postgres_dag',
         description='test postgres dag',
         default_args = default_args,
         schedule_interval="@once",
         catchup=False) as dag:
    start_task = DummyOperator(task_id='start_task_pg', dag=dag)
    hook_task = PythonOperator(task_id='hook_task_pg', dag=dag, python_callable=test_hook)
    print_task = PythonOperator(task_id='print_task_pg', op_args=[hook_task.output], dag=dag, python_callable=print_records)

    start_task >> hook_task >> print_task

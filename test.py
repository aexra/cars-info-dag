from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, World!")

with DAG(
    dag_id='hello_world_dag',
    start_date=datetime(2024, 11, 17),
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_hello_world = PythonOperator(
        task_id='say_hello',
        python_callable=hello_world
    )

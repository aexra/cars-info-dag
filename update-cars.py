from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import psycopg2

# Параметры для подключения Postgres
PG_CONN_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": 5432
}

# Получение курса вылюты с ЦБ
def get_exchange_rate(**context):
    execution_date = context['execution_date']

    # Если сегодня суббота, возьмем целевую дату на день раньше
    if execution_date.weekday() == 5:
        target_date = execution_date - timedelta(days=1)
    else:
        target_date = execution_date

    date_str = target_date.strftime("%d/%m/%Y")

    url = f"https://cbr.ru/scripts/xml_daily.asp?date_req=05/12/2021"

    response = requests.get(url)

    root = ET.fromstring(response.content)

    for currency in root.findall(".//Valute"):
        if currency.find("CharCode").text == "USD":  # Курс доллара (в ТЗ не было указано какая именно иностранная валюта хранится в csv)
            rate = float(currency.find("Value").text.replace(",", "."))
            return rate

    raise ValueError("USD rate not found")

# Вытаскиваем csv из MinIO
def download_csv_from_minio(bucket_name, object_key, local_path, **kwargs):
    s3 = S3Hook(aws_conn_id='minio_default')
    s3.get_key(object_key, bucket_name).download_file(local_path)

# Обработка данных и запись в PostgreSQL
def process_and_load_to_postgres(csv_path, exchange_rate, **kwargs):
    df = pd.read_csv(csv_path, sep=';')
    df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')
    df['price_rub'] = df['price_usd'] * float(exchange_rate)

    conn = psycopg2.connect(**PG_CONN_PARAMS)
    cur = conn.cursor()

    cur.execute('TRUNCATE TABLE cars;')

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO cars (brand, model, engine_capacity, prod_year, price)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (row['brand'], row['model'], row['engine_capacity'], row['prod_year'], row['price_rub'])
        )

    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
        dag_id='cars_update',
        default_args=default_args,
        start_date=datetime(2024, 11, 18),
        schedule_interval='0 12 * * 1-6',   # Понедельник-суббота в 12:00
        catchup=False
) as dag:

    get_rate = PythonOperator(
        task_id='get_exchange_rate',
        python_callable=get_exchange_rate,
        provide_context=True
    )

    download_csv = PythonOperator(
        task_id='download_csv_from_minio',
        python_callable=download_csv_from_minio,
        op_kwargs={
            'bucket_name': 'cars-bucket',
            'object_key': 'cars.csv',
            'local_path': '/tmp/cars.csv'
        }
    )

    process_and_load = PythonOperator(
        task_id='process_and_load_to_postgres',
        python_callable=process_and_load_to_postgres,
        op_kwargs={
            'csv_path': '/tmp/cars.csv',
            'exchange_rate': '{{ ti.xcom_pull(task_ids="get_exchange_rate") }}'
        }
    )

    get_rate >> download_csv >> process_and_load

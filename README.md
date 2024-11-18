# THE DAG

## Содержание
- [Содержание](#Содержание)
- [ТЗ](#ТЗ)
- [DAG](#DAG)
- [Тестирование](#Тестирование)

## ТЗ

В бакет на S3 в будние дни по расписанию кладется файл csv с содержимым: автомобиль, стоимость в иностранной валюте, дата актуальности всего файла. Характеристики автомобилей: марка, модель, объем двигателя, год выпуска. С каждым файлом информация может изменяться: добавляются строки для других автомобилей, удаляются строки для уже бывших или же изменяется стоимость на них.

Написать DAG для Apache Airflow, который переводит стоимость в рублевый эквивалент по актуальному на дату создания файла курсу ЦБ и складывает эту информацию в таблицу в GreenPlum. DAG должен работать с понедельника по субботу, но не по воскресеньям. В субботу необходимо использовать курс валют, актуальный на минувшую пятницу.

Курс валют можно скачать с сайта ЦБ на нужную дату https://cbr.ru/scripts/xml_daily.asp?date_req=05/12/2021

## DAG

Чисто по ТЗ был реализован следующий граф [(ссылка на сам файл)](./update-cars.py):

```python
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
def download_csv_from_s3(bucket_name, object_key, local_path, **kwargs):
    s3 = S3Hook(aws_conn_id='minio_default')
    s3.get_key(object_key, bucket_name).download_file(local_path)

# Обработка данных и запись в PostgreSQL
def process_and_load_to_db(csv_path, exchange_rate, **kwargs):
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
        task_id='download_csv_from_s3',
        python_callable=download_csv_from_s3,
        op_kwargs={
            'bucket_name': 'cars-bucket',
            'object_key': 'cars.csv',
            'local_path': '/tmp/cars.csv'
        }
    )

    process_and_load = PythonOperator(
        task_id='process_and_load_to_db',
        python_callable=process_and_load_to_db,
        op_kwargs={
            'csv_path': '/tmp/cars.csv',
            'exchange_rate': '{{ ti.xcom_pull(task_ids="get_exchange_rate") }}'
        }
    )

    get_rate >> download_csv >> process_and_load
```

Граф разбит на три последовательных задачи соответственно:
- Получение курса валюты из ЦБ
- Загрузка csv из S3
- Преобразование цены на все авто в csv по полученному в первой таске курсу и зазгрузка их в базу данных

![image](https://github.com/user-attachments/assets/680550c1-7c70-47cf-aa51-339af26cdc7d)

## Тестирование

- [ОС](#ОС)
- [S3 хранилище](#S3)
- [База данных](#База-данных)
- [Результат работы DAG](#Результат)

### ОС

Вся работа проводилась под [Ubuntu 24.04.1 LTS](https://ubuntu.com/)

### S3

В качестве S3 хранилища для поставленной задачи был выбран проект [MinIO](https://min.io/)

![image](https://github.com/user-attachments/assets/eca017fb-7cfa-4dee-8598-7082a36a87f1)

С соответствующим подключением в Airflow как ```Amazon Web Service```

![image](https://github.com/user-attachments/assets/88bc6f75-e33c-4e83-b1e5-ab76f3dfe4db)

Файл [cars.csv](./data/cars/csv), использованный для тестов:

```csv
brand;model;engine_capacity;prod_year;price_usd
Toyota;Corolla;1.6;2020;1500000
Ford;Mustang;5.0;2022;5500000
Hyundai;Elantra;2.0;2018;1100000
BMW;X5;3.0;2019;4000000
Lada;Vesta;1.8;2021;950000
```

### База данных

> Так сказать, нюанс...

В качестве БД для этого проекта был использован сервер PostgreSQL 17, в связи с невозможностью получить Greenplum с [официального сайта](https://greenplum.org/) (кнопка Downloads оттуда пропала вроде в прошлом году)

### Результат

DAG находит курс на дату вызова, читает ```csv```, получает стоимость в рублях для каждого авто в датафрейме, очищает таблицу в бд со старыми записями и записывает актуальные:

![image](https://github.com/user-attachments/assets/c9e46dd5-ff34-4b13-b951-274769b4d9b3)

![image](https://github.com/user-attachments/assets/1a431227-1f8d-434f-a8c9-81d2b3d4f1dd)

> [!NOTE]
> pgAdmin отображает рядом со значением типа ```money``` знак валюты, соответствующей локализации ОС

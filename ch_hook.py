# Библиотеки для работы с XML, http, data frame и date
import requests as req  # для выполнения HTTP-запросов
import pandas as pd  # для обработки данных
from datetime import datetime, timedelta  # для работы с датами
import json as json # для парсинга JSON
from clickhouse_driver import Client  # для подключения к ClickHouseimport requests as req


# Библиотеки для работы с Airflow

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models.baseoperator import BaseOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator
import os

# Создаем
s_file = '/opt/airflow/airflow_data/data.json'
csv_file = '/opt/airflow/airflow_data/data.csv'

url = 'https://api.exchangerate.host/timeframe?access_key=043dc9dad696914726d3064e9d917294&source=USD'

# Функция для извлечения данных с API и локального сохранения
def extract_data(url,**context):
    date = context['ds']
    s_file = f'/opt/airflow/airflow_data/data_{date}.json'
    request = req.get(f'{url}&start_date={date}&end_date={date}')
    data = json.loads(request.text)
    with open(s_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Extracted data for {date}")

# Функция для обработки данных в формате JSON и преобразования их в CSV

def transform_data(s_file, **context):
    date = context['ds']
    csv_file = f'/opt/airflow/airflow_data/data_{date}.csv'
    with open(s_file, 'r', encoding='utf-8') as f:
        data = json.load(f)


    rows=[]
    for k,v in data['quotes'].items():
      for keys,values in v.items():
        rows.append({
            "valute": keys,
            "salary": values})
    
    df = pd.DataFrame(rows)
    df['currency_source'] = df['valute'].str[:3]
    df['currency'] = df['valute'].str[3:]
    df['value'] = df['salary']
    del df['valute']
    df['date'] = list(data['quotes'].keys())[0]
    df = df[['date', 'currency_source', 'currency','value']]
    df.to_csv(csv_file, sep=",", encoding="utf-8", index=False)
    print(f"Transformed data for {context['ds']}")

# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(csv_file, table_name,**context):
    date = context['ds']

    # Чтение данных из CSV
    df = pd.read_csv(csv_file)

    ch_hook = ClickHouseHook(clickhouse_conn_id='ch_client')

    table_exists = ch_hook.execute(f"EXISTS TABLE {table_name}")[0][0]
    if table_exists:
        ch_hook.execute(f'INSERT INTO {table_name} VALUES', df.to_dict('records'))
    print(f"Uploaded data for {context['ds']}")


# Определяем DAG, это контейнер для описания нашего пайплайна
dag = DAG(
    dag_id='ch_hook',
    schedule='@daily',      # Как часто запускать, счит. CRON запись
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 10),
    catchup=True,
    max_active_runs=1,
    tags=['custom'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)


# Задача для извлечения данных 
task_extract = PythonOperator(
    task_id='extract_data',          # Уникальное имя задачи
    python_callable=extract_data,    # Функция которая будет запущена (определена выше)
    
    # Параметры в виде списка которые будут переданы в функцию "extract_data"
    op_args=[url],
    dag=dag,                         # DAG к которому приклеплена задача
)

# Задачи для преобразования данных 
task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    # Передача аргументов через словарь, а не список
    op_kwargs = {
        's_file': '/opt/airflow/airflow_data/data_{{ ds }}.json'
	},
    dag=dag,
)

#Создаем таблицу
create_table = ClickHouseOperator(
        task_id='create_table',
        sql='''CREATE TABLE IF NOT EXISTS fedos_ch_hook 
        (date String, currency_source String, currency  String, value Float64)
        ENGINE = MergeTree()
        PRIMARY KEY (date)
        ORDER BY (date, currency_source, currency)''',
        clickhouse_conn_id='ch_client', 
        dag=dag,
        )

# Задачи для загрузки данных 
task_upload = PythonOperator(
    task_id='upload_to_clickhouse',
    python_callable=upload_to_clickhouse,
    op_args = [
        '/opt/airflow/airflow_data/data_{{ ds }}.csv',  # ← шаблон
        'fedos_ch_hook'
    ],
    dag=dag,
)

# Связываем задачи в соответствующих дагах. Посмотреть связь можно здесь 
task_extract >> task_transform >> create_table >> task_upload

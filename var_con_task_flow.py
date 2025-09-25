# Библиотеки для работы с XML, http, data frame и date
import requests as req
import pandas as pd
from datetime import datetime, timedelta
import json
from clickhouse_driver import Client

# Библиотеки для работы с Airflow
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable

HOST = BaseHook.get_connection("ch_client").host
USER = BaseHook.get_connection("ch_client").login
PASSWORD = BaseHook.get_connection("ch_client").password
DATABASE = BaseHook.get_connection("ch_client").schema



# Настройка подключения к ClickHouse
ch_client = Client(
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DATABASE
)
url = Variable.get("api_url")
save_path= Variable.get("save_path")

with DAG(
    'var_con_priority2',
    schedule='@daily',
    start_date=datetime(2024,1,1),
    end_date=datetime(2024,1,10),
    catchup=True,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    @task(task_id='extract_data',
        priority_weight=3)
    def extract_data(url: str, **context) -> str:
        date = context['ds']
        s_file = f'{save_path}/data_{date}.json'
        request = req.get(f'{url}&start_date={date}&end_date={date}')
        data = json.loads(request.text)
        with open(s_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Extracted data for {date}")
        return s_file

    @task(
        task_id='transform_data',
        priority_weight=2
          )
    def transform_data(s_file: str, **context) -> str:
        date = context['ds']
        csv_file = f'{save_path}/data_{date}.csv'
        with open(s_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        rows = []
        for k, v in data['quotes'].items():
            for keys, values in v.items():
                rows.append({
                    "valute": keys,
                    "salary": values,
                    "date": k
                })
    
        df = pd.DataFrame(rows)
        df['currency_source'] = df['valute'].str[:3]
        df['currency'] = df['valute'].str[3:]
        df['value'] = df['salary']
        del df['valute']
        df = df[['date', 'currency_source', 'currency', 'value']]
        df.to_csv(csv_file, sep=",", encoding="utf-8", index=False)
        print(f"Transformed data for {context['ds']}")
        return csv_file

    @task(
        task_id='upload_to_clickhouse',
        priority_weight=1
          )
    def upload_to_clickhouse(csv_file: str, table_name, client, **context):
        date = context['ds']
        df = pd.read_csv(csv_file)

        # Проверка на существование таблицы
        table_exists = client.execute(f"EXISTS TABLE {table_name}")[0][0]

        if not table_exists:
            client.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    date String,
                    currency_source String,
                    currency String,
                    value Float64
                ) ENGINE = MergeTree()
                PRIMARY KEY (date)
                ORDER BY (date, currency_source, currency)
            ''')

        ch_client.execute(f"ALTER TABLE {table_name} DELETE WHERE date = '{date}'")
        client.execute(f'INSERT INTO {table_name} VALUES', df.to_dict('records'))
        print(f"Uploaded data for {context['ds']}")

    # Определение порядка выполнения задач
    a=extract_data(url)
    b=transform_data(a)
    c=upload_to_clickhouse(b,'fedos_incremental_load', ch_client)

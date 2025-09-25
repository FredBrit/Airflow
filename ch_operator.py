# Библиотеки
import requests as req
import pandas as pd
from datetime import datetime, timedelta
import json
from clickhouse_driver import Client

# Airflow
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook

# Переменные
url = Variable.get("api_url")
save_path = Variable.get("save_path")


# --- Функция для отправки в Telegram ---
def send_telegram_failure(context):
    hook = TelegramHook(telegram_conn_id='telegram_id')
    hook.send_message({
        'text': f"❌ Ошибка в задаче!\n"
                f"Task: {context['task_instance'].task_id}\n"
                f"DAG: {context['dag'].dag_id}\n"
                f"Execution Time: {context['ts']}\n"
                f"Log: {context['task_instance'].log_url}"
    })




# DAG
with DAG(
    'ch_operator_tg_alert_1',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 10),
    catchup=True,
    max_active_runs = 1, # чтобы сначала выполнялись все таски за один день, потом за другой без паралеллизма
    tags=['ch_operator'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': send_telegram_failure,
    }
) as dag:

    @task(task_id='extract_data', priority_weight=3)
    def extract_data(url: str, **context) -> str:
        date = context['ds']
        s_file = f'{save_path}/data_{date}.json'
        request = req.get(f'{url}&start_date={date}&end_date={date}')
        data = json.loads(request.text)
        with open(s_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Extracted data for {date}")
        return s_file

    @task(task_id='transform_data', priority_weight=2)
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
        df = df[['date', 'currency_source', 'currency', 'value']]
        df.to_csv(csv_file, sep=",", encoding="utf-8", index=False)
        print(f"Transformed data for {date}")
        return csv_file

    
    create_table = ClickHouseOperator(
        task_id='create_table',
        sql='''CREATE TABLE IF NOT EXISTS fedos_ch_operator (
                    date String, 
                    currency_source String, 
                    currency String, 
                    value Float64
                ) 
                ENGINE = MergeTree()
                PRIMARY KEY (date)
                ORDER BY (date, currency_source, currency)''',
        clickhouse_conn_id='ch_client',  
        dag=dag,
    )

    @task(task_id='upload_to_clickhouse', priority_weight=1)
    def upload_to_clickhouse(csv_file: str, table_name: str, **context):
        from airflow.hooks.base import BaseHook

        # Получаем параметры подключения
        conn = BaseHook.get_connection("ch_client")
        client = Client(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            database=conn.schema
        )

        df = pd.read_csv(csv_file)
        date = context['ds']

        # Проверка на существование таблицы
        table_exists = client.execute(f"EXISTS TABLE {table_name}")[0][0]

        if table_exists:
            client.execute(f"ALTER TABLE {table_name} DELETE WHERE date = '{date}'")
        client.execute(f'INSERT INTO {table_name} VALUES', df.to_dict('records'))
        print(f"Uploaded data for {date}")

    # ✅ Правильный порядок выполнения
    a = extract_data(url)
    b = transform_data(a)
    c = create_table  
    d = upload_to_clickhouse(b, 'fedos_ch_operator')

    # Порядок выполнения
    a >> b >> c >> d

from datetime import datetime, timedelta
import logging
import random
import inspect
import pandas as pd
from clickhouse_driver import Client

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models.baseoperator import BaseOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

# Кастомный хук для ClickHouse
class ClickhouseTransferHook(ClickHouseHook):
    def __init__(self, clickhouse_conn_id='ch_client', source=None):
        super().__init__(clickhouse_conn_id=clickhouse_conn_id)
        self.source = source

    # Метод который преобразует csv в датафрейм
    def get_pandas_df(self, path):
        data_frame=pd.read_csv(path)
        return data_frame
    
    # Метод для вставки Data Frame в ClickHouse
    def insert_df_to_db(self, data_frame, table_name):

        # Преобразуем значения столбцов в список кортежей
        records = [tuple(row) for row in data_frame.values]
        self.log.info(f"Первые 2 записи: {records[:2]}")

        # Объект ClickHouseHook может используя подключение к бд выполнить SQL код
        self.execute(f"INSERT INTO {table_name} VALUES", records)
        self.log.info(f"Вставлено {len(records)} записей")
        


class ClickhouseTransferOperator(BaseOperator):
    def __init__(self, path, table_name, **kwargs):
        super().__init__(**kwargs)
        self.path = path
        self.table_name = table_name

    def execute(self, context):
        # Создание объекта хука
        self.hook = ClickhouseTransferHook()

        # Читаем CSV в DataFrame
        df = self.hook.get_pandas_df(self.path)

        # Вставляем в ClickHouse
        self.hook.insert_df_to_db(df, self.table_name)

# DAG
with DAG(
    'custom_ch_operator_2',
    schedule='@daily',
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['custom'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    
    create_table = ClickHouseOperator(
        task_id='create_table',
        sql='''CREATE TABLE IF NOT EXISTS fedos_ch_custom_operator 
        (date String, currency_source String, currency  String, value Float64)
        ENGINE = MergeTree()
        PRIMARY KEY (date)
        ORDER BY (date, currency_source, currency)''',
        clickhouse_conn_id='ch_client', 
        dag=dag,
        )

    transfer_data = ClickhouseTransferOperator(
        task_id='transfer_data', 
        path='/opt/airflow/airflow_data/data_2024-01-01.csv', 
        table_name = 'fedos_ch_custom_operator',
        dag=dag)


create_table >> transfer_data

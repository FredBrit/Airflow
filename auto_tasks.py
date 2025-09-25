from datetime import datetime, timedelta
import os 
import json
import logging
from clickhouse_driver import Client

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


folder_path = "/opt/airflow/plugins/sql/"


# DAG
with DAG(
    'auto_task4',
    schedule='@daily',
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs = 1, # чтобы сначала выполнялись все таски за один день, потом за другой без паралеллизма
    tags=['automatization'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    
    
    task_list = []

    logger = logging.getLogger(__name__)

    # Получаем список всех .sql файлов в папке
    sql_files = sorted([
        f for f in os.listdir(folder_path)
        if f.endswith('.sql')
    ])

    for idx, filename in enumerate(sql_files, start=1):
        filepath = os.path.join(folder_path, filename)

        # Читаем содержимое SQL-файла
        with open(filepath, 'r', encoding='utf-8') as file:
            sql_query = file.read()

    # Создаем задачу ClickHouseOperator
        task = ClickHouseOperator(
            task_id=f'execute_sql_{idx}_{filename.replace(".sql", "").replace(" ", "_")}',
            sql=f"CREATE VIEW {filename.replace(".sql", "").replace(" ", "_")}_{idx} AS {sql_query}",
            clickhouse_conn_id='ch_client',
            dag=dag,
        )

        task_list.append(task)    

    for i in range(len(task_list) - 1):
            logger.info(f"Текущий таск: {task_list[i]}")
            logger.info(f"Запрос: {sql_query}")
            task_list[i] >> task_list[i + 1]    

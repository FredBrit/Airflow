import requests as req
import pandas as pd
from datetime import datetime, timedelta
import json
from clickhouse_driver import Client

# Библиотеки для работы с Airflow
from airflow import DAG
from airflow.decorators import task



with DAG(
    'xcom_return',
    schedule='@daily',
    start_date=datetime.now() - timedelta(days=1),
    catchup=True,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    @task(task_id='push_task')
    def push_function(**context):
        context['ti'].xcom_push(key='fedor', value="britanov")
        print(context['ti'])
        return None
        


    @task(task_id='pull_task')
    # Функция которая извлечет это значение
    def pull_function(**context):
        
        ti = context['ti'] # Получим из контекста экземпляр задачи
        value_pulled = ti.xcom_pull(key='fedor', task_ids='push_task') # Через экземпляр обратимся по имени к Xcom
        
        print(f"Value from XCom pull: {value_pulled}")
        return value_pulled
                

    push_function() >> pull_function() 

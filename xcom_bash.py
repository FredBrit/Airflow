import requests as req
import pandas as pd
from datetime import datetime, timedelta
import json
from clickhouse_driver import Client

# Библиотеки для работы с Airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator



with DAG(
    'bash_xcom',
    schedule='@daily',
    start_date=datetime.now() - timedelta(days=1),
    catchup=True,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    echo = BashOperator(
        task_id='echo',
        bash_command='echo "Hello, I am a value!"',
        # do_xcom_push=True - в Airflow 3.0.3 не требуется, включено по умолчанию
    )
        


    @task.bash(task_id='fetch')
    def fetch():
        return """
        echo 'XCom fetched: {{ ti.xcom_pull(task_ids=["echo"]) }}'
        """
                

    echo >> fetch()

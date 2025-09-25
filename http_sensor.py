from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator          # ✅ Правильно
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

dag = DAG(
    dag_id='http_sensor_2_tasks',
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['http'],
)

# --- Задача 1: Запуск отчёта ---
start_report_task = HttpOperator(  # ✅ Используем HttpOperator, а не SimpleHttpOperator
    task_id='start_report_task',
    http_conn_id='report_api',
    endpoint='start_report',
    method='GET',
    response_check=lambda response: 'report_id' in response.json(),
    response_filter=lambda response: response.json(),
    do_xcom_push=True,
    dag=dag,
)

# --- Задача 2: Проверка статуса отчёта ---
check_report_task = HttpSensor(
    task_id='check_report_task',
    http_conn_id='report_api',
    endpoint='check_report/{{ ti.xcom_pull(task_ids="start_report_task",key="return_value")["report_id"] }}',
    method='GET',
    response_check=lambda response: response.json().get('message') == 'The report is ready!',
    poke_interval=2,
    timeout=120,
    mode='poke',
    dag=dag,
)

start_report_task >> check_report_task

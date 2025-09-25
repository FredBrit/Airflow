from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator          # ✅ Правильно
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import logging

dag = DAG(
    dag_id='http_sensor_with_custom_functions',
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['http'],
)

# --- Задача 1: Запуск отчёта и полностью сохраняем весь JSON ответа для работы с ним в других тасках
start_report_task = HttpOperator(
    task_id='start_report_task',
    http_conn_id='report_api',
    endpoint='start_report',
    method='GET',
    do_xcom_push=True,  # ← Сохраняем ответ
    response_filter=lambda response: response.json(), # преобразуем response.text в JSON
    dag=dag,
)

# --- Задача 2: Какая-то логика проверки
def validate_and_extract(**kwargs):
    logger = logging.getLogger(__name__)
    
    ti = kwargs['ti']
    
    # Получаем JSON из предыдущей задачи
    response_data = ti.xcom_pull(task_ids='start_report_task')
    report_id = response_data['report_id']
    ti.xcom_push(key='report_id', value=report_id)
    logger.info(f'Значение report_id:{report_id}')
    return report_id

validate_task = PythonOperator(
    task_id='validate_task',
    python_callable=validate_and_extract,
    dag=dag,
)

# --- Задача 3: Проверить готовность отчёта ---
check_report_task = HttpSensor(
    task_id='check_report_task',
    http_conn_id='report_api',
    # Берём report_id из XCom (ключ 'report_id')
    endpoint='check_report/{{ ti.xcom_pull(task_ids="validate_task", key="report_id") }}',
    method='GET',
    response_check=lambda response: response.json().get('status') == 'ready',
    poke_interval=2,
    timeout=120,
    mode='poke',
    dag=dag,
)

start_report_task >> validate_task >> check_report_task

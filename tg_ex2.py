from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException

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

# --- Функция, которая выбрасывает исключение ---
def raise_exc():
    raise AirflowException("Тестовая ошибка!")

# --- DAG ---
dag = DAG(
    'tg_example3',
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    default_args={
        'on_failure_callback': send_telegram_failure,
        'retries':0,
    },
    tags=['telegram'],
)

# --- Задача ---
python_task = PythonOperator(
    task_id='raise_exception_task',
    python_callable=raise_exc,
    dag=dag,
)

python_task

from datetime import datetime, timedelta
import logging
import random
import inspect

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow import __version__ as AIRFLOW_VERSION

logging.info(f"BaseHook class location: {inspect.getfile(BaseHook)}")
logging.info(f"[DEBUG] Airflow version: {AIRFLOW_VERSION}")


# Кастомный хук
class CustomHook(BaseHook):
    def __init__(self, source=None):
        super().__init__()          # ✅ Исправлено: убрали source=source
        self.source = source        # ✅ Сохраняем для внутреннего использования

    # Метод который генерирует случайное число
    def random_number(self):
        random.seed(10, version=2)
        return random.randint(0, 10)


# Кастомный оператор
class CustomOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.hook = None

    # Метод отправляет в XCom некоторое значение
    def execute(self, context):
        # Передаём источник вызова — например, task_id
        self.hook = CustomHook(source=f"{self.dag_id}.{self.task_id}")
        result = self.hook.random_number()
        logging.info(f"Dag_ID: {self.dag_id}")
        logging.info(f"Task_ID: {self.task_id}")
        logging.info(f"Generated random number: {result}")
        return result


# DAG
with DAG(
    'custom_operator_ex6',
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

    t1 = CustomOperator(task_id='task_1')

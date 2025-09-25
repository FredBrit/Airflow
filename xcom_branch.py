from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator  # В Airflow 3.0.3 DummyOperator переименован
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

with DAG(
    'xcom_branch',
    schedule=None,  # В Airflow 3.x schedule вместо schedule_interval
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Добавляем для безопасности
    tags=['example', 'branching'],  # Метки для организации в UI
) as dag:

    # 1. Стартовая задача - генерирует значение (10) и сохраняет в XCom
    start_task = BashOperator(
        task_id='start_task',
        bash_command="echo 10",  # Вывод команды автоматически сохраняется в XCom
    )

    # 2. Функция для ветвления
    @task.branch(task_id='branch_decision')  # Новый декоратор для ветвления
    def branch_function(**context):
        # Получаем значение из XCom предыдущей задачи
        xcom_value = int(context['ti'].xcom_pull(task_ids='start_task'))

        print(f'Значние XCom:{xcom_value}')
        
        if xcom_value >= 5:
            return 'continue_task'  # ID следующей задачи
        return 'stop_task'

    # 3. Альтернативные пути выполнения (пустые операторы)
    continue_task = EmptyOperator(task_id='continue_task')
    stop_task = EmptyOperator(task_id='stop_task')

    # Определение зависимостей
    start_task >> branch_function() >> [continue_task, stop_task]

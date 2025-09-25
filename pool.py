from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta

# Создаем DAG
dag = DAG(
    'pool_dag',
    schedule=None,  # Запускать вручную
    start_date=datetime(2024, 1, 1),
)

# Задачи с ограничением на параллельное выполнение через пул
task_1 = BashOperator(
    task_id='task_1',
    pool='ex_pool',  # Указываем пул для ограничения параллельности
    bash_command="sleep 3",
    priority_weight=6,
    dag=dag,
)

task_2 = BashOperator(
    task_id='task_2',
    pool='ex_pool',
    bash_command="sleep 3",
    priority_weight=4,
    dag=dag,
)

task_3 = BashOperator(
    task_id='task_3',
    pool='ex_pool',
    bash_command="sleep 3",
    priority_weight=2,
    dag=dag,
)

task_11 = BashOperator(
    task_id='task_11',
    pool='ex_pool',  # Указываем пул для ограничения параллельности
    bash_command="sleep 3",
    priority_weight=5,
    dag=dag,
)

task_22 = BashOperator(
    task_id='task_22',
    pool='ex_pool',
    bash_command="sleep 3",
    priority_weight=3,
    dag=dag,
)

task_33 = BashOperator(
    task_id='task_33',
    pool='ex_pool',
    bash_command="sleep 3",
    priority_weight=1,
    dag=dag,
)

task_1 >> task_11
task_2 >> task_22
task_3 >> task_33

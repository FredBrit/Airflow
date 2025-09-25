from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Определяем параметры DAG
dag = DAG('con',
    schedule=None,  # Запускать вручную
    start_date=datetime.now() - timedelta(days=1),  
)

# Создание задачи для выполнения SQL-запроса
create_table_task = PostgresOperator(
    task_id='run_sql',
    postgres_conn_id='meta_airflow',  # Имя подключения к PostgreSQL в Airflow
    sql=""" select count(1) from dag """,
    dag=dag,
)

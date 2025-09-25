# Библиотеки для работы с Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta

# Определяем DAG
dag = DAG(
    dag_id='chain',
    schedule='@daily',      # Как часто запускать, счит. CRON запись
    start_date=datetime.now() - timedelta(days=1), # Начало и конец загрузки (такая запись всегад будет ставить вчерашний день)
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)

t1 = EmptyOperator(task_id='task_1', dag=dag)
t2 = EmptyOperator(task_id='task_2',dag=dag)
t3 = EmptyOperator(task_id='task_3',dag=dag)
t4 = EmptyOperator(task_id='task_4',dag=dag)
t5 = EmptyOperator(task_id='task_5',dag=dag)
t6 = EmptyOperator(task_id='task_6',dag=dag)
t7 = EmptyOperator(task_id='task_7',dag=dag)



chain([t1,t2], t5, t7)
chain([t2,t4], t6, t7)
chain([t4,t3], t6, t7)
chain(t4,t7)

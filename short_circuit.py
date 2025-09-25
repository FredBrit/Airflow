from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator  # В Airflow 3.0+ DummyOperator переименован в EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='short_circuit_ex',
    start_date=datetime(2023, 1, 1),  # Используем явную дату вместо days_ago
    schedule=None,  # В Airflow 3.0+ schedule вместо schedule_interval
    catchup=False,  # Добавляем для безопасности
    tags=['example', 'branching'],  # Добавляем теги для организации
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    # 1. Создаем ShortCircuitOperator задачи
    # True-условия продолжат выполнение последующих задач
    cond_true = ShortCircuitOperator(
        task_id='condition_is_True',
        python_callable=lambda: True,  # Всегда возвращает True
    )

    cond_false = ShortCircuitOperator(
        task_id='condition_is_False',
        python_callable=lambda: False,  # Всегда возвращает False
    )
    
    cond_true1 = ShortCircuitOperator(
        task_id='condition_is_True1',
        python_callable=lambda: True,
    )

    cond_false1 = ShortCircuitOperator(
        task_id='condition_is_False1',
        python_callable=lambda: False,
    )
    
    cond_true2 = ShortCircuitOperator(
        task_id='condition_is_True2',
        python_callable=lambda: True,
    )

    cond_false2 = ShortCircuitOperator(
        task_id='condition_is_False2',
        python_callable=lambda: False,
    )

    # 2. Создаем цепочки выполнения
    # Эта цепочка выполнится полностью, так как все условия True
    chain(cond_true, cond_true1, cond_true2)
    
    # Эта цепочка прервется после cond_false, так как он возвращает False
    # cond_false1 и cond_false2 не будут выполнены
    chain(cond_false, cond_false1, cond_false2)

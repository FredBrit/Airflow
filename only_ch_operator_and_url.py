from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime, timedelta


url = 'https://api.exchangerate.host/timeframe?access_key=043dc9dad696914726d3064e9d917294&source=USD'

sql_query="""
    INSERT INTO fedos_only_ch
    SELECT '{{ ds }}' AS date, currency_source, currency, toFloat64(value) AS value
    FROM (
        SELECT
            arrayJoin(
                splitByChar(
                    ',',
                    replaceAll(
                        replaceAll(
                            replaceAll(
                                visitParamExtractRaw(column, '{{ ds }}'),
                                '{', ''),
                            '}', ''),
                        '"', '')
                )
            ) AS col1,
            LEFT(col1, 3) AS currency_source,
            SUBSTRING(col1, 4, 3) AS currency,
            SUBSTRING(col1, 8) AS value
        FROM url(
            'https://api.exchangerate.host/timeframe?access_key=043dc9dad696914726d3064e9d917294&source=USD&start_date={{ ds }}&end_date={{ ds }}',
            LineAsString,
            'column String'
        )
    )
    """





dag = DAG(
    dag_id='only_ch2',
    schedule='@daily',      # Как часто запускать, счит. CRON запись
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 4),
    catchup=True,
    max_active_runs=1,
    tags=['custom'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)


create_table = ClickHouseOperator(
        task_id='create_table',
        sql='''CREATE TABLE IF NOT EXISTS fedos_only_ch 
        (date String, currency_source String, currency  String, value Float64)
        ENGINE = MergeTree()
        PRIMARY KEY (date)
        ORDER BY (date, currency_source, currency)''',
        clickhouse_conn_id='ch_client', 
        dag=dag,
        )


insert_data = ClickHouseOperator(
    task_id='insert_data',
    sql=sql_query,  # SQL запрос, который нужно выполнить
    clickhouse_conn_id='ch_client',  # ID подключения, настроенное в Airflow
    dag=dag,
)

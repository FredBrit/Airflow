import requests as req  # для выполнения HTTP-запросов
import pandas as pd  # для обработки данных
from datetime import datetime, timedelta  # для работы с датами
from clickhouse_driver import Client  # для подключения к ClickHouseimport requests as req
import xml.etree.ElementTree as ET


# Библиотеки для работы с Airflow

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models.baseoperator import BaseOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os


api_xml = Variable.get("api_xml")

# Функция для извлечения данных с API Центрального банка и сохранения их в локальный файл
def extract_data(**context):
    ds = context['ds']

    url = context["templates_dict"]["url"]
    date = context["templates_dict"]["date"]
    s_file = context["templates_dict"]["s_file"]
    # Выполняем GET-запрос для получения данных за указанную дату
    request = req.get(f"{url}?date_req={date}")  
    with open(s_file, "w", encoding="utf-8") as tmp_file:
        tmp_file.write(request.text)  # Записываем текст ответа в файл

# Функция для обработки данных в формате XML и преобразования их в CSV
def transform_data(**context):
    ds = context['ds']

    s_file = context["templates_dict"]["s_file"]
    csv_file = context["templates_dict"]["csv_file"]
    date = context["templates_dict"]["date"]

    rows = list()  # Список для хранения значений из XML
    
    # Парсинг XML дерева
    parser = ET.XMLParser(encoding="utf-8")
    tree = ET.parse(s_file, parser=parser).getroot()
    
    # Получение необходимых значений
    for child in tree.findall("Valute"):
        num_code = child.find("NumCode").text
        char_code = child.find("CharCode").text
        nominal = child.find("Nominal").text
        name = child.find("Name").text
        value = child.find("Value").text
        
        # Добавление одной записи в список для последующих преобразований
        rows.append((num_code, char_code, nominal, name, value)) 

    # Считывание полученного списка в Data Frame, добавление даты и запись в CSV файл
    data_frame = pd.DataFrame(
        rows, columns=["num_code", "char_code", "nominal", "name", "value"]
    )
    # Меняем формат даты в более привычный для последующего join
    data_frame['date'] = datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
    
    data_frame.to_csv(csv_file, sep=",", encoding="utf-8", index=False) 

# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(table_name,**context):
    
    ds = context['ds']

    csv_file = context["templates_dict"]["csv_file"]

    # Чтение данных из CSV
    data_frame = pd.read_csv(csv_file)  

    ch_hook = ClickHouseHook(clickhouse_conn_id='ch_client')
    ch_hook.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records'))
    
# Определяем DAG, это контейнер для описания нашего пайплайна
dag = DAG(
    dag_id='xml_join5',
    schedule='@daily',      
    
    # Начало и конец загрузки 
    start_date=datetime(2024,1,1),
    end_date=datetime(2024,1,5),
    max_active_runs=1,
    catchup=True,
    tags=['xml','join']
)

# Задача для извлечения данных 
task_extract = PythonOperator(
    task_id='extract_data',          # Уникальное имя задачи
    python_callable=extract_data,    # Функция которая будет запущена (определена выше)
    
    # Параметры в виде списка которые будут переданы в функцию "extract_data"
    templates_dict={
        'url': api_xml,
        'date': '{{ macros.ds_format(ds, "%Y-%m-%d", "%d/%m/%Y") }}',
        's_file': '/opt/airflow/airflow_data/extracted_data_{{ ds }}.xml'
    },
    dag=dag,                         # DAG к которому приклеплена задача
)

# Задачи для преобразования данных 
task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    # Передача аргументов через словарь, а не список
    templates_dict={
        's_file': '/opt/airflow/airflow_data/extracted_data_{{ ds }}.xml',
        'csv_file': '/opt/airflow/airflow_data/transformed_data_{{ ds }}.csv',
        'date': '{{ macros.ds_format(ds, "%Y-%m-%d", "%d/%m/%Y") }}'
    },
    dag=dag,
)

# Оператор для выполнения запроса
create_currency_table = ClickHouseOperator(
    task_id='create_currency_table',
    sql='''CREATE TABLE IF NOT EXISTS fedos_currency_cb (num_code Int64, char_code String, nominal Int64, name String, value String, date String) 
        ENGINE = MergeTree()
        PRIMARY KEY (date)
        ORDER BY (date)''',
    clickhouse_conn_id='ch_client', 
    dag=dag,
)

# Задачи для загрузки данных 
task_upload = PythonOperator(
    task_id='upload_to_clickhouse',
    python_callable=upload_to_clickhouse,
    templates_dict={
        'csv_file': '/opt/airflow/airflow_data/transformed_data_{{ ds }}.csv',
        'table_name': 'fedos_currency_cb'
    },
    dag=dag,
)

# Оператор для выполнения запроса
create_new_order_table = ClickHouseOperator(
    task_id='create_new_order_table',
    sql='''CREATE TABLE IF NOT EXISTS fedos_order_usd (date String, order_id Int64, purchase_rub Float64, purchase_usd Float64) 
        ENGINE = MergeTree()
        PRIMARY KEY (date)
        ORDER BY (date)''',
    clickhouse_conn_id='ch_client', 
    dag=dag,
)

# Оператор для выполнения запроса
insert_join = ClickHouseOperator(
    task_id='insert_join',
    sql=""" INSERT INTO fedos_order_usd 
            SELECT date, order_id, purchase_rub, purchase_rub / toFloat64(replace(value, ',', '.')) as purchase_usd
            FROM airflow.orders t1
                left join (select * from sandbox.fedos_currency_cb where name = 'Доллар США' and date = '{{ds}}') t2 on t1.date = t2.date
            where date = '{{ds}}' """,
    clickhouse_conn_id='ch_client', 
    dag=dag,
)

# Связываем задачи в соответствующих дагах. Посмотреть связь можно здесь 
task_extract >> task_transform >> [create_currency_table, create_new_order_table] >> task_upload >> insert_join

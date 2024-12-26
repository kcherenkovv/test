import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from transform_script import transfrom 
import tempfile


# Настройки DAG
default_args = {
    'owner': 'kiriiill-cherenkov',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='client_activity_dag',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    max_active_runs=1,
    description='DAG для расчета активности клиентов по продуктам.',
)

AIRFLOW_HOME = "/home/airflow" # Задаем напрямую путь
DATA_PATH = os.path.join(AIRFLOW_HOME, 'data')
INPUT_PATH = os.path.join(DATA_PATH, 'input')
OUTPUT_PATH = os.path.join(DATA_PATH, 'output')

def extract(**context):
    """Функция извлечения данных"""
    profit_data = pd.read_csv(os.path.join(INPUT_PATH, 'profit_table.csv'))
    
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp_file:
        profit_data.to_csv(tmp_file.name, index=False)
        file_path = tmp_file.name
        
    context['task_instance'].xcom_push(key="profit_data_path", value=file_path)

def transform(**context):
    """Функция преобразования данных"""
    file_path = context['task_instance'].xcom_pull(task_ids='extract', key='profit_data_path')
    profit_data = pd.read_csv(file_path)
    date = context['ds']  # Используем макрос `ds` из Airflow
    transformed_data = transfrom(profit_data, date)
    
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp_file:
        transformed_data.to_csv(tmp_file.name, index=False)
        transformed_data_path = tmp_file.name
    
    context['task_instance'].xcom_push(key="transformed_data_path", value=transformed_data_path)
    os.remove(file_path) # Удаляем временный файл после обработки


def load(**context):
    """Функция загрузки данных"""
    transformed_data_path = context['task_instance'].xcom_pull(task_ids='transform', key='transformed_data_path')
    transformed_data = pd.read_csv(transformed_data_path)
    
    transformed_data.to_csv(os.path.join(OUTPUT_PATH, 'flags_activity.csv'), mode='a', header=False, index=False)
    
    os.remove(transformed_data_path) # Удаление временного файла после загрузки

with dag:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task

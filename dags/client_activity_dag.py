import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from transform_script import transform


def extract_data(**kwargs):
    """Извлечение данных из файла profit_table.csv"""
    profit_table = pd.read_csv('data/profit_table.csv')
    profit_table.to_parquet('data/profit_table_extracted.parquet', index=False)
    kwargs['ti'].xcom_push(key='profit_table_path', value='data/profit_table_extracted.parquet')


def transform_data(**kwargs):
    """Обработка данных с использованием функции transform из transform_script.py"""
    ti = kwargs['ti']
    profit_table = pd.read_parquet(ti.xcom_pull(key='profit_table_path'))
    transformed_data = transform(profit_table, kwargs['ds']) # Для передачи текущей даты используем макрос `ds` из Airflow
    ti.xcom_push(key='transformed_data', value=transformed_data.to_dict())


def load_data(**kwargs):
    """Сохранение обработанных данных в файл flags_activity.csv"""
    ti = kwargs['ti']
    transformed_data = pd.DataFrame(ti.xcom_pull(key='transformed_data'))
    transformed_data.to_csv('data/flags_activity.csv', mode='a', index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'client_activity_dag',
        default_args=default_args,
        description='ETL для витрины активности клиентов',
        schedule_interval='0 0 5 * *',  # Запуск каждый месяц 5-го числа
        start_date=datetime(2023, 10, 1),
        catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )


    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )


    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task

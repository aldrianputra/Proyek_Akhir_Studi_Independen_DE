from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Definisikan argumen default untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your.email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_data',
    default_args=default_args,
    description='ETL data ke HIVE',
    schedule_interval='@yearly',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

#Mendefinisikan Spark Job
    load_data = SparkSubmitOperator (
        task_id = 'load_data',
        application='/home/aldrian/airflow/dags/loaddataspark.py',
        conn_id='aldriansparkconn',
        total_executor_cores='2',
        executor_memory='2g',
        executor_cores='1',
        num_executors='1',
        driver_memory='2g',
        verbose= False
)

    load_data

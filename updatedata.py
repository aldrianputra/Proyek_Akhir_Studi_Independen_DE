from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your.email@example.com'],  # Ganti dengan email Anda
    'email_on_failure': True,  # Aktifkan email jika gagal
    'email_on_retry': False, 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'update_data_olap',
    default_args=default_args,
    description='ETL data ke HIVE',
    schedule_interval=timedelta(days=1),  # Interval harian
    start_date=datetime(2024, 1, 1),
    catchup=False  # Tidak perlu mengejar eksekusi yang terlewat
) as dag:

    update_data = SparkSubmitOperator(
        task_id='update_data',
        application='/home/aldrian/airflow/dags/updatedataspark.py',
        conn_id='aldriansparkconn',  
        total_executor_cores='2',
        executor_memory='2g',
        executor_cores='1',
        num_executors='1',
        driver_memory='2g',
    )

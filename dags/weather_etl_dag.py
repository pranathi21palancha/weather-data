import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Add the project root directory to the Python path
dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(dag_folder, '..'))
sys.path.append(project_root)

# Now import your scripts
from scripts.api_client import fetch_weather_data
from scripts.data_transformer import transform_weather_data
from scripts.data_loader import load_data, create_spark_session, create_tables

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='A DAG for weather data ETL process using Spark',
    schedule_interval=timedelta(days=1),
)

def etl_process():
    try:
        spark = create_spark_session()
        raw_data = fetch_weather_data()
        if not raw_data:
            raise ValueError("No data fetched from the API")
        fact_df, dim_df = transform_weather_data(spark, raw_data)
        load_data(fact_df, dim_df)
        spark.stop()
    except Exception as e:
        print(f"Error in ETL process: {str(e)}")
        raise

with dag:
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
    )
    
    etl_task = PythonOperator(
        task_id='weather_etl_process',
        python_callable=etl_process,
    )
    
    create_tables_task >> etl_task

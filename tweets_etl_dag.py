from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tweets_etl import process_tweets, load_tweets_to_db

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 27),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=15)
}

dag = DAG(
    'twitter-dag',
    default_args = default_args,
    description="twitter dag",
    schedule_interval=None,
    catchup=False 
)

run_etl = PythonOperator(
    task_id = 'twitter_etl',
    python_callable= process_tweets,
    dag = dag
)

load_data_sql = PythonOperator(
    task_id = 'load_tweets',
    python_callable=load_tweets_to_db,
    dag = dag
)

run_etl >> load_data_sql
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'russia_losses_etl',
    default_args=default_args,
    description='ETL pipeline for Russia Losses Equipment data',
    schedule_interval='@daily',
)

t1 = BashOperator(
    task_id='extract_data',
    bash_command='python D:\Finpro-Data Engineer\Finpro-DE5\extract_data.py',
    dag=dag,
)

t2 = BashOperator(
    task_id='transform_data',
    bash_command='python D:\Finpro-Data Engineer\Finpro-DE5\transform_data.py',
    dag=dag,
)

t3 = BashOperator(
    task_id='load_data',
    bash_command='python D:\Finpro-Data Engineer\Finpro-DE5\load_data.py',
    dag=dag,
)

t1 >> t2 >> t3

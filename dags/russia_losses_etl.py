from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'russia_losses_etl',
    default_args=default_args,
    description='ETL pipeline for Russia Losses Equipment data',
    schedule_interval='@daily',
)

def extract_data():
    import pandas as pd
    import requests
    logging.info("Ekstraksi data dimulai.")
    try:
        # Ekstraksi data dari file CSV
        df_csv = pd.read_csv('D:\Finpro-Data Engineer\Finpro-DE5\Data\russia_losses_equipment.csv')
        # Ekstraksi data dari API
        response = requests.get('https://api.example.com/data')
        data_api = response.json()
        df_api = pd.DataFrame(data_api)
        # Gabungkan data
        df_combined = pd.merge(df_csv, df_api, on='key_column')
        df_combined.to_csv('D:\Finpro-Data Engineer\Finpro-DE5\extracted_data.csv', index=False)
        logging.info("Ekstraksi data selesai.")
    except Exception as e:
        logging.error("Error dalam proses ekstraksi data: %s", e)
        raise

def transform_data():
    import pandas as pd
    logging.info("Transformasi data dimulai.")
    try:
        # Muat data gabungan
        df_combined = pd.read_csv('D:\Finpro-Data Engineer\Finpro-DE5\extracted_data.csv')
        # Normalisasi data
        df_combined['losses_normalized'] = (df_combined['losses'] - df_combined['losses'].mean()) / df_combined['losses'].std()
        # Standarisasi data
        df_combined['date'] = pd.to_datetime(df_combined['date'])
        df_combined['month'] = df_combined['date'].dt.month
        # Buat kolom tambahan
        df_combined['loss_category'] = df_combined['losses'].apply(lambda x: 'High' if x > 100 else 'Low')
        df_combined.to_csv('D:\Finpro-Data Engineer\Finpro-DE5\transformed_data.csv', index=False)
        logging.info("Transformasi data selesai.")
    except Exception as e:
        logging.error("Error dalam proses transformasi data: %s", e)
        raise

def load_data():
    import pandas as pd
    from sqlalchemy import create_engine
    logging.info("Proses load data dimulai.")
    try:
        # Koneksi ke PostgreSQL
        engine = create_engine('postgresql://postgres:admin123@localhost:5432/postgres')
        # Muat data yang telah ditransformasi
        df_transformed = pd.read_csv('D:\Finpro-Data Engineer\Finpro-DE5\transformed_data.csv')
        df_transformed.to_sql('russia_losses_equipment', engine, index=False, if_exists='replace')
        logging.info("Proses load data selesai.")
    except Exception as e:
        logging.error("Error dalam proses load data: %s", e)
        raise

t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

t1 >> t2 >> t3

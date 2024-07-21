import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)

def load_data():
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

if __name__ == "__main__":
    load_data()

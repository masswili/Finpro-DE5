import pandas as pd
import requests
import logging

logging.basicConfig(level=logging.INFO)

def extract_data():
    logging.info("Ekstraksi data dimulai.")
    try:
        # Ekstraksi data dari file CSV
        df_csv = pd.read_csv('D:\Finpro-Data Engineer\Finpro-DE5\Data\russia_losses_equipment_correction.csv')
        # Ekstraksi data dari API
        # response = requests.get('https://api.example.com/data')
        # data_api = response.json()
        # df_api = pd.DataFrame(data_api)
        # Gabungkan data
        df_combined = pd.merge(df_csv, on='key_column')
        df_combined.to_csv('D:\Finpro-Data Engineer\Finpro-DE5\extracted_data.csv', index=False)
        logging.info("Ekstraksi data selesai.")
    except Exception as e:
        logging.error("Error dalam proses ekstraksi data: %s", e)
        raise

if __name__ == "__main__":
    extract_data()

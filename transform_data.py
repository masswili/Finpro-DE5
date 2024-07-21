import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def transform_data():
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

if __name__ == "__main__":
    transform_data()

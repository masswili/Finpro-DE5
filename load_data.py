import pandas as pd
from sqlalchemy import create_engine

# Memuat data yang telah ditransformasi
data_path = 'transformed_data.csv'
df = pd.read_csv(data_path)

# Koneksi ke PostgreSQL
engine = create_engine('postgresql://postgres:admin123@localhost:5432/postgres')

# Memuat DataFrame ke PostgreSQL
df.to_sql('russia_losses_equipment', engine, index=False, if_exists='replace')

import pandas as pd

# Memuat dataset
data_path = 'extracted_data.csv'
df = pd.read_csv(data_path)

# Membersihkan data
df.dropna(inplace=True) 

# Mengubah tipe data
df['date'] = pd.to_datetime(df['date'])

# Simpan data yang sudah ditransformasi
df.to_csv('transformed_data.csv', index=False)

import pandas as pd

# Memuat dataset
data_path = 'russia_losses_equipment.csv'
df = pd.read_csv(data_path)

# Tinjau data
print(df.head())

# Menyimpan data yang diekstrak ke file baru (opsional)
df.to_csv('extracted_data.csv', index=False)

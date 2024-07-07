import pandas as pd

# Memuat dataset
data_path = 'D:/Finpro-Data Engineer/Finpro-DE5/Data/russia_losses_equipment_correction.csv'  # Gunakan garis miring ke depan (/) atau dua backslash (\\)
df = pd.read_csv(data_path)

# Tinjau data
print(df.head())

# Menyimpan data yang diekstrak ke file baru (opsional)
df.to_csv('extracted_data.csv', index=False)

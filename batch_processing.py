from pyspark.sql import SparkSession

# Inisialisasi Spark
spark = SparkSession.builder.appName("RussiaLossesBatchProcessing").getOrCreate()

# Memuat data
data_path = 'transformed_data.csv'
df_spark = spark.read.csv(data_path, header=True, inferSchema=True)

# Transformasi data dengan PySpark
df_spark_cleaned = df_spark.dropna()

# Menyimpan data ke PostgreSQL
df_spark_cleaned.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "russia_losses_equipment_cleaned") \
    .option("user", "postgres") \
    .option("password", "admin123") \
    .save()

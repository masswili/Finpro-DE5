from pyspark.sql import SparkSession
import time

# Inisialisasi Spark
spark = SparkSession.builder \
    .appName("RussiaLossesBatchProcessing") \
    .config("spark.jars", "D:\Finpro-Data Engineer\Finpro-DE5\\postgresql-42.7.3.jar") \
    .config("spark.local.dir", "D:\Finpro-Data Engineer\Finpro-DE5\spark-temp") \
    .getOrCreate()

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
    .option("driver", "org.postgresql.Driver") \
    .save()

# Stop Spark session
time.sleep(5)
spark.stop()

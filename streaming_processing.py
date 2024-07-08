from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

# Inisialisasi Spark
spark = SparkSession.builder.appName("RussiaLossesStreamingProcessing").getOrCreate()

# Definisi skema untuk data streaming
schema = StructType([
    StructField("date", DateType(), True),
    StructField("equipment", StringType(), True),
    StructField("losses", IntegerType(), True)
])

# Membaca stream dari Kafka
df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "russia_losses_equipment") \
    .load()

# Transformasi data
df_stream = df_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Menyimpan data ke PostgreSQL
df_stream.writeStream \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "russia_losses_equipment_streaming") \
    .option("user", "postgres") \
    .option("password", "admin123") \
    .start() \
    .awaitTermination()

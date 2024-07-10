from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

# Inisialisasi Spark
spark = SparkSession.builder \
    .appName("RussiaLossesStreamingProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

# Define skema untuk streaming data
schema = StructType([
    StructField("date", DateType(), True),
    StructField("equipment", StringType(), True),
    StructField("losses", IntegerType(), True)
])

# Read stream from Kafka
df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "russia_losses_equipment") \
    .load()

# Transform data
df_stream = df_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Save data to PostgreSQL with checkpointing
checkpoint_location = "D:\Finpro-Data Engineer\Finpro-DE5\checkpoint\dir"


query = df_stream.writeStream \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "russia_losses_equipment_streaming") \
    .option("user", "postgres") \
    .option("password", "admin123") \
    .option("driver", "org.postgresql.Driver") \
    .option("checkpointLocation", checkpoint_location) \
    .outputMode("append") \
    .start()

query.awaitTermination()

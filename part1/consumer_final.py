from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config
import uuid
import os

# Назва топіку
my_name = "vika"
topic_name = f'{my_name}_athlete_event_results_output'

# Set up Spark packages
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
    "pyspark-shell"
)



# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())


# Читання потоку даних із Kafka
df_event = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

json_schema = StructType([
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("avg_height", DoubleType(), True),
    StructField("avg_weight", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])


# Маніпуляції з даними
clean_df = df_event.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*") \
    .drop('key', 'value') \
    .withColumnRenamed("key_deserialized", "key") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .withColumn("sport", col("value_json.sport")) \
    .withColumn("medal", col("value_json.medal")) \
    .withColumn("sex", col("value_json.sex")) \
    .withColumn("country_noc", col("value_json.country_noc")) \
    .withColumn("avg_height", col("value_json.avg_height")) \
    .withColumn("avg_weight", col("value_json.avg_weight")) \
    .withColumn("timestamp", col("value_json.timestamp")) \
    .drop("value_json", "value_deserialized", "offset", "topic", "key", "partition", "timestampType") 

#Виведення даних на екран
displaying_df = clean_df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "FP/checkpoints_2") \
    .start() \
    .awaitTermination()

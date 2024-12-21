from kafka import KafkaProducer
from configs import kafka_config
from pyspark.sql.functions import *
import json
import uuid
import time
import os
from pyspark.sql import SparkSession

# Set up Spark packages
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
    "pyspark-shell"
)

# # Налаштування конфігурації SQL бази даних
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_event_results"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"


# Створення Spark сесії
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .appName("JDBCToKafka") \
    .getOrCreate()

# Читання даних з SQL бази даних
df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password) \
    .load()

df.show()

# Назва топіку
my_name = "vika"
topic_name = f'{my_name}_athlete_event_results_input'

df.printSchema()
# Підготовка даних для запису в Kafka: формування ключ-значення
prepare_to_kafka_df = df.select(
    to_json(struct(
        col("edition_id"), col("country_noc"), col("sport"), col("event"),
        col("result_id"), col("athlete"), col("athlete_id"), col("pos"),
        col("medal"), col("isTeamSport")
    )).alias("value")
)


#Запис оброблених даних у Kafka-топік 
prepare_to_kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", topic_name) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"admin\" password=\"VawEzo1ikLtrA8Ug8THa\";") \
    .option("kafka.metadata.max.age.ms", "120000")\
    .save()


print("Data has been written to Kafka.")



from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config
import uuid
import os

# Назва топіку
my_name = "vika"
topic_name = f'{my_name}_athlete_event_results_input'
topic_for_send = f'{my_name}_athlete_event_results_output'

# Set up Spark packages
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
    "pyspark-shell"
)

# Функція для обробки кожної партії даних
def foreach_batch_function(batch_df, batch_id):

    prepare_to_kafka_df = batch_df.select(
    to_json(struct(*batch_df.columns)).alias("value")
    )
    # Відправка збагачених даних до Kafka
    prepare_to_kafka_df \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
        .option("topic", topic_for_send) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"admin\" password=\"VawEzo1ikLtrA8Ug8THa\";") \
        .option("kafka.metadata.max.age.ms", "120000")\
        .save()


    # Збереження збагачених даних до MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "viktoriia.enriched_results") \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()
    
# Налаштування конфігурації SQL бази даних
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_bio"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# # Створення Spark сесії
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("JDBCToKafka") \
    .getOrCreate()

# Етап 1. Читання даних з SQL бази даних
df_bio = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password) \
    .load()

df_bio.show()

# Етап 2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами.
df_bio_cleaned = df_bio.filter(
    (col("height").isNotNull()) & 
    (col("weight").isNotNull()) &
    (col("height").cast(DoubleType()).isNotNull()) &  # Перевірка, чи є зріст числом
    (col("weight").cast(DoubleType()).isNotNull())   # Перевірка, чи є вага числом
)
df_bio_cleaned.show()


# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())


# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
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

# Визначення схеми для JSON,
# оскільки Kafka має структуру ключ-значення, а значення має формат JSON. 
json_schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", StringType(), True)
])

# Етап 3. Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.
clean_df = df_event.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*") \
    .drop('key', 'value') \
    .withColumnRenamed("key_deserialized", "key") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .withColumn("sport", col("value_json.sport")) \
    .withColumn("athlete_id", col("value_json.athlete_id")) \
    .withColumn("medal", col("value_json.medal")) \
    .drop("value_json", "value_deserialized", "offset", "topic", "key", "partition", "timestampType") 
    

# Етапи 4 і 5
df_cross = clean_df.join(df_bio_cleaned, "athlete_id")\
    .groupby(["sport", "medal", "sex", "country_noc"]).agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
    .withColumn("timestamp", current_timestamp())

# Етап 6. Запис в кафка топік і в базу даних
df_cross \
            .writeStream \
            .foreachBatch(foreach_batch_function) \
            .outputMode("update") \
            .start() \
            .awaitTermination()










from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, BooleanType
import logging

# Настройка логгирования
logging.basicConfig(level=logging.INFO)

# Инициализация Spark Session
spark = SparkSession.builder \
    .appName("TelegramChannelAnomalyDetection") \
    .getOrCreate()

# Определение схемы данных

schema = StructType([
    StructField("time", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("text", StringType(), True),
    StructField("has_media", BooleanType(), True),
])

try:
    # Чтение данных с определенной схемой
    df = spark.readStream \
        .format("parquet") \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .load("parquet_data")
    
    query = df.writeStream \
            .format("console") \
            .start()
    query.awaitTermination()

    # Преобразование данных
    messages_count = df \
        .withWatermark("time", "1 minute") \
        .groupBy(window(col("time"), "10 seconds"), col("source")) \
        .count()

    threshold_value = 1
    # Условие для обнаружения аномалии (в данном случае, всплеск сообщений)
    anomaly_condition = (col("count") > threshold_value)

    # Вывод результатов
    query = messages_count \
        .where(anomaly_condition) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()
except Exception as e:
    logging.error(f"Произошла ошибка: {e}")

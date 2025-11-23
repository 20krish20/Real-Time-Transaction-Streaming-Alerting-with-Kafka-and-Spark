# Databricks notebook: Bronze from Kafka (Confluent Cloud)

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.sql.functions import from_json, col, current_timestamp
import logging

# ---------- Logger ----------
logger = logging.getLogger("bronze_from_kafka")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# ---------- Kafka / Confluent Cloud config ----------
# USE THE SAME VALUES THAT WORKED IN YOUR PYTHON PRODUCER
kafka_bootstrap = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
kafka_topic = "payments.raw"

kafka_api_key = "72I4WJOBIQ6RXQUG"
kafka_api_secret = """cfltyu0QtjanAa5kCwxmWXaP4UK02I3iTKyF+xl3yteMRRHXNBWuEjclbqvJdQdQ"""

logger.info("Kafka config loaded")
logger.info(f"Bootstrap: {kafka_bootstrap}, topic: {kafka_topic}")
logger.info(f"API key prefix: {kafka_api_key[:4]}*** (secret hidden)")

# ---------- Delta / Bronze config ----------
bronze_table = "payments_bronze"
# checkpoint_base = "/mnt/checkpoints/real_time_payments"
# checkpoint_path = f"{checkpoint_base}/bronze_from_kafka"
checkpoint_path = "dbfs:/checkpoints/real_time_payments/bronze_from_kafka"


# ---------- Schema for the JSON payload ----------
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("card_id",       StringType(), False),
    StructField("customer_id",   StringType(), False),
    StructField("merchant_id",   StringType(), True),
    StructField("amount",        DoubleType(), True),
    StructField("currency",      StringType(), True),
    StructField("country",       StringType(), True),
    StructField("channel",       StringType(), True),
    StructField("event_time",    StringType(), True),
])

logger.info("Starting Bronze streaming read from Kafka")

# ---------- Read from Kafka ----------
logger.info("Building kafka_options and creating readStream...")
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap,
    "subscribe": kafka_topic,
    "startingOffsets": "latest",

    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.ssl.endpoint.identification.algorithm": "https",
    "kafka.sasl.jaas.config": (
        f'org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="{kafka_api_key}" password="{kafka_api_secret}";'
    ),
}

raw_stream = (
    spark.readStream
         .format("kafka")
         .options(**kafka_options)
         .load()
)
logger.info(f"raw_stream created. isStreaming={raw_stream.isStreaming}")

# ---------- Parse JSON and add ingestion timestamp ----------
parsed = (
    raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("raw_ingested_at", current_timestamp())
)
logger.info("parsed DataFrame created")

# ---------- Write to Delta Bronze table ----------

bronze_root = "dbfs:/real_time_payments/bronze"
bronze_path = f"{bronze_root}/{bronze_table}"

query = (
    parsed.writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint_path)
          .outputMode("append")
          .start(bronze_path)   # <-- IMPORTANT: write to path, not table
)

logger.info(f"Streaming query object created. id={query.id}, name={query.name}")

# print("Is stream active? ", query.isActive)
# print("Status:", query.status)
# print("Exception:", query.exception)
# print("Last progress:", query.lastProgress)

# for s in spark.streams.active:
#     print("---- ACTIVE STREAM ----")
#     print("Name:", s.name)
#     print("Id:", s.id)
#     print("Is active:", s.isActive)
#     print("Status:", s.status)
#     print("Exception:", s.exception)
#     print("Last progress:", s.lastProgress)


# spark.sql("USE default")

# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {bronze_table}
# USING delta
# LOCATION '{bronze_path}'
# """)

# spark.sql("SELECT * FROM payments_bronze LIMIT 10").show()
# spark.sql("SELECT COUNT(*) FROM payments_bronze").show()

# from time import sleep
# for _ in range(5):
#     print(query.status)
#     sleep(5)

# display(
#     raw_stream.selectExpr(
#         "CAST(key AS STRING) AS k",
#         "CAST(value AS STRING) AS v"
#     )
# )

# Databricks notebook

from pyspark.sql.functions import (
    col, to_timestamp, to_date, hour,
    when, current_timestamp
)
from databricks.utils.logging_utils import get_logger

logger = get_logger("silver_enrichment")

bronze_table = "payments_bronze"
silver_table = "payments_silver"
checkpoint_base = "/mnt/checkpoints/real_time_payments"
checkpoint_path = f"{checkpoint_base}/silver_enrichment"

high_value_threshold = 2000.0  # or load from a config table

logger.info("Starting Silver streaming from Bronze table")

# Example DIM_CARD; in real usage, load from path/table
# dim_card = spark.table("dim_card")
dim_card = spark.createDataFrame(
    [
        ("card_1", "cust_1", "US", "LOW"),
        ("card_2", "cust_2", "US", "MEDIUM"),
    ],
    ["card_id", "customer_id_dim", "home_country", "risk_tier"]
)

bronze_stream = spark.readStream.table(bronze_table)

base = (
    bronze_stream
    .withColumn("event_time_ts", to_timestamp("event_time"))
    .withColumn("txn_date", to_date("event_time_ts"))
    .withColumn("txn_hour", hour("event_time_ts"))
)

enriched = (
    base.join(dim_card, on="card_id", how="left")
        .withColumn("is_high_value", col("amount") > high_value_threshold)
        .withColumn(
            "is_international",
            (col("country").isNotNull()) & (col("home_country").isNotNull()) &
            (col("country") != col("home_country"))
        )
        .withColumn("processed_at", current_timestamp())
)

query = (
    enriched.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_path)
            .outputMode("append")
            .table(silver_table)
)

logger.info(f"Streaming query started. Writing to table {silver_table}")
query.awaitTermination()

# Databricks notebook

from pyspark.sql.functions import current_timestamp, col
from databricks.utils.logging_utils import get_logger

logger = get_logger("load_to_snowflake")

silver_table = "payments_silver"
snowflake_table = "FACT_TRANSACTIONS_STREAMING"

# In Databricks, store these in secret scopes:
sfOptions = {
    "sfURL": dbutils.secrets.get("snowflake", "url"),
    "sfUser": dbutils.secrets.get("snowflake", "user"),
    "sfPassword": dbutils.secrets.get("snowflake", "password"),
    "sfDatabase": "PAYMENTS_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "PAYMENTS_WH",
    "sfRole": "PAYMENTS_ROLE",
}

logger.info("Reading from Silver table")
df = spark.table(silver_table)

# Map columns to Snowflake schema
sf_df = (
    df.select(
        col("transaction_id").alias("TRANSACTION_ID"),
        col("card_id").alias("CARD_ID"),
        col("customer_id").alias("CUSTOMER_ID"),
        col("merchant_id").alias("MERCHANT_ID"),
        col("amount").alias("AMOUNT"),
        col("currency").alias("CURRENCY"),
        col("country").alias("COUNTRY"),
        col("channel").alias("CHANNEL"),
        col("event_time_ts").alias("EVENT_TIME_UTC"),
        col("txn_date").alias("TXN_DATE"),
        col("txn_hour").alias("TXN_HOUR"),
        col("is_high_value").alias("IS_HIGH_VALUE"),
        col("is_international").alias("IS_INTERNATIONAL"),
    )
    .withColumn("INGESTED_AT", current_timestamp())
)

logger.info(f"Writing to Snowflake table {snowflake_table}")

(
    sf_df.write
         .format("snowflake")
         .options(**sfOptions)
         .option("dbtable", snowflake_table)
         .mode("append")
         .save()
)

logger.info("Load to Snowflake completed")

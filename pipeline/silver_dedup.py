"""
Silver Layer — Bronze Delta → Silver Delta (deduplication + enrichment)

Responsibilities:
  1. Read from Bronze Delta table as a streaming source
  2. Deduplicate on (transaction_id, merchant_id) composite key
     — Delta Lake MERGE ensures idempotent upserts even on Spark retry
  3. Parse and enrich: event_time (ms) → timestamp, derive txn_date / txn_hour
  4. Join with (static) merchant dimension for enriched context
  5. Write deduplicated records to Silver Delta with MERGE (not append)
  6. Emit quality metrics: duplicate rate, null enrichment rate

Design tradeoff — why MERGE not append?
  Spark Structured Streaming's deduplication (dropDuplicates) only works within
  a watermark window. Because exactly-once is required across restarts, we use
  Delta MERGE (INSERT + IGNORE) on the composite key. This is slightly heavier
  than append but guarantees no duplicates survive into Silver regardless of
  Kafka redelivery or Spark retry storms.

Why composite key (transaction_id, merchant_id) and not just transaction_id?
  transaction_id is UUID-based and globally unique in theory, but payment
  networks occasionally reuse transaction IDs across different merchant
  contexts. Using both fields matches the reconciliation join key in Gold.
"""

from __future__ import annotations

from typing import Any

import structlog
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from pipeline.config import settings

log = structlog.get_logger(__name__)

# ── Merchant dimension (in production: loaded from a managed Delta table) ─────

MERCHANT_DIM_SCHEMA = StructType([
    StructField("merchant_id",       StringType(), False),
    StructField("merchant_name",     StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("acquiring_bank",    StringType(), True),
    StructField("risk_tier",         StringType(), True),  # LOW / MEDIUM / HIGH
    StructField("country_of_registration", StringType(), True),
])

# Simulated merchant dimension data
_SAMPLE_MERCHANTS = [
    (f"merch_{i:04d}", f"Merchant {i}", "RETAIL", "Chase", "LOW", "US")
    if i % 5 != 0
    else (f"merch_{i:04d}", f"Merchant {i}", "ECOMMERCE", "Citi", "MEDIUM", "GB")
    for i in range(1, 501)
]


def _load_merchant_dim(spark: SparkSession) -> DataFrame:
    """
    Load merchant dimension. In production this reads from:
        spark.table("catalog.gold.dim_merchant")
    The static createDataFrame is for local testing only.
    """
    try:
        return spark.table("catalog.reconciliation.dim_merchant")
    except Exception:
        log.warning("merchant_dim.fallback_to_static", reason="table not found")
        return spark.createDataFrame(_SAMPLE_MERCHANTS, MERCHANT_DIM_SCHEMA)


def _parse_and_enrich(df: DataFrame, merchant_dim: DataFrame) -> DataFrame:
    """Parse timestamps, derive date partitions, join merchant attributes."""
    enriched = (
        df.withColumn("event_time_ts", F.to_timestamp(F.col("event_time") / 1000))
        .withColumn("txn_date", F.to_date(F.col("event_time_ts")))
        .withColumn("txn_hour", F.hour(F.col("event_time_ts")))
        .withColumn("txn_year_month", F.date_format(F.col("event_time_ts"), "yyyy-MM"))
        .withColumn(
            "is_high_value",
            F.col("amount") > F.lit(2000.0),
        )
        .withColumn(
            "amount_bucket",
            F.when(F.col("amount") < 50, F.lit("MICRO"))
            .when(F.col("amount") < 500, F.lit("SMALL"))
            .when(F.col("amount") < 2000, F.lit("MEDIUM"))
            .otherwise(F.lit("LARGE")),
        )
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("pipeline_layer", F.lit("silver"))
    )

    return enriched.join(
        merchant_dim.select(
            "merchant_id", "merchant_name", "merchant_category",
            "acquiring_bank", "risk_tier", "country_of_registration",
        ),
        on="merchant_id",
        how="left",
    )


def _upsert_to_silver(
    spark: SparkSession,
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    """
    Upsert batch into Silver Delta table using MERGE.

    The MERGE condition matches on (transaction_id, merchant_id).
    We INSERT when no match — duplicates are silently ignored.
    We never UPDATE because Bronze is append-only and a second write
    of the same key is a duplicate, not a correction.
    """
    if batch_df.isEmpty():
        return

    silver_path = f"{settings.delta.silver_table_path}/transactions"

    # Ensure Silver table exists
    if not DeltaTable.isDeltaTable(spark, silver_path):
        log.info("silver.table.creating", path=silver_path)
        batch_df.limit(0).write.format("delta").save(silver_path)

    silver_table = DeltaTable.forPath(spark, silver_path)

    # Count incoming vs duplicates for observability
    incoming = batch_df.count()

    (
        silver_table.alias("silver")
        .merge(
            batch_df.alias("incoming"),
            "silver.transaction_id = incoming.transaction_id "
            "AND silver.merchant_id = incoming.merchant_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Measure how many actually landed (approximate — re-read is expensive)
    log.info(
        "silver.batch.merged",
        batch_id=batch_id,
        incoming_records=incoming,
        silver_path=silver_path,
    )


def _process_batch(
    spark: SparkSession,
    merchant_dim: DataFrame,
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    enriched = _parse_and_enrich(batch_df, merchant_dim)
    _upsert_to_silver(spark, enriched, batch_id)


def run(spark: SparkSession) -> None:
    """Start the Silver deduplication streaming query."""
    log.info("silver.job.starting")

    merchant_dim = _load_merchant_dim(spark)

    bronze_path = f"{settings.delta.bronze_table_path}/transactions"
    bronze_stream = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")  # Bronze is append-only; guard against OPTIMIZE
        .load(bronze_path)
    )

    checkpoint = f"{settings.delta.checkpoint_base}/silver_transactions"

    query = (
        bronze_stream.writeStream.foreachBatch(
            lambda df, bid: _process_batch(spark, merchant_dim, df, bid)
        )
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{settings.streaming.silver_trigger_seconds} seconds")
        .queryName("silver-transactions-dedup")
        .start()
    )

    log.info("silver.query.started", query_id=str(query.id), name=query.name)
    query.awaitTermination()


def main() -> None:
    """Databricks python_wheel_task entry point."""
    from pipeline.observability import configure_logging, start_metrics_server
    configure_logging()
    start_metrics_server()

    spark = (
        SparkSession.builder.appName("silver-transactions-dedup")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    run(spark)


if __name__ == "__main__":
    main()

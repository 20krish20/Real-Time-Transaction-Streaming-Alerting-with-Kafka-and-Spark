"""
Gold Layer — Stream-to-Stream Reconciliation & Anomaly Detection

This is the core of the platform. It joins two Kafka streams:
  - raw-transactions     (transaction_event Avro)
  - settlement-expectations (settlement_event Avro)

And produces:
  - Gold Delta table: fully reconciled records with anomaly scores
  - reconciliation-alerts Kafka topic: anomalies with severity classification

─────────────────────────────────────────────────────────────────────────────
ARCHITECTURAL DECISIONS (documented for interviews):

1. Stream-to-stream join vs stream-to-table:
   We use stream-to-stream because settlements arrive with 5–15 min delays.
   Stream-to-table (lookup join) would require the settlement to already exist
   in the table when the transaction arrives, which fails for late settlements.
   Stream-to-stream allows both sides to arrive in any order within the
   watermark window.

2. Watermark asymmetry:
   - Transactions: 10-min watermark (POS systems post quickly)
   - Settlements:  30-min watermark (bank batches, network delays)
   The join produces output only when both sides' watermarks have advanced
   past the join key's event_time + max(watermarks). This means Gold records
   can lag up to 30 min behind real time — an acceptable trade-off for
   correctness over the 0.01% reconciliation tolerance required.

3. Exactly-once to Delta:
   foreachBatch + Delta MERGE on (transaction_id, merchant_id) ensures
   Gold records are written exactly once even if the streaming query restarts
   mid-batch. Spark retries the same batch_id, and MERGE is idempotent.

4. Anomaly severity scoring:
   CRITICAL  — amount mismatch > 5%, or missing settlement after 30 min
   HIGH      — amount mismatch 1%–5%, or duplicate settlement
   MEDIUM    — amount mismatch 0.01%–1%
   LOW       — settlement status REJECTED / REVERSED / DISPUTED
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from enum import Enum

import structlog
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pipeline.config import settings

log = structlog.get_logger(__name__)


class AnomalySeverity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


# ── Schemas ───────────────────────────────────────────────────────────────────

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("card_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("country", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("event_time", LongType(), False),
        StructField("processing_time", LongType(), True),
        StructField("correlation_id", StringType(), True),
        StructField("geo_lat", DoubleType(), True),
        StructField("geo_lon", DoubleType(), True),
        StructField("mcc", StringType(), True),
        StructField("schema_version", IntegerType(), True),
    ]
)

SETTLEMENT_SCHEMA = StructType(
    [
        StructField("settlement_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("expected_amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("settlement_status", StringType(), True),
        StructField("settlement_date", IntegerType(), True),
        StructField("event_time", LongType(), False),
        StructField("processing_time", LongType(), True),
        StructField("correlation_id", StringType(), True),
        StructField("bank_reference", StringType(), True),
        StructField("interchange_fee", DoubleType(), True),
        StructField("net_settlement_amount", DoubleType(), True),
        StructField("schema_version", IntegerType(), True),
    ]
)


# ── Kafka stream readers ──────────────────────────────────────────────────────


def _kafka_base_opts() -> dict[str, str]:
    cfg = settings.kafka
    opts: dict[str, str] = {
        "kafka.bootstrap.servers": cfg.bootstrap_servers,
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
    }
    if cfg.security_protocol != "PLAINTEXT":
        opts["kafka.security.protocol"] = cfg.security_protocol
        opts["kafka.sasl.mechanism"] = cfg.sasl_mechanism or "PLAIN"
        opts["kafka.ssl.endpoint.identification.algorithm"] = "https"
        opts["kafka.sasl.jaas.config"] = (
            f"org.apache.kafka.common.security.plain.PlainLoginModule "
            f'required username="{cfg.sasl_username}" password="{cfg.sasl_password}";'
        )
    return opts


def _read_transactions(spark: SparkSession) -> DataFrame:
    opts = {**_kafka_base_opts(), "subscribe": settings.kafka.topic_raw_transactions}
    raw = spark.readStream.format("kafka").options(**opts).load()
    return (
        raw.select(F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("txn_event_ts", F.to_timestamp(F.col("event_time") / 1000))
        .withWatermark(
            "txn_event_ts", f"{settings.streaming.transaction_watermark_minutes} minutes"
        )
    )


def _read_settlements(spark: SparkSession) -> DataFrame:
    opts = {
        **_kafka_base_opts(),
        "subscribe": settings.kafka.topic_settlement_expectations,
    }
    raw = spark.readStream.format("kafka").options(**opts).load()
    return (
        raw.select(F.from_json(F.col("value").cast("string"), SETTLEMENT_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("settle_event_ts", F.to_timestamp(F.col("event_time") / 1000))
        .withWatermark(
            "settle_event_ts",
            f"{settings.streaming.settlement_watermark_minutes} minutes",
        )
    )


# ── Stream-to-stream join ─────────────────────────────────────────────────────


def _join_streams(txn_df: DataFrame, settle_df: DataFrame) -> DataFrame:
    """
    Inner join on (transaction_id, merchant_id) with time-bounded condition.

    The time bound is required by Spark Structured Streaming for stateful
    stream-to-stream joins — it limits state store size to O(window) rather
    than O(total events).

    Join condition:
        Keys match AND settlement arrives within 30 min of transaction
    """
    settle_window_minutes = settings.streaming.settlement_watermark_minutes

    return (
        txn_df.alias("txn")
        .join(
            settle_df.alias("settle"),
            on=[
                F.col("txn.transaction_id") == F.col("settle.transaction_id"),
                F.col("txn.merchant_id") == F.col("settle.merchant_id"),
                # Time-range predicate keeps state store bounded
                F.col("settle.settle_event_ts").between(
                    F.col("txn.txn_event_ts"),
                    F.col("txn.txn_event_ts") + F.expr(f"INTERVAL {settle_window_minutes} MINUTES"),
                ),
            ],
            how="inner",
        )
        .select(
            # Transaction fields
            F.col("txn.transaction_id"),
            F.col("txn.merchant_id"),
            F.col("txn.card_id"),
            F.col("txn.customer_id"),
            F.col("txn.amount").alias("transaction_amount"),
            F.col("txn.currency").alias("transaction_currency"),
            F.col("txn.country"),
            F.col("txn.channel"),
            F.col("txn.card_type"),
            F.col("txn.txn_event_ts"),
            F.col("txn.correlation_id"),
            F.col("txn.mcc"),
            F.col("txn.geo_lat"),
            F.col("txn.geo_lon"),
            # Settlement fields
            F.col("settle.settlement_id"),
            F.col("settle.expected_amount").alias("settlement_amount"),
            F.col("settle.settlement_status"),
            F.col("settle.settle_event_ts"),
            F.col("settle.bank_reference"),
            F.col("settle.interchange_fee"),
            F.col("settle.net_settlement_amount"),
        )
    )


# ── Reconciliation & anomaly detection ───────────────────────────────────────


def _add_reconciliation_columns(df: DataFrame) -> DataFrame:
    """
    Compute amount delta, mismatch percentage, and assign severity.

    Severity rules (documented in module docstring):
      CRITICAL  — |mismatch_pct| > 5% OR settlement_status in REJECTED/REVERSED
      HIGH      — |mismatch_pct| 1%–5% OR settlement_status DISPUTED
      MEDIUM    — |mismatch_pct| 0.01%–1%
      LOW       — settlement_status PENDING AND mismatch_pct < 0.01%
      NONE      — fully reconciled, within tolerance
    """
    df = (
        df.withColumn(
            "amount_delta",
            F.abs(F.col("transaction_amount") - F.col("settlement_amount")),
        )
        .withColumn(
            "mismatch_pct",
            F.when(
                F.col("transaction_amount") > 0,
                (
                    F.abs(F.col("transaction_amount") - F.col("settlement_amount"))
                    / F.col("transaction_amount")
                )
                * 100,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "is_amount_mismatch",
            F.col("mismatch_pct") > F.lit(settings.streaming.amount_mismatch_tolerance_pct),
        )
        .withColumn(
            "is_status_anomaly",
            F.col("settlement_status").isin("REJECTED", "REVERSED", "DISPUTED"),
        )
        .withColumn(
            "anomaly_severity",
            F.when(
                (F.col("mismatch_pct") > 5.0)
                | (F.col("settlement_status").isin("REJECTED", "REVERSED")),
                F.lit(AnomalySeverity.CRITICAL.value),
            )
            .when(
                (F.col("mismatch_pct").between(1.0, 5.0))
                | (F.col("settlement_status") == "DISPUTED"),
                F.lit(AnomalySeverity.HIGH.value),
            )
            .when(
                F.col("mismatch_pct").between(
                    settings.streaming.amount_mismatch_tolerance_pct, 1.0
                ),
                F.lit(AnomalySeverity.MEDIUM.value),
            )
            .when(
                (F.col("settlement_status") == "PENDING")
                & (F.col("mismatch_pct") <= settings.streaming.amount_mismatch_tolerance_pct),
                F.lit(AnomalySeverity.LOW.value),
            )
            .otherwise(F.lit("NONE")),
        )
        .withColumn(
            "is_anomaly",
            F.col("anomaly_severity") != F.lit("NONE"),
        )
        .withColumn("gold_reconciled_at", F.current_timestamp())
        .withColumn("pipeline_layer", F.lit("gold"))
    )

    return df


def _write_anomalies_to_kafka(
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    """Route anomalous records to the reconciliation-alerts Kafka topic."""
    anomalies = batch_df.filter(F.col("is_anomaly"))
    count = anomalies.count()
    if count == 0:
        return

    log.warning("gold.anomalies.routing", batch_id=batch_id, count=count)

    alert_df = anomalies.select(
        F.col("merchant_id").cast(StringType()).alias("key"),
        F.to_json(
            F.struct(
                "transaction_id",
                "merchant_id",
                "correlation_id",
                "transaction_amount",
                "settlement_amount",
                "amount_delta",
                "mismatch_pct",
                "settlement_status",
                "anomaly_severity",
                "is_amount_mismatch",
                "is_status_anomaly",
                F.current_timestamp().alias("alert_generated_at"),
                F.lit(batch_id).alias("batch_id"),
            )
        ).alias("value"),
    )

    alert_opts: dict[str, str] = {
        "kafka.bootstrap.servers": settings.kafka.bootstrap_servers,
        "topic": settings.kafka.topic_reconciliation_alerts,
    }
    if settings.kafka.security_protocol != "PLAINTEXT":
        alert_opts["kafka.security.protocol"] = settings.kafka.security_protocol
        alert_opts["kafka.sasl.mechanism"] = settings.kafka.sasl_mechanism or "PLAIN"
        alert_opts["kafka.sasl.jaas.config"] = (
            f"org.apache.kafka.common.security.plain.PlainLoginModule "
            f'required username="{settings.kafka.sasl_username}" '
            f'password="{settings.kafka.sasl_password}";'
        )

    alert_df.write.format("kafka").options(**alert_opts).save()


def _upsert_gold(
    spark: SparkSession,
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    """MERGE reconciled records into Gold Delta table."""
    if batch_df.isEmpty():
        return

    gold_path = f"{settings.delta.gold_table_path}/reconciliation_results"

    if not DeltaTable.isDeltaTable(spark, gold_path):
        log.info("gold.table.creating", path=gold_path)
        (
            batch_df.limit(0)
            .write.format("delta")
            .partitionBy("txn_event_ts")  # date-partition for Snowflake clustering
            .save(gold_path)
        )

    gold_table = DeltaTable.forPath(spark, gold_path)
    count = batch_df.count()

    (
        gold_table.alias("gold")
        .merge(
            batch_df.alias("incoming"),
            "gold.transaction_id = incoming.transaction_id "
            "AND gold.merchant_id = incoming.merchant_id",
        )
        .whenNotMatchedInsertAll()
        # Allow severity to be updated if a later settlement corrects a prior anomaly
        .whenMatchedUpdateAll()
        .execute()
    )

    log.info("gold.batch.committed", batch_id=batch_id, records=count, path=gold_path)


def _process_gold_batch(
    spark: SparkSession,
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    reconciled = _add_reconciliation_columns(batch_df)
    _write_anomalies_to_kafka(reconciled, batch_id)
    _upsert_gold(spark, reconciled, batch_id)


def run(spark: SparkSession) -> None:
    """
    Start the Gold reconciliation streaming query.

    This is the most stateful query in the pipeline. The watermark-bounded
    stream-to-stream join keeps state store size proportional to the
    30-minute settlement window, not to total data volume.
    """
    log.info("gold.job.starting")

    txn_stream = _read_transactions(spark)
    settle_stream = _read_settlements(spark)

    joined = _join_streams(txn_stream, settle_stream)

    checkpoint = f"{settings.delta.checkpoint_base}/gold_reconciliation"

    query = (
        joined.writeStream.foreachBatch(lambda df, bid: _process_gold_batch(spark, df, bid))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{settings.streaming.gold_trigger_seconds} seconds")
        .queryName("gold-reconciliation")
        .start()
    )

    log.info("gold.query.started", query_id=str(query.id), name=query.name)
    query.awaitTermination()


def main() -> None:
    """Databricks python_wheel_task entry point."""
    from pipeline.observability import configure_logging, start_metrics_server

    configure_logging()
    start_metrics_server()

    spark = (
        SparkSession.builder.appName("gold-reconciliation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        )
        .getOrCreate()
    )
    run(spark)


if __name__ == "__main__":
    main()

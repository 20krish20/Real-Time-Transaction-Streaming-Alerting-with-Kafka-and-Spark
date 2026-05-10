"""
Snowflake Writer — Gold Delta → Snowflake

Reads the Gold Delta table in micro-batch mode and writes to Snowflake using
the Snowflake Spark Connector (preferred over JDBC for bulk-loading efficiency).

Production deployment options (documented for interviews):
  Option A (this file): Spark batch job triggered on Gold table COMMIT events
                        via Databricks Delta Live Tables or a Databricks workflow
                        with a DeltaTable file-trigger.
  Option B:            Snowflake Kafka Connector reading directly from
                        reconciliation-alerts topic — avoids Spark entirely for
                        the alert path but doesn't support Delta Gold layer sync.
  Option C:            Snowpipe Streaming via Snowflake's Kafka connector
                        ingesting directly from the Gold Kafka output.

We implement Option A because:
  - Gold Delta is the system of record (not Kafka which has 7-day retention)
  - We need the full reconciled row (not just the alert payload)
  - Spark connector supports bulk COPY, which is 10-100x faster than row INSERT

RBAC:
  Writes use the RECONCILIATION_ENGINEER role.
  The Snowflake Analyst role is read-only — enforced at the Snowflake level,
  not here. See snowflake/rbac.sql.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import structlog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from pipeline.config import settings

log = structlog.get_logger(__name__)

# ── Snowflake connector options ───────────────────────────────────────────────

def _build_sf_options(table: str) -> dict[str, str]:
    cfg = settings.snowflake
    opts: dict[str, str] = {
        "sfURL": f"{cfg.account}.snowflakecomputing.com",
        "sfUser": cfg.user,
        "sfDatabase": cfg.database,
        "sfSchema": cfg.schema_name,
        "sfWarehouse": cfg.warehouse,
        "sfRole": cfg.role,
        "dbtable": table,
        # Bulk load via Snowflake internal stage (faster than row-by-row)
        "sfCompress": "on",
        "truncate_table": "off",
        "usestagingtable": "on",
        "column_mapping": "name",  # map by column name, not position
    }

    if cfg.password:
        opts["sfPassword"] = cfg.password
    elif cfg.private_key_path:
        opts["pem_private_key"] = _load_private_key(cfg.private_key_path, cfg.private_key_passphrase)

    return opts


def _load_private_key(path: str, passphrase: Optional[str]) -> str:
    """Load RSA private key for Snowflake key-pair authentication."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

    with open(path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")


# ── Column mapping ────────────────────────────────────────────────────────────

def _map_reconciliation_columns(df: DataFrame) -> DataFrame:
    """
    Select and rename Gold columns to match the Snowflake RECONCILIATION_RESULTS DDL.
    Snowflake column names are uppercase by convention.
    """
    return df.select(
        F.col("transaction_id").alias("TRANSACTION_ID"),
        F.col("merchant_id").alias("MERCHANT_ID"),
        F.col("card_id").alias("CARD_ID"),
        F.col("customer_id").alias("CUSTOMER_ID"),
        F.col("settlement_id").alias("SETTLEMENT_ID"),
        F.col("transaction_amount").alias("TRANSACTION_AMOUNT"),
        F.col("settlement_amount").alias("SETTLEMENT_AMOUNT"),
        F.col("amount_delta").alias("AMOUNT_DELTA"),
        F.col("mismatch_pct").alias("MISMATCH_PCT"),
        F.col("transaction_currency").alias("CURRENCY"),
        F.col("country").alias("COUNTRY"),
        F.col("channel").alias("CHANNEL"),
        F.col("card_type").alias("CARD_TYPE"),
        F.col("mcc").alias("MCC"),
        F.col("settlement_status").alias("SETTLEMENT_STATUS"),
        F.col("anomaly_severity").alias("ANOMALY_SEVERITY"),
        F.col("is_anomaly").alias("IS_ANOMALY"),
        F.col("is_amount_mismatch").alias("IS_AMOUNT_MISMATCH"),
        F.col("is_status_anomaly").alias("IS_STATUS_ANOMALY"),
        F.col("txn_event_ts").alias("TXN_EVENT_TS"),
        F.col("settle_event_ts").alias("SETTLE_EVENT_TS"),
        F.col("correlation_id").alias("CORRELATION_ID"),
        F.col("bank_reference").alias("BANK_REFERENCE"),
        F.col("interchange_fee").alias("INTERCHANGE_FEE"),
        F.col("net_settlement_amount").alias("NET_SETTLEMENT_AMOUNT"),
        F.col("gold_reconciled_at").alias("RECONCILED_AT"),
        F.current_timestamp().alias("SF_LOADED_AT"),
    )


def _map_anomaly_columns(df: DataFrame) -> DataFrame:
    """Map anomaly-only records to the ANOMALY_LOG table schema."""
    return df.filter(F.col("IS_ANOMALY")).select(
        F.col("TRANSACTION_ID"),
        F.col("MERCHANT_ID"),
        F.col("ANOMALY_SEVERITY"),
        F.col("MISMATCH_PCT"),
        F.col("SETTLEMENT_STATUS"),
        F.col("CORRELATION_ID"),
        F.col("RECONCILED_AT").alias("DETECTED_AT"),
        F.col("SF_LOADED_AT"),
    )


# ── Write with retry ──────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=30),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def _write_to_snowflake(df: DataFrame, table: str) -> None:
    """Write a DataFrame to Snowflake with exponential-backoff retry."""
    opts = _build_sf_options(table)
    (
        df.write.format("net.snowflake.spark.snowflake")
        .options(**opts)
        .mode("append")
        .save()
    )
    log.info("snowflake.write.success", table=table, rows=df.count())


def _write_batch(
    spark: SparkSession,
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    if batch_df.isEmpty():
        return

    mapped = _map_reconciliation_columns(batch_df)

    try:
        _write_to_snowflake(mapped, settings.snowflake.table_reconciliation_results)
    except Exception as exc:
        log.error("snowflake.write.failed", table="RECONCILIATION_RESULTS", error=str(exc))
        raise

    anomaly_mapped = _map_anomaly_columns(mapped)
    if not anomaly_mapped.isEmpty():
        try:
            _write_to_snowflake(anomaly_mapped, settings.snowflake.table_anomaly_log)
        except Exception as exc:
            log.error("snowflake.write.failed", table="ANOMALY_LOG", error=str(exc))
            raise

    log.info("snowflake.batch.complete", batch_id=batch_id)


def run(spark: SparkSession) -> None:
    """
    Stream Gold Delta records to Snowflake.

    Uses Delta's CDF (Change Data Feed) to read only new/changed rows
    since the last checkpoint — avoids full-table scans on large Gold tables.
    """
    log.info("snowflake_writer.starting")

    gold_path = f"{settings.delta.gold_table_path}/reconciliation_results"

    gold_stream = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")      # Delta CDF — only new rows
        .option("startingVersion", "latest")
        .load(gold_path)
        # Filter out CDF system rows (_change_type = "update_preimage")
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )

    checkpoint = f"{settings.delta.checkpoint_base}/snowflake_writer"

    query = (
        gold_stream.writeStream.foreachBatch(
            lambda df, bid: _write_batch(spark, df, bid)
        )
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{settings.streaming.gold_trigger_seconds} seconds")
        .queryName("snowflake-gold-writer")
        .start()
    )

    log.info("snowflake_writer.query.started", query_id=str(query.id))
    query.awaitTermination()


def main() -> None:
    """Databricks python_wheel_task entry point."""
    from pipeline.observability import configure_logging, start_metrics_server
    configure_logging()
    start_metrics_server()

    spark = (
        SparkSession.builder.appName("snowflake-gold-writer")
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

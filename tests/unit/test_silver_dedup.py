"""
Unit tests for Silver layer — deduplication and enrichment logic.

Tests use the shared `spark` fixture and in-memory DataFrames.
No Delta Lake writes happen here (no tmp directory required).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from pipeline.silver_dedup import _parse_and_enrich

# ── Helpers ───────────────────────────────────────────────────────────────────


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _make_bronze_rows(spark: SparkSession, overrides: list[dict]) -> DataFrame:
    now = _now_ms()
    base = {
        "transaction_id": "txn_001",
        "merchant_id": "merch_0001",
        "card_id": "card_000001",
        "customer_id": "cust_00001",
        "amount": 100.0,
        "currency": "USD",
        "country": "US",
        "channel": "POS",
        "card_type": "VISA",
        "event_time": now,
        "processing_time": now,
        "correlation_id": "corr_001",
        "geo_lat": 40.7,
        "geo_lon": -74.0,
        "mcc": "5411",
        "schema_version": 1,
        "bronze_ingested_at": datetime.now(timezone.utc),
        "pipeline_layer": "bronze",
    }
    rows = []
    for override in overrides:
        row = {**base, **override}
        rows.append(Row(**row))
    return spark.createDataFrame(rows)


def _make_merchant_dim(spark: SparkSession):
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [
            StructField("merchant_id", StringType(), False),
            StructField("merchant_name", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("acquiring_bank", StringType(), True),
            StructField("risk_tier", StringType(), True),
            StructField("country_of_registration", StringType(), True),
        ]
    )
    rows = [
        ("merch_0001", "Merchant One", "RETAIL", "Chase", "LOW", "US"),
        ("merch_0002", "Merchant Two", "ECOMMERCE", "Citi", "MEDIUM", "GB"),
        ("merch_0003", "Merchant Three", "TRAVEL", "BoA", "HIGH", "US"),
    ]
    return spark.createDataFrame(rows, schema)


# ── Tests ─────────────────────────────────────────────────────────────────────


@pytest.mark.unit
class TestParseAndEnrich:
    def test_event_time_ts_parsed_from_epoch_ms(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["event_time_ts"] is not None

    def test_txn_date_derived(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["txn_date"] is not None

    def test_txn_hour_within_range(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert 0 <= result["txn_hour"] <= 23

    def test_is_high_value_false_below_threshold(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{"amount": 1999.99}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["is_high_value"] is False

    def test_is_high_value_true_above_threshold(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{"amount": 2000.01}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["is_high_value"] is True

    def test_amount_bucket_micro(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{"amount": 10.0}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["amount_bucket"] == "MICRO"

    def test_amount_bucket_small(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{"amount": 100.0}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["amount_bucket"] == "SMALL"

    def test_amount_bucket_medium(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{"amount": 1000.0}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["amount_bucket"] == "MEDIUM"

    def test_amount_bucket_large(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{"amount": 3000.0}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["amount_bucket"] == "LARGE"

    def test_merchant_enrichment_matched(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{"merchant_id": "merch_0002"}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["merchant_name"] == "Merchant Two"
        assert result["risk_tier"] == "MEDIUM"
        assert result["acquiring_bank"] == "Citi"

    def test_merchant_enrichment_unmatched_is_null(self, spark: SparkSession) -> None:
        """Merchants not in dim produce null enrichment columns (left join)."""
        df = _make_bronze_rows(spark, [{"merchant_id": "merch_9999"}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["merchant_name"] is None
        assert result["risk_tier"] is None

    def test_pipeline_layer_set_to_silver(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["pipeline_layer"] == "silver"

    def test_silver_processed_at_populated(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        assert result["silver_processed_at"] is not None

    def test_txn_year_month_derived(self, spark: SparkSession) -> None:
        df = _make_bronze_rows(spark, [{}])
        dim = _make_merchant_dim(spark)
        result = _parse_and_enrich(df, dim).collect()[0]
        # Format: "YYYY-MM"
        ym = result["txn_year_month"]
        assert ym is not None
        assert len(ym) == 7
        assert ym[4] == "-"

    def test_batch_of_multiple_rows(self, spark: SparkSession) -> None:
        """Enrich 3 different merchants in one batch — all should match."""
        rows = [
            {"transaction_id": "txn_001", "merchant_id": "merch_0001", "amount": 50.0},
            {"transaction_id": "txn_002", "merchant_id": "merch_0002", "amount": 1500.0},
            {"transaction_id": "txn_003", "merchant_id": "merch_0003", "amount": 4000.0},
        ]
        df = _make_bronze_rows(spark, rows)
        dim = _make_merchant_dim(spark)
        results = _parse_and_enrich(df, dim).collect()
        assert len(results) == 3

        by_txn = {r["transaction_id"]: r for r in results}
        assert by_txn["txn_001"]["amount_bucket"] == "SMALL"
        assert by_txn["txn_002"]["amount_bucket"] == "MEDIUM"
        assert by_txn["txn_003"]["amount_bucket"] == "LARGE"
        assert by_txn["txn_001"]["risk_tier"] == "LOW"
        assert by_txn["txn_002"]["risk_tier"] == "MEDIUM"
        assert by_txn["txn_003"]["risk_tier"] == "HIGH"

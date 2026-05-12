"""
Shared pytest fixtures for the merchant-reconciliation-platform test suite.

Fixtures:
  spark            — Local PySpark session with Delta Lake extensions
  sample_transactions — DataFrame of realistic transaction rows
  sample_settlements  — DataFrame of matching settlement rows
  sample_mismatched   — Settlements with amount mismatches / bad statuses
"""

from __future__ import annotations

import os
from collections.abc import Generator
from datetime import datetime, timezone

import pytest
from pyspark.sql import DataFrame, Row, SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Session-scoped Spark session with Delta Lake.

    Scope is 'session' (not 'function') to avoid the 60-second JVM startup
    cost on every test. Tests must NOT mutate shared state in the session.
    """
    os.environ.setdefault("PYSPARK_PYTHON", "python")

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("reconciliation-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")  # keep tests fast
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


@pytest.fixture
def sample_transactions(spark: SparkSession) -> DataFrame:
    now = _now_ms()
    rows = [
        Row(
            transaction_id="txn_001",
            merchant_id="merch_0001",
            card_id="card_000001",
            customer_id="cust_00001",
            amount=100.00,
            currency="USD",
            country="US",
            channel="POS",
            card_type="VISA",
            event_time=now,
            processing_time=now,
            correlation_id="corr_001",
            geo_lat=40.7128,
            geo_lon=-74.0060,
            mcc="5411",
            schema_version=1,
        ),
        Row(
            transaction_id="txn_002",
            merchant_id="merch_0002",
            card_id="card_000002",
            customer_id="cust_00002",
            amount=2500.00,
            currency="USD",
            country="GB",
            channel="ONLINE",
            card_type="MASTERCARD",
            event_time=now,
            processing_time=now,
            correlation_id="corr_002",
            geo_lat=None,
            geo_lon=None,
            mcc="5999",
            schema_version=1,
        ),
        Row(
            transaction_id="txn_003",
            merchant_id="merch_0001",
            card_id="card_000003",
            customer_id="cust_00003",
            amount=49.99,
            currency="USD",
            country="US",
            channel="CONTACTLESS",
            card_type="AMEX",
            event_time=now,
            processing_time=now,
            correlation_id="corr_003",
            geo_lat=34.0522,
            geo_lon=-118.2437,
            mcc="5812",
            schema_version=1,
        ),
    ]
    return spark.createDataFrame(rows)


@pytest.fixture
def sample_settlements(spark: SparkSession) -> DataFrame:
    """Perfectly matching settlements (no anomalies)."""
    now = _now_ms()
    rows = [
        Row(
            settlement_id="settle_001",
            transaction_id="txn_001",
            merchant_id="merch_0001",
            expected_amount=100.00,
            currency="USD",
            settlement_status="APPROVED",
            settlement_date=19847,
            event_time=now + 300_000,  # 5 min later
            processing_time=now + 300_000,
            correlation_id="corr_001",
            bank_reference="BNKREF100001",
            interchange_fee=1.80,
            net_settlement_amount=98.20,
            schema_version=1,
        ),
        Row(
            settlement_id="settle_002",
            transaction_id="txn_002",
            merchant_id="merch_0002",
            expected_amount=2500.00,
            currency="USD",
            settlement_status="APPROVED",
            settlement_date=19847,
            event_time=now + 600_000,  # 10 min later
            processing_time=now + 600_000,
            correlation_id="corr_002",
            bank_reference="BNKREF100002",
            interchange_fee=46.25,
            net_settlement_amount=2453.75,
            schema_version=1,
        ),
    ]
    return spark.createDataFrame(rows)


@pytest.fixture
def sample_mismatched_settlements(spark: SparkSession) -> DataFrame:
    """Settlements with various anomaly scenarios."""
    now = _now_ms()
    rows = [
        # 3% amount mismatch — HIGH severity
        Row(
            settlement_id="settle_010",
            transaction_id="txn_010",
            merchant_id="merch_0010",
            expected_amount=97.00,  # 3% less than 100.00
            currency="USD",
            settlement_status="APPROVED",
            settlement_date=19847,
            event_time=now,
            processing_time=now,
            correlation_id="corr_010",
            bank_reference=None,
            interchange_fee=1.75,
            net_settlement_amount=95.25,
            schema_version=1,
        ),
        # 8% amount mismatch + REVERSED — CRITICAL severity
        Row(
            settlement_id="settle_011",
            transaction_id="txn_011",
            merchant_id="merch_0011",
            expected_amount=920.00,  # 8% less than 1000.00
            currency="USD",
            settlement_status="REVERSED",
            settlement_date=19847,
            event_time=now,
            processing_time=now,
            correlation_id="corr_011",
            bank_reference="BNKREF200001",
            interchange_fee=16.56,
            net_settlement_amount=903.44,
            schema_version=1,
        ),
        # DISPUTED status — HIGH severity
        Row(
            settlement_id="settle_012",
            transaction_id="txn_012",
            merchant_id="merch_0012",
            expected_amount=50.00,
            currency="USD",
            settlement_status="DISPUTED",
            settlement_date=19847,
            event_time=now,
            processing_time=now,
            correlation_id="corr_012",
            bank_reference=None,
            interchange_fee=0.90,
            net_settlement_amount=49.10,
            schema_version=1,
        ),
    ]
    return spark.createDataFrame(rows)

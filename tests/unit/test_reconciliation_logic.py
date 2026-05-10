"""
Unit tests for Gold reconciliation logic.

Tests are self-contained — they use the shared `spark` fixture but do not
touch Kafka, Delta Lake, or Snowflake. All logic under test is pure DataFrame
transformation, making tests fast and deterministic.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F

from pipeline.gold_reconciliation import (
    _add_reconciliation_columns,
    AnomalySeverity,
)
from pipeline.bronze_ingestion import _add_validation_flags


# ─── helpers ──────────────────────────────────────────────────────────────────

def _make_joined_row(
    spark: SparkSession,
    transaction_amount: float,
    settlement_amount: float,
    settlement_status: str = "APPROVED",
    transaction_id: str = "txn_test",
    merchant_id: str = "merch_0001",
) -> DataFrame:
    """Build a minimal joined DataFrame that mimics the stream-to-stream join output."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    rows = [
        Row(
            transaction_id=transaction_id,
            merchant_id=merchant_id,
            card_id="card_000001",
            customer_id="cust_00001",
            transaction_amount=transaction_amount,
            settlement_amount=settlement_amount,
            transaction_currency="USD",
            country="US",
            channel="POS",
            card_type="VISA",
            txn_event_ts=now,
            correlation_id="corr_test",
            mcc="5411",
            geo_lat=40.7,
            geo_lon=-74.0,
            settlement_id="settle_test",
            settlement_status=settlement_status,
            settle_event_ts=now,
            bank_reference="BNKREF",
            interchange_fee=1.80,
            net_settlement_amount=round(settlement_amount - 1.80, 2),
        )
    ]
    return spark.createDataFrame(rows)


# ─── anomaly severity tests ────────────────────────────────────────────────────

@pytest.mark.unit
class TestAnomalySeverityClassification:
    def test_no_anomaly_exact_match(self, spark: SparkSession) -> None:
        df = _make_joined_row(spark, 100.0, 100.0, "APPROVED")
        result = _add_reconciliation_columns(df).collect()[0]
        assert result["anomaly_severity"] == "NONE"
        assert result["is_anomaly"] is False

    def test_no_anomaly_within_tolerance(self, spark: SparkSession) -> None:
        # 0.005% mismatch — under 0.01% tolerance
        df = _make_joined_row(spark, 100.00, 99.995, "APPROVED")
        result = _add_reconciliation_columns(df).collect()[0]
        assert result["anomaly_severity"] == "NONE"

    def test_medium_anomaly_just_above_tolerance(self, spark: SparkSession) -> None:
        # 0.02% mismatch — above 0.01% tolerance, below 1%
        df = _make_joined_row(spark, 100.00, 99.98, "APPROVED")
        result = _add_reconciliation_columns(df).collect()[0]
        assert result["anomaly_severity"] == AnomalySeverity.MEDIUM.value
        assert result["is_anomaly"] is True

    def test_high_anomaly_3pct_mismatch(self, spark: SparkSession) -> None:
        df = _make_joined_row(spark, 100.00, 97.00, "APPROVED")
        result = _add_reconciliation_columns(df).collect()[0]
        assert result["anomaly_severity"] == AnomalySeverity.HIGH.value

    def test_critical_anomaly_6pct_mismatch(self, spark: SparkSession) -> None:
        df = _make_joined_row(spark, 100.00, 94.00, "APPROVED")
        result = _add_reconciliation_columns(df).collect()[0]
        assert result["anomaly_severity"] == AnomalySeverity.CRITICAL.value

    def test_critical_anomaly_reversed_status(self, spark: SparkSession) -> None:
        df = _make_joined_row(spark, 100.00, 100.00, "REVERSED")
        result = _add_reconciliation_columns(df).collect()[0]
        assert result["anomaly_severity"] == AnomalySeverity.CRITICAL.value

    def test_high_anomaly_disputed_status(self, spark: SparkSession) -> None:
        df = _make_joined_row(spark, 100.00, 100.00, "DISPUTED")
        result = _add_reconciliation_columns(df).collect()[0]
        assert result["anomaly_severity"] == AnomalySeverity.HIGH.value

    def test_critical_anomaly_rejected_status(self, spark: SparkSession) -> None:
        df = _make_joined_row(spark, 500.00, 500.00, "REJECTED")
        result = _add_reconciliation_columns(df).collect()[0]
        assert result["anomaly_severity"] == AnomalySeverity.CRITICAL.value

    def test_amount_delta_computed_correctly(self, spark: SparkSession) -> None:
        df = _make_joined_row(spark, 200.00, 194.00, "APPROVED")
        result = _add_reconciliation_columns(df).collect()[0]
        assert abs(result["amount_delta"] - 6.00) < 0.001
        assert abs(result["mismatch_pct"] - 3.0) < 0.01

    def test_mismatch_pct_zero_for_zero_amount(self, spark: SparkSession) -> None:
        """Guard against division-by-zero when transaction_amount is 0."""
        df = _make_joined_row(spark, 0.0, 0.0, "APPROVED")
        result = _add_reconciliation_columns(df).collect()[0]
        assert result["mismatch_pct"] == 0.0

    def test_large_transaction_boundary(self, spark: SparkSession) -> None:
        """$50,000 transaction with 0.008% mismatch — should stay NONE."""
        df = _make_joined_row(spark, 50_000.00, 49_996.00, "APPROVED")
        result = _add_reconciliation_columns(df).collect()[0]
        # 4 / 50000 = 0.008% — below 0.01% tolerance
        assert result["anomaly_severity"] == "NONE"

    def test_batch_multiple_severities(self, spark: SparkSession) -> None:
        """All four severities appear in a single batch — verify counts."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        rows = [
            Row(
                transaction_id="txn_none",    merchant_id="m1",
                card_id="c1", customer_id="cu1",
                transaction_amount=100.0, settlement_amount=100.0,
                transaction_currency="USD", country="US", channel="POS",
                card_type="VISA", txn_event_ts=now, correlation_id="corr1",
                mcc="5411", geo_lat=None, geo_lon=None,
                settlement_id="s1", settlement_status="APPROVED",
                settle_event_ts=now, bank_reference=None,
                interchange_fee=1.8, net_settlement_amount=98.2,
            ),
            Row(
                transaction_id="txn_medium",  merchant_id="m2",
                card_id="c2", customer_id="cu2",
                transaction_amount=100.0, settlement_amount=99.98,
                transaction_currency="USD", country="US", channel="POS",
                card_type="VISA", txn_event_ts=now, correlation_id="corr2",
                mcc="5411", geo_lat=None, geo_lon=None,
                settlement_id="s2", settlement_status="APPROVED",
                settle_event_ts=now, bank_reference=None,
                interchange_fee=1.8, net_settlement_amount=98.18,
            ),
            Row(
                transaction_id="txn_high",    merchant_id="m3",
                card_id="c3", customer_id="cu3",
                transaction_amount=100.0, settlement_amount=97.0,
                transaction_currency="USD", country="US", channel="POS",
                card_type="VISA", txn_event_ts=now, correlation_id="corr3",
                mcc="5411", geo_lat=None, geo_lon=None,
                settlement_id="s3", settlement_status="APPROVED",
                settle_event_ts=now, bank_reference=None,
                interchange_fee=1.75, net_settlement_amount=95.25,
            ),
            Row(
                transaction_id="txn_critical", merchant_id="m4",
                card_id="c4", customer_id="cu4",
                transaction_amount=100.0, settlement_amount=100.0,
                transaction_currency="USD", country="US", channel="POS",
                card_type="VISA", txn_event_ts=now, correlation_id="corr4",
                mcc="5411", geo_lat=None, geo_lon=None,
                settlement_id="s4", settlement_status="REVERSED",
                settle_event_ts=now, bank_reference=None,
                interchange_fee=1.8, net_settlement_amount=98.2,
            ),
        ]
        df = spark.createDataFrame(rows)
        result = _add_reconciliation_columns(df)

        severity_counts = {
            row["anomaly_severity"]: row["count"]
            for row in result.groupBy("anomaly_severity").count().collect()
        }
        assert severity_counts.get("NONE", 0) == 1
        assert severity_counts.get("MEDIUM", 0) == 1
        assert severity_counts.get("HIGH", 0) == 1
        assert severity_counts.get("CRITICAL", 0) == 1


# ─── Bronze validation tests ───────────────────────────────────────────────────

@pytest.mark.unit
class TestBronzeValidation:
    def _make_raw_txn(self, spark: SparkSession, **overrides) -> DataFrame:
        from datetime import datetime, timezone

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        base = dict(
            transaction_id="txn_valid",
            merchant_id="merch_0001",
            card_id="card_000001",
            customer_id="cust_00001",
            amount=100.0,
            currency="USD",
            country="US",
            channel="POS",
            card_type="VISA",
            event_time=now_ms,
            processing_time=now_ms,
            correlation_id="corr_valid",
            geo_lat=40.7,
            geo_lon=-74.0,
            mcc="5411",
            schema_version=1,
            raw_json='{"transaction_id":"txn_valid"}',
            topic="raw-transactions",
            partition=0,
            offset=0,
            kafka_timestamp=datetime.now(timezone.utc),
        )
        base.update(overrides)
        return spark.createDataFrame([Row(**base)])

    def test_valid_record_passes_all_checks(self, spark: SparkSession) -> None:
        df = self._make_raw_txn(spark)
        result = _add_validation_flags(df).collect()[0]
        assert result["_qc_pass"] is True

    def test_null_transaction_id_fails(self, spark: SparkSession) -> None:
        df = self._make_raw_txn(spark, transaction_id=None)
        result = _add_validation_flags(df).collect()[0]
        assert result["_qc_not_null"] is False
        assert result["_qc_pass"] is False

    def test_negative_amount_fails(self, spark: SparkSession) -> None:
        df = self._make_raw_txn(spark, amount=-10.0)
        result = _add_validation_flags(df).collect()[0]
        assert result["_qc_amount_range"] is False
        assert result["_qc_pass"] is False

    def test_zero_amount_fails(self, spark: SparkSession) -> None:
        df = self._make_raw_txn(spark, amount=0.0)
        result = _add_validation_flags(df).collect()[0]
        assert result["_qc_amount_range"] is False

    def test_invalid_currency_fails(self, spark: SparkSession) -> None:
        df = self._make_raw_txn(spark, currency="FAKE")
        result = _add_validation_flags(df).collect()[0]
        assert result["_qc_currency_valid"] is False

    def test_null_currency_passes(self, spark: SparkSession) -> None:
        """Null currency is allowed (some legacy systems omit it)."""
        df = self._make_raw_txn(spark, currency=None)
        result = _add_validation_flags(df).collect()[0]
        assert result["_qc_currency_valid"] is True

    def test_stale_event_time_fails(self, spark: SparkSession) -> None:
        from datetime import timedelta, timezone

        stale_ms = int(
            (datetime.now(timezone.utc) - timedelta(hours=30)).timestamp() * 1000
        )
        df = self._make_raw_txn(spark, event_time=stale_ms)
        result = _add_validation_flags(df).collect()[0]
        assert result["_qc_event_time_range"] is False

    def test_invalid_channel_fails(self, spark: SparkSession) -> None:
        df = self._make_raw_txn(spark, channel="CARRIER_PIGEON")
        result = _add_validation_flags(df).collect()[0]
        assert result["_qc_channel_valid"] is False

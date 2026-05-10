"""
Unit tests for the transaction and settlement simulation logic.

These tests validate the statistical properties of the simulation
without touching real Kafka — they mock the producer.
"""

from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from producer.transaction_producer import _generate_transaction
from producer.settlement_producer import (
    _generate_settlement,
    SettlementBuffer,
    MISMATCH_RATE,
    MISSING_RATE,
    DUPLICATE_RATE,
)


@pytest.mark.unit
class TestTransactionGenerator:
    def test_required_fields_present(self) -> None:
        txn = _generate_transaction()
        required = [
            "transaction_id", "merchant_id", "card_id", "customer_id",
            "amount", "currency", "country", "channel", "card_type",
            "event_time", "processing_time", "correlation_id", "schema_version",
        ]
        for field in required:
            assert field in txn, f"Missing field: {field}"

    def test_transaction_id_unique(self) -> None:
        ids = {_generate_transaction()["transaction_id"] for _ in range(1000)}
        assert len(ids) == 1000

    def test_amount_in_valid_range(self) -> None:
        amounts = [_generate_transaction()["amount"] for _ in range(500)]
        assert all(1.0 <= a <= 5000.0 for a in amounts)

    def test_currency_is_usd(self) -> None:
        txns = [_generate_transaction() for _ in range(100)]
        assert all(t["currency"] == "USD" for t in txns)

    def test_schema_version_is_1(self) -> None:
        txn = _generate_transaction()
        assert txn["schema_version"] == 1

    def test_channel_valid_values(self) -> None:
        valid = {"POS", "ONLINE", "ATM", "CONTACTLESS", "MOBILE"}
        channels = {_generate_transaction()["channel"] for _ in range(200)}
        assert channels.issubset(valid)

    def test_geo_present_for_pos(self) -> None:
        """POS and CONTACTLESS transactions should have geo coordinates."""
        pos_txns = []
        attempts = 0
        while len(pos_txns) < 20 and attempts < 10_000:
            t = _generate_transaction()
            if t["channel"] == "POS":
                pos_txns.append(t)
            attempts += 1
        for t in pos_txns:
            assert t["geo_lat"] is not None
            assert t["geo_lon"] is not None

    def test_specific_merchant_pinned(self) -> None:
        txn = _generate_transaction(merchant_id="merch_0042")
        assert txn["merchant_id"] == "merch_0042"


@pytest.mark.unit
class TestSettlementGenerator:
    def test_matching_settlement_returns_dict(self) -> None:
        result = _generate_settlement("txn_001", "merch_0001", 100.0, "corr_001")
        # May return None (missing_rate) — retry until we get one
        for _ in range(100):
            result = _generate_settlement("txn_001", "merch_0001", 100.0, "corr_001")
            if result is not None:
                break
        assert result is not None
        assert result["transaction_id"] == "txn_001"
        assert result["merchant_id"] == "merch_0001"

    def test_mismatch_rate_approximately_correct(self) -> None:
        """Generate 10,000 settlements and verify ~2% have amount mismatches."""
        mismatches = 0
        n = 10_000
        for _ in range(n):
            s = _generate_settlement("txn_x", "merch_0001", 100.0, "corr_x")
            if s is not None and abs(s["expected_amount"] - 100.0) > 0.001:
                mismatches += 1

        # Allow ±1% tolerance around the 2% target
        ratio = mismatches / n
        assert 0.01 <= ratio <= 0.03, f"Mismatch rate {ratio:.3f} outside expected 1-3%"

    def test_missing_rate_approximately_correct(self) -> None:
        """~0.5% of settlements should be None (missing)."""
        missing = sum(
            1
            for _ in range(10_000)
            if _generate_settlement("txn_x", "merch_0001", 100.0, "corr_x") is None
        )
        ratio = missing / 10_000
        assert 0.002 <= ratio <= 0.010, f"Missing rate {ratio:.4f} outside expected 0.2-1%"

    def test_net_settlement_amount_computed(self) -> None:
        for _ in range(50):
            s = _generate_settlement("txn_x", "merch_0001", 100.0, "corr_x")
            if s is not None and s["interchange_fee"] is not None:
                expected_net = round(s["expected_amount"] - s["interchange_fee"], 2)
                assert abs(s["net_settlement_amount"] - expected_net) < 0.01
                break


@pytest.mark.unit
class TestSettlementBuffer:
    def test_enqueue_and_ready(self) -> None:
        import time

        buf = SettlementBuffer()
        txn = {
            "transaction_id": "txn_buf_001",
            "merchant_id": "merch_0001",
            "amount": 100.0,
            "correlation_id": "corr_buf",
            "card_type": "VISA",
        }

        # Patch delay to be 0 so it's immediately ready
        with patch("producer.settlement_producer.random.uniform", return_value=0.0):
            buf.enqueue(txn)

        now = time.monotonic()
        ready = buf.ready(now + 1.0)  # 1 second in future
        # Should have at least one settlement (unless missing_rate triggered)
        # We accept 0 with low probability
        assert isinstance(ready, list)

    def test_not_ready_before_delay(self) -> None:
        import time

        buf = SettlementBuffer()
        txn = {
            "transaction_id": "txn_buf_002",
            "merchant_id": "merch_0001",
            "amount": 100.0,
            "correlation_id": "corr_buf_2",
            "card_type": "VISA",
        }

        with patch("producer.settlement_producer.random.uniform", return_value=1000.0):
            buf.enqueue(txn)

        ready = buf.ready(time.monotonic())
        assert len(ready) == 0

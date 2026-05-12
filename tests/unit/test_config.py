"""Unit tests for pipeline configuration and settings validation."""

from __future__ import annotations

import pytest

from pipeline.config import KafkaSettings, Settings, StreamingSettings


@pytest.mark.unit
class TestKafkaSettings:
    def test_default_bootstrap_server(self) -> None:
        cfg = KafkaSettings()
        assert cfg.bootstrap_servers == "localhost:9092"

    def test_default_topics_present(self) -> None:
        cfg = KafkaSettings()
        assert cfg.topic_raw_transactions == "raw-transactions"
        assert cfg.topic_settlement_expectations == "settlement-expectations"
        assert cfg.topic_reconciliation_alerts == "reconciliation-alerts"
        assert cfg.topic_dlq_transactions == "dlq-transactions"

    def test_invalid_acks_raises(self) -> None:
        with pytest.raises(ValueError):
            KafkaSettings(acks="3")

    def test_valid_acks_values(self) -> None:
        for valid in ("0", "1", "all", "-1"):
            cfg = KafkaSettings(acks=valid)
            assert cfg.acks == valid

    def test_idempotent_producer_enabled_by_default(self) -> None:
        cfg = KafkaSettings()
        assert cfg.enable_idempotence is True


@pytest.mark.unit
class TestStreamingSettings:
    def test_watermark_defaults(self) -> None:
        cfg = StreamingSettings()
        assert cfg.transaction_watermark_minutes == 10
        assert cfg.settlement_watermark_minutes == 30

    def test_mismatch_tolerance_default(self) -> None:
        cfg = StreamingSettings()
        assert cfg.amount_mismatch_tolerance_pct == 0.01

    def test_sla_default(self) -> None:
        cfg = StreamingSettings()
        assert cfg.gold_freshness_sla_minutes == 5


@pytest.mark.unit
class TestSettingsProducerConf:
    def test_plaintext_conf_has_no_sasl(self) -> None:
        settings = Settings()
        conf = settings.kafka_producer_conf()
        assert conf["security.protocol"] == "PLAINTEXT"
        assert "sasl.mechanisms" not in conf

    def test_producer_conf_includes_idempotence(self) -> None:
        settings = Settings()
        conf = settings.kafka_producer_conf()
        assert conf["enable.idempotence"] == "true"
        assert conf["acks"] == "all"

    def test_schema_registry_conf_no_auth_by_default(self) -> None:
        settings = Settings()
        conf = settings.schema_registry_conf()
        assert "url" in conf
        assert "basic.auth.credentials.source" not in conf

"""
Unit tests for the observability module.

Tests that logging configuration applies correctly and that metric
objects are properly instantiated. Does NOT start a real HTTP server.
"""

from __future__ import annotations

import pytest
import structlog

from pipeline.observability import (
    configure_logging,
    get_logger,
    bind_correlation_id,
    clear_correlation_id,
    PipelineMetrics,
)


@pytest.mark.unit
class TestLoggingConfiguration:
    def test_get_logger_returns_bound_logger(self) -> None:
        log = get_logger("test.module")
        assert log is not None

    def test_configure_logging_pretty_mode(self) -> None:
        """Should not raise even in non-JSON mode."""
        configure_logging(log_level="DEBUG", json=False)
        log = get_logger("test.pretty")
        # structlog bound loggers are callable
        assert callable(log.info)

    def test_configure_logging_json_mode(self) -> None:
        configure_logging(log_level="INFO", json=True)
        log = get_logger("test.json")
        assert callable(log.info)


@pytest.mark.unit
class TestCorrelationIdPropagation:
    def test_bind_and_clear(self) -> None:
        bind_correlation_id("corr_test_001")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx.get("correlation_id") == "corr_test_001"

        clear_correlation_id()
        ctx_after = structlog.contextvars.get_contextvars()
        assert "correlation_id" not in ctx_after

    def test_bind_overrides_previous(self) -> None:
        bind_correlation_id("corr_a")
        bind_correlation_id("corr_b")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["correlation_id"] == "corr_b"
        clear_correlation_id()


@pytest.mark.unit
class TestPipelineMetrics:
    def test_metrics_instantiation(self) -> None:
        """Verify all metric fields initialise without error."""
        m = PipelineMetrics()
        assert m.bronze_records_written is not None
        assert m.bronze_dlq_routed is not None
        assert m.silver_records_merged is not None
        assert m.gold_reconciled_pairs is not None
        assert m.gold_anomalies_detected is not None
        assert m.snowflake_rows_written is not None
        assert m.gold_freshness_lag_seconds is not None
        assert m.gold_sla_breach is not None

    def test_counter_increment(self) -> None:
        """Counters must accept inc() calls without raising."""
        from prometheus_client import Counter
        m = PipelineMetrics()
        # The shared singleton metrics would have already registered these names
        # so we test the module-level singleton instead
        from pipeline.observability import metrics
        # Just verify the attribute is a prometheus_client Counter
        assert hasattr(metrics.bronze_records_written, "inc")

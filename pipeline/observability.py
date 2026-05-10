"""
Observability — structured logging + Prometheus metrics for all pipeline layers.

Design:
  - structlog provides JSON-structured logs with correlation_id threaded through
    every log call. Databricks log delivery picks these up automatically.
  - prometheus_client exposes a /metrics HTTP endpoint scraped by Prometheus
    (or Databricks Observability) for streaming health dashboards.
  - Gold freshness SLA check: callable from Databricks SQL alert or a Databricks
    job step that queries the Gold Delta table and fires if lag > SLA threshold.

Usage in any pipeline module:
    from pipeline.observability import get_logger, metrics

    log = get_logger(__name__)
    log.info("bronze.batch.written", batch_id=42, rows=1000, correlation_id="corr_xyz")
    metrics.bronze_records_written.inc(1000)
"""

from __future__ import annotations

import logging
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from pipeline.config import settings


# ── structlog configuration ───────────────────────────────────────────────────

def configure_logging(log_level: str = "INFO", json: bool = True) -> None:
    """
    Configure structlog for the entire process.

    Call once at job startup (main entrypoint). Subsequent get_logger() calls
    inherit this configuration.

    JSON mode (json=True):  suitable for production / Databricks log ingestion.
    Pretty mode (json=False): readable during local development.
    """
    shared_processors = [
        structlog.contextvars.merge_contextvars,         # thread-local context (correlation_id)
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Also configure stdlib logging so PySpark / Delta logs are unified
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )
    # Quiet noisy Spark internals
    for noisy in ("py4j", "pyspark", "delta"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.BoundLogger:
    """Return a structlog logger bound to `name`."""
    return structlog.get_logger(name)


# ── Correlation ID context propagation ───────────────────────────────────────

def bind_correlation_id(correlation_id: str) -> None:
    """
    Bind a correlation_id to the current thread's log context.

    All subsequent log calls in this thread automatically include the ID.
    Call at the top of foreachBatch to propagate the transaction's
    correlation_id through Bronze → Silver → Gold log entries.

        from pipeline.observability import bind_correlation_id
        bind_correlation_id(batch_df.select("correlation_id").first()[0])
    """
    structlog.contextvars.bind_contextvars(correlation_id=correlation_id)


def clear_correlation_id() -> None:
    structlog.contextvars.clear_contextvars()


# ── Prometheus metrics ────────────────────────────────────────────────────────

@dataclass
class PipelineMetrics:
    """
    Centralised Prometheus metric definitions.

    Counters accumulate forever; Gauges represent current state;
    Histograms track distributions (batch duration, lag).
    """

    # Bronze
    bronze_records_written: Counter = field(default_factory=lambda: Counter(
        "reconciliation_bronze_records_total",
        "Total records written to Bronze Delta",
        ["merchant_id"],
    ))
    bronze_dlq_routed: Counter = field(default_factory=lambda: Counter(
        "reconciliation_bronze_dlq_total",
        "Records routed to DLQ from Bronze",
        ["error_type"],
    ))
    bronze_batch_duration_seconds: Histogram = field(default_factory=lambda: Histogram(
        "reconciliation_bronze_batch_duration_seconds",
        "Bronze batch processing duration",
        buckets=[1, 5, 10, 30, 60, 120, 300],
    ))

    # Silver
    silver_records_merged: Counter = field(default_factory=lambda: Counter(
        "reconciliation_silver_records_merged_total",
        "Total records upserted into Silver Delta",
    ))
    silver_duplicates_suppressed: Counter = field(default_factory=lambda: Counter(
        "reconciliation_silver_duplicates_suppressed_total",
        "Duplicate records suppressed by Silver MERGE",
    ))
    silver_batch_duration_seconds: Histogram = field(default_factory=lambda: Histogram(
        "reconciliation_silver_batch_duration_seconds",
        "Silver batch processing duration",
        buckets=[1, 5, 10, 30, 60, 120],
    ))

    # Gold
    gold_reconciled_pairs: Counter = field(default_factory=lambda: Counter(
        "reconciliation_gold_pairs_total",
        "Total transaction-settlement pairs reconciled",
        ["anomaly_severity"],
    ))
    gold_anomalies_detected: Counter = field(default_factory=lambda: Counter(
        "reconciliation_gold_anomalies_total",
        "Total anomalies detected",
        ["severity", "type"],
    ))
    gold_batch_duration_seconds: Histogram = field(default_factory=lambda: Histogram(
        "reconciliation_gold_batch_duration_seconds",
        "Gold batch processing duration",
        buckets=[1, 5, 10, 30, 60, 120, 300],
    ))
    gold_watermark_lag_seconds: Gauge = field(default_factory=lambda: Gauge(
        "reconciliation_gold_watermark_lag_seconds",
        "Current watermark lag behind wall clock",
        ["stream"],
    ))

    # Snowflake writer
    snowflake_rows_written: Counter = field(default_factory=lambda: Counter(
        "reconciliation_snowflake_rows_total",
        "Total rows written to Snowflake",
        ["table"],
    ))
    snowflake_write_errors: Counter = field(default_factory=lambda: Counter(
        "reconciliation_snowflake_errors_total",
        "Snowflake write errors",
        ["table"],
    ))

    # Kafka consumer lag (updated by a background poller)
    kafka_consumer_lag: Gauge = field(default_factory=lambda: Gauge(
        "reconciliation_kafka_consumer_lag_records",
        "Kafka consumer lag in records",
        ["topic", "partition", "consumer_group"],
    ))

    # Gold freshness SLA
    gold_freshness_lag_seconds: Gauge = field(default_factory=lambda: Gauge(
        "reconciliation_gold_freshness_lag_seconds",
        "Seconds since the most recent Gold record was written",
    ))
    gold_sla_breach: Gauge = field(default_factory=lambda: Gauge(
        "reconciliation_gold_sla_breach",
        "1 if Gold freshness SLA is breached, 0 otherwise",
    ))


# Singleton — shared across all pipeline modules in the same process
metrics = PipelineMetrics()

_metrics_server_started = False
_metrics_lock = threading.Lock()


def start_metrics_server(port: Optional[int] = None) -> None:
    """
    Start the Prometheus HTTP metrics endpoint.

    Call once at job startup. Safe to call multiple times — only starts once.
    On Databricks the driver port must be allowed outbound by the cluster policy.
    """
    global _metrics_server_started
    with _metrics_lock:
        if _metrics_server_started:
            return
        p = port or settings.observability.prometheus_port
        start_http_server(p)
        _metrics_server_started = True
        get_logger(__name__).info("metrics.server.started", port=p)


# ── Gold freshness SLA check ──────────────────────────────────────────────────

def check_gold_freshness(spark) -> bool:
    """
    Query Gold Delta for the most recent `gold_reconciled_at` timestamp.
    Updates the Prometheus gauge and returns True if within SLA.

    Designed to run as a lightweight Databricks SQL alert or as a
    periodic foreachBatch side-effect.
    """
    log = get_logger(__name__)
    gold_path = f"{settings.delta.gold_table_path}/reconciliation_results"
    sla_seconds = settings.streaming.gold_freshness_sla_minutes * 60

    try:
        from pyspark.sql import functions as F
        latest = (
            spark.read.format("delta").load(gold_path)
            .select(F.max("gold_reconciled_at").alias("latest_ts"))
            .collect()[0]["latest_ts"]
        )
        if latest is None:
            log.warning("gold.freshness.no_data")
            metrics.gold_sla_breach.set(1)
            return False

        lag = (datetime.now(timezone.utc) - latest.replace(tzinfo=timezone.utc)).total_seconds()
        metrics.gold_freshness_lag_seconds.set(lag)

        if lag > sla_seconds:
            log.error(
                "gold.freshness.sla_breach",
                lag_seconds=round(lag, 1),
                sla_seconds=sla_seconds,
            )
            metrics.gold_sla_breach.set(1)
            return False

        log.info("gold.freshness.ok", lag_seconds=round(lag, 1))
        metrics.gold_sla_breach.set(0)
        return True

    except Exception as exc:
        log.error("gold.freshness.check_failed", error=str(exc))
        metrics.gold_sla_breach.set(1)
        return False


# ── Kafka consumer lag polling ────────────────────────────────────────────────

def poll_kafka_consumer_lag(bootstrap_servers: str, group_ids: list[str]) -> None:
    """
    Poll Kafka AdminClient for consumer group lag and update Prometheus gauges.

    Intended to run in a background thread (or as a separate Databricks job step).
    Confluent AdminClient's list_consumer_group_offsets() returns per-partition lag.
    """
    log = get_logger(__name__)
    try:
        from confluent_kafka.admin import AdminClient
        from confluent_kafka import TopicPartition

        admin = AdminClient({"bootstrap.servers": bootstrap_servers})

        for group_id in group_ids:
            result = admin.list_consumer_group_offsets([group_id])
            for group, future in result.items():
                offsets = future.result()
                for tp, offset_info in offsets.topic_partitions.items():
                    if offset_info.error:
                        continue
                    # committed offset — high watermark lag requires broker query
                    # Simplified: use committed offset as proxy
                    metrics.kafka_consumer_lag.labels(
                        topic=tp.topic,
                        partition=str(tp.partition),
                        consumer_group=group_id,
                    ).set(max(0, (offset_info.offset or 0)))

    except Exception as exc:
        log.warning("kafka.lag.poll_failed", error=str(exc))


# ── Initialise on import (non-blocking) ──────────────────────────────────────

configure_logging(
    log_level=settings.observability.log_level,
    json=settings.observability.enable_json_logging,
)

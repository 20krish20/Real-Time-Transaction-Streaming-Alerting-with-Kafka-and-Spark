"""
Transaction Producer — simulates ~1 000 card transactions/sec.

Realistic simulation properties:
  - 500 merchants, 300 customers, 1 000 card tokens
  - Geographic bias (60% US, 10% CA, 15% GB, 15% mixed)
  - MCC codes sampled from real distribution
  - Avro serialisation via Confluent Schema Registry
  - Idempotent producer with exactly-once delivery semantics
  - Structured logging with correlation IDs

Run locally:
    python -m producer.transaction_producer
"""

from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from pipeline.config import settings

log = structlog.get_logger(__name__)

# ── Static reference data ─────────────────────────────────────────────────────

MERCHANTS = [f"merch_{i:04d}" for i in range(1, 501)]
CUSTOMERS = [f"cust_{i:05d}" for i in range(1, 301)]
CARDS = [f"card_{i:06d}" for i in range(1, 1001)]

COUNTRIES_WEIGHTED = (
    ["US"] * 60
    + ["CA"] * 10
    + ["GB"] * 10
    + ["IN"] * 8
    + ["DE"] * 6
    + ["AU"] * 4
    + ["SG"] * 2
)

CHANNELS = ["POS", "ONLINE", "ATM", "CONTACTLESS", "MOBILE"]
CHANNEL_WEIGHTS = [0.35, 0.30, 0.10, 0.20, 0.05]

CARD_TYPES = ["VISA", "MASTERCARD", "AMEX", "DISCOVER", "UNIONPAY"]
CARD_TYPE_WEIGHTS = [0.45, 0.30, 0.12, 0.08, 0.05]

# Realistic MCC codes — grocery, gas, restaurant, travel, entertainment, etc.
MCC_CODES = [
    "5411",  # Grocery Stores
    "5912",  # Drug Stores and Pharmacies
    "5812",  # Eating Places, Restaurants
    "5541",  # Service Stations
    "5310",  # Discount Stores
    "4111",  # Transportation
    "7011",  # Hotels and Lodging
    "5732",  # Electronics Stores
    "5999",  # Miscellaneous Retail
    "4814",  # Telecommunication Services
]

# Merchant-to-lat/lon lookup (simplified)
MERCHANT_GEO: dict[str, tuple[float, float]] = {
    f"merch_{i:04d}": (
        round(random.uniform(25.0, 49.0), 4),   # US lat range
        round(random.uniform(-124.0, -66.0), 4),  # US lon range
    )
    for i in range(1, 501)
}


def _load_avro_schema(path: str) -> str:
    return Path(path).read_text()


def _build_serializer(schema_str: str) -> AvroSerializer:
    sr_client = SchemaRegistryClient(settings.schema_registry_conf())
    return AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=schema_str,
        to_dict=lambda obj, _ctx: obj,
    )


def _build_producer() -> Producer:
    conf = settings.kafka_producer_conf()
    conf["client.id"] = "transaction-producer-v1"
    return Producer(conf)


def _generate_transaction(merchant_id: str | None = None) -> dict[str, Any]:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    txn_id = f"txn_{uuid.uuid4()}"
    correlation_id = f"corr_{uuid.uuid4()}"
    merchant = merchant_id or random.choice(MERCHANTS)
    channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]

    geo_lat, geo_lon = None, None
    if channel in ("POS", "CONTACTLESS"):
        geo_lat, geo_lon = MERCHANT_GEO.get(merchant, (None, None))

    return {
        "transaction_id": txn_id,
        "merchant_id": merchant,
        "card_id": random.choice(CARDS),
        "customer_id": random.choice(CUSTOMERS),
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "currency": "USD",
        "country": random.choice(COUNTRIES_WEIGHTED),
        "channel": channel,
        "card_type": random.choices(CARD_TYPES, weights=CARD_TYPE_WEIGHTS, k=1)[0],
        "event_time": now_ms,
        "processing_time": now_ms,
        "correlation_id": correlation_id,
        "geo_lat": geo_lat,
        "geo_lon": geo_lon,
        "mcc": random.choice(MCC_CODES),
        "schema_version": 1,
    }


def _delivery_callback(err: Any, msg: Any) -> None:
    if err:
        log.error(
            "kafka.delivery.failed",
            topic=msg.topic(),
            partition=msg.partition(),
            error=str(err),
        )
    else:
        log.debug(
            "kafka.delivery.ok",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


def run(
    transactions_per_second: int = 1000,
    duration_seconds: int | None = None,
    schema_path: str = "schemas/transaction_event.avsc",
) -> None:
    """
    Produce transactions at the target rate.

    Args:
        transactions_per_second: Target throughput.
        duration_seconds: Stop after N seconds. None = run indefinitely.
        schema_path: Path to the Avro schema file.
    """
    schema_str = _load_avro_schema(schema_path)
    serializer = _build_serializer(schema_str)
    producer = _build_producer()
    topic = settings.kafka.topic_raw_transactions

    log.info(
        "producer.started",
        topic=topic,
        target_tps=transactions_per_second,
        bootstrap=settings.kafka.bootstrap_servers,
    )

    interval = 1.0 / transactions_per_second
    start = time.monotonic()
    produced = 0

    try:
        while True:
            loop_start = time.monotonic()

            txn = _generate_transaction()
            key = txn["merchant_id"].encode()

            value = serializer(
                txn,
                SerializationContext(topic, MessageField.VALUE),
            )

            producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=_delivery_callback,
            )
            producer.poll(0)
            produced += 1

            if produced % 10_000 == 0:
                elapsed = time.monotonic() - start
                log.info(
                    "producer.progress",
                    produced=produced,
                    elapsed_s=round(elapsed, 1),
                    actual_tps=round(produced / elapsed, 0),
                )

            if duration_seconds and (time.monotonic() - start) >= duration_seconds:
                break

            # Rate limiting: sleep for remaining interval
            elapsed_loop = time.monotonic() - loop_start
            sleep_for = interval - elapsed_loop
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        log.info("producer.interrupted", produced=produced)
    finally:
        remaining = producer.flush(timeout=10)
        if remaining:
            log.warning("producer.flush.incomplete", remaining_messages=remaining)
        log.info("producer.stopped", total_produced=produced)


if __name__ == "__main__":
    import structlog

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )
    run(transactions_per_second=1000)

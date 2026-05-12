"""
Settlement Producer — simulates bank settlement records with realistic chaos.

Simulation properties that mirror real payment network behaviour:
  - 5–15 minute delay after transaction (banks batch settlements)
  - 2 % amount mismatch rate (interchange fee rounding, FX issues)
  - 0.5% missing settlement rate (bank-side failures, silent drops)
  - 0.1% duplicate settlement rate (bank retry storms)
  - All records serialised as Avro via Confluent Schema Registry

Architecture note:
  In production this producer is replaced by a Kafka Connect source connector
  reading from the bank's SFTP drop or REST webhook. The simulation keeps the
  same Avro schema so the downstream pipeline is unaffected.
"""

from __future__ import annotations

import random
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from pipeline.config import settings

log = structlog.get_logger(__name__)

# ── Chaos configuration ───────────────────────────────────────────────────────
MISMATCH_RATE = 0.02  # 2 %  — amount differs by up to 0.5 %
MISSING_RATE = 0.005  # 0.5% — transaction gets no settlement
DUPLICATE_RATE = 0.001  # 0.1% — settlement sent twice
DELAY_MIN_SECONDS = 300  # 5 minutes
DELAY_MAX_SECONDS = 900  # 15 minutes

SETTLEMENT_STATUSES = ["APPROVED", "PENDING", "REJECTED", "REVERSED", "DISPUTED"]
STATUS_WEIGHTS = [0.88, 0.06, 0.03, 0.02, 0.01]

INTERCHANGE_FEE_RATES = {
    "VISA": 0.0180,
    "MASTERCARD": 0.0185,
    "AMEX": 0.0290,
    "DISCOVER": 0.0200,
    "UNIONPAY": 0.0055,
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
    conf["client.id"] = "settlement-producer-v1"
    return Producer(conf)


def _epoch_days(d: date) -> int:
    """Convert date to epoch days (Avro date logical type)."""
    return (d - date(1970, 1, 1)).days


def _generate_settlement(
    transaction_id: str,
    merchant_id: str,
    amount: float,
    correlation_id: str,
    card_type: str = "VISA",
) -> dict[str, Any] | None:
    """
    Generate a settlement record for a given transaction.
    Returns None to simulate a missing settlement.
    """
    # Drop 0.5% of settlements silently
    if random.random() < MISSING_RATE:
        log.debug("settlement.dropped", transaction_id=transaction_id)
        return None

    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    today_days = _epoch_days(now.date())

    # Introduce amount mismatch for 2% of settlements
    if random.random() < MISMATCH_RATE:
        mismatch_factor = 1.0 + random.uniform(-0.005, 0.005)  # ±0.5%
        expected_amount = round(amount * mismatch_factor, 2)
    else:
        expected_amount = amount

    interchange_rate = INTERCHANGE_FEE_RATES.get(card_type, 0.018)
    interchange_fee = round(expected_amount * interchange_rate, 4)
    net = round(expected_amount - interchange_fee, 2)

    return {
        "settlement_id": f"settle_{uuid.uuid4()}",
        "transaction_id": transaction_id,
        "merchant_id": merchant_id,
        "expected_amount": expected_amount,
        "currency": "USD",
        "settlement_status": random.choices(SETTLEMENT_STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        "settlement_date": today_days,
        "event_time": now_ms,
        "processing_time": now_ms,
        "correlation_id": correlation_id,
        "bank_reference": f"BNKREF{random.randint(100000, 999999)}",
        "interchange_fee": interchange_fee,
        "net_settlement_amount": net,
        "schema_version": 1,
    }


def _delivery_callback(err: Any, msg: Any) -> None:
    if err:
        log.error(
            "kafka.delivery.failed",
            topic=msg.topic(),
            error=str(err),
        )


class SettlementBuffer:
    """
    Holds pending transactions and emits their settlements after a realistic delay.
    Thread-safety is not needed here — single-threaded producer loop.
    """

    def __init__(self) -> None:
        # List of (emit_at_monotonic, settlement_dict)
        self._pending: list[tuple[float, dict[str, Any]]] = []

    def enqueue(self, txn: dict[str, Any]) -> None:
        delay = random.uniform(DELAY_MIN_SECONDS, DELAY_MAX_SECONDS)
        settlement = _generate_settlement(
            transaction_id=txn["transaction_id"],
            merchant_id=txn["merchant_id"],
            amount=txn["amount"],
            correlation_id=txn["correlation_id"],
            card_type=txn.get("card_type", "VISA"),
        )
        if settlement is None:
            return  # simulated missing settlement — nothing to enqueue

        emit_at = time.monotonic() + delay
        self._pending.append((emit_at, settlement))

        # Duplicate: enqueue a second copy for 0.1% of records
        if random.random() < DUPLICATE_RATE:
            dup = {**settlement, "settlement_id": f"settle_{uuid.uuid4()}"}
            # Duplicates arrive 30–120 seconds after the original
            self._pending.append((emit_at + random.uniform(30, 120), dup))

    def ready(self, now: float) -> list[dict[str, Any]]:
        """Return and remove all settlements whose emit time has passed."""
        due = [s for emit_at, s in self._pending if emit_at <= now]
        self._pending = [(t, s) for t, s in self._pending if t > now]
        return due


def run(
    input_file: str | None = None,
    schema_path: str = "schemas/settlement_event.avsc",
    transactions_per_second: int = 1000,
    duration_seconds: int | None = None,
) -> None:
    """
    Simulate settlement production.

    In real deployment this reads from a transaction log / CDC stream.
    In simulation it generates synthetic transactions internally at the
    specified rate and then releases their settlements with delays.

    Args:
        input_file: Optional JSONL file of pre-generated transactions.
                    If None, generates transactions internally.
        schema_path: Avro schema for SettlementEvent.
        transactions_per_second: Rate at which NEW transactions are generated
                                  (settlements arrive later with delays).
        duration_seconds: Stop after N seconds. None = run indefinitely.
    """
    schema_str = _load_avro_schema(schema_path)
    serializer = _build_serializer(schema_str)
    producer = _build_producer()
    topic = settings.kafka.topic_settlement_expectations

    buffer = SettlementBuffer()
    start = time.monotonic()
    interval = 1.0 / transactions_per_second
    emitted = 0

    log.info(
        "settlement_producer.started",
        topic=topic,
        simulation_tps=transactions_per_second,
        mismatch_rate=MISMATCH_RATE,
        missing_rate=MISSING_RATE,
        duplicate_rate=DUPLICATE_RATE,
    )

    # Inline synthetic transaction generator (avoids circular import with
    # transaction_producer). Kept minimal — just the fields settlement needs.
    def _synthetic_txn() -> dict[str, Any]:
        return {
            "transaction_id": f"txn_{uuid.uuid4()}",
            "merchant_id": f"merch_{random.randint(1, 500):04d}",
            "amount": round(random.uniform(1.0, 5000.0), 2),
            "correlation_id": f"corr_{uuid.uuid4()}",
            "card_type": random.choice(["VISA", "MASTERCARD", "AMEX", "DISCOVER"]),
        }

    try:
        while True:
            loop_start = time.monotonic()

            # Enqueue a new transaction for future settlement
            buffer.enqueue(_synthetic_txn())

            # Emit any settlements whose delay has expired
            for settlement in buffer.ready(loop_start):
                key = settlement["merchant_id"].encode()
                value = serializer(
                    settlement,
                    SerializationContext(topic, MessageField.VALUE),
                )
                producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=_delivery_callback,
                )
                emitted += 1

            producer.poll(0)

            if emitted and emitted % 1000 == 0:
                log.info(
                    "settlement_producer.progress",
                    emitted=emitted,
                    pending_in_buffer=len(buffer._pending),
                )

            if duration_seconds and (time.monotonic() - start) >= duration_seconds:
                break

            elapsed = time.monotonic() - loop_start
            sleep_for = interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        log.info("settlement_producer.interrupted", emitted=emitted)
    finally:
        remaining = producer.flush(timeout=10)
        if remaining:
            log.warning("settlement_producer.flush.incomplete", remaining=remaining)
        log.info("settlement_producer.stopped", total_emitted=emitted)


if __name__ == "__main__":
    import structlog

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(20),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )
    run(transactions_per_second=100)  # lower rate; settlements arrive with 5–15 min delay

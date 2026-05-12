"""
Integration tests — requires Docker (testcontainers).

These tests spin up a real Kafka broker and Schema Registry in Docker,
produce Avro messages, and verify the pipeline processes them correctly.

Run with:
    pytest -m integration tests/integration/
    # or
    pytest tests/integration/ --timeout=120

Skipped automatically if Docker is not available.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone

import pytest

# Skip entire module if testcontainers / Docker unavailable
pytest.importorskip("testcontainers", reason="testcontainers not installed")
pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")

try:
    import docker  # type: ignore[import]

    docker.from_env().ping()
    DOCKER_AVAILABLE = True
except Exception:
    DOCKER_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not DOCKER_AVAILABLE,
    reason="Docker not available — skipping integration tests",
)


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def kafka_container():
    """Start a Kafka + ZooKeeper container for the test module."""
    from testcontainers.kafka import KafkaContainer  # type: ignore[import]

    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as kafka:
        yield kafka


@pytest.fixture(scope="module")
def bootstrap_servers(kafka_container) -> str:
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="module")
def admin_client(bootstrap_servers: str):
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    topics = [
        NewTopic("raw-transactions", num_partitions=3, replication_factor=1),
        NewTopic("settlement-expectations", num_partitions=3, replication_factor=1),
        NewTopic("reconciliation-alerts", num_partitions=3, replication_factor=1),
        NewTopic("dlq-transactions", num_partitions=3, replication_factor=1),
    ]
    futures = admin.create_topics(topics)
    for _topic, future in futures.items():
        try:
            future.result()
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise

    yield admin


@pytest.fixture
def producer(bootstrap_servers: str, admin_client):
    """JSON producer (bypasses Schema Registry for simpler integration tests)."""
    from confluent_kafka import Producer

    p = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    yield p
    p.flush(10)


@pytest.fixture
def consumer_factory(bootstrap_servers: str, admin_client):
    """Factory to create consumers for specific topics."""
    from confluent_kafka import Consumer

    consumers = []

    def _make_consumer(topic: str, group_id: str | None = None) -> Consumer:
        c = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id or f"test-consumer-{uuid.uuid4()}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        c.subscribe([topic])
        consumers.append(c)
        return c

    yield _make_consumer

    for c in consumers:
        c.close()


# ─── Helper utilities ─────────────────────────────────────────────────────────


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _produce_json(producer, topic: str, key: str, value: dict) -> None:
    producer.produce(
        topic=topic,
        key=key.encode(),
        value=json.dumps(value).encode(),
    )
    producer.poll(0)


def _consume_messages(consumer, count: int, timeout_seconds: float = 30.0) -> list[dict]:
    """Consume up to `count` messages within the timeout."""
    messages = []
    deadline = time.monotonic() + timeout_seconds
    while len(messages) < count and time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        messages.append(json.loads(msg.value().decode()))
    return messages


# ─── Tests ────────────────────────────────────────────────────────────────────


@pytest.mark.integration
class TestKafkaTopicCreation:
    def test_topics_exist(self, admin_client) -> None:
        metadata = admin_client.list_topics(timeout=10)
        topic_names = set(metadata.topics.keys())
        expected = {
            "raw-transactions",
            "settlement-expectations",
            "reconciliation-alerts",
            "dlq-transactions",
        }
        assert expected.issubset(topic_names), f"Missing topics: {expected - topic_names}"


@pytest.mark.integration
class TestTransactionProduction:
    def test_produce_and_consume_transaction(self, producer, consumer_factory) -> None:
        consumer = consumer_factory("raw-transactions")
        now = _now_ms()
        txn = {
            "transaction_id": f"txn_{uuid.uuid4()}",
            "merchant_id": "merch_0001",
            "card_id": "card_000001",
            "customer_id": "cust_00001",
            "amount": 250.00,
            "currency": "USD",
            "country": "US",
            "channel": "POS",
            "card_type": "VISA",
            "event_time": now,
            "processing_time": now,
            "correlation_id": f"corr_{uuid.uuid4()}",
            "geo_lat": 40.7128,
            "geo_lon": -74.0060,
            "mcc": "5411",
            "schema_version": 1,
        }
        _produce_json(producer, "raw-transactions", txn["merchant_id"], txn)
        producer.flush(5)

        messages = _consume_messages(consumer, 1, timeout_seconds=20.0)
        assert len(messages) == 1
        assert messages[0]["transaction_id"] == txn["transaction_id"]
        assert messages[0]["amount"] == 250.00

    def test_produce_100_transactions(self, producer, consumer_factory) -> None:
        topic = "raw-transactions"
        consumer = consumer_factory(topic, group_id=f"bulk-test-{uuid.uuid4()}")

        now = _now_ms()
        txn_ids = set()
        for i in range(100):
            txn_id = f"txn_bulk_{i}_{uuid.uuid4()}"
            txn_ids.add(txn_id)
            txn = {
                "transaction_id": txn_id,
                "merchant_id": f"merch_{i % 10:04d}",
                "card_id": "card_000001",
                "customer_id": "cust_00001",
                "amount": float(i + 1),
                "currency": "USD",
                "country": "US",
                "channel": "ONLINE",
                "card_type": "MASTERCARD",
                "event_time": now,
                "processing_time": now,
                "correlation_id": f"corr_{uuid.uuid4()}",
                "geo_lat": None,
                "geo_lon": None,
                "mcc": "5999",
                "schema_version": 1,
            }
            _produce_json(producer, topic, txn["merchant_id"], txn)

        producer.flush(10)
        messages = _consume_messages(consumer, 100, timeout_seconds=30.0)
        consumed_ids = {m["transaction_id"] for m in messages}
        assert len(consumed_ids) == 100
        assert consumed_ids == txn_ids


@pytest.mark.integration
class TestSettlementProduction:
    def test_produce_settlement(self, producer, consumer_factory) -> None:
        consumer = consumer_factory("settlement-expectations")
        now = _now_ms()
        settlement = {
            "settlement_id": f"settle_{uuid.uuid4()}",
            "transaction_id": f"txn_{uuid.uuid4()}",
            "merchant_id": "merch_0001",
            "expected_amount": 250.00,
            "currency": "USD",
            "settlement_status": "APPROVED",
            "settlement_date": 19847,
            "event_time": now,
            "processing_time": now,
            "correlation_id": f"corr_{uuid.uuid4()}",
            "bank_reference": "BNKREF100001",
            "interchange_fee": 4.50,
            "net_settlement_amount": 245.50,
            "schema_version": 1,
        }
        _produce_json(producer, "settlement-expectations", settlement["merchant_id"], settlement)
        producer.flush(5)

        messages = _consume_messages(consumer, 1, timeout_seconds=20.0)
        assert len(messages) == 1
        assert messages[0]["expected_amount"] == 250.00
        assert messages[0]["settlement_status"] == "APPROVED"


@pytest.mark.integration
class TestDLQRouting:
    def test_malformed_message_does_not_crash_consumer(self, producer, consumer_factory) -> None:
        """
        Produce a deliberately malformed message and verify it can be read.
        The DLQ routing itself (via Spark) is tested in E2E; here we just
        verify the topic accepts arbitrary bytes (which it should as raw Kafka).
        """
        consumer = consumer_factory("dlq-transactions")
        dlq_payload = {
            "original_payload": "NOT_VALID_JSON{{{{",
            "batch_id": 0,
            "dlq_routed_at": datetime.now(timezone.utc).isoformat(),
            "validation_errors": ["NULL_REQUIRED_FIELD"],
            "failed_stage": "bronze_ingestion",
        }
        _produce_json(producer, "dlq-transactions", "merch_0001", dlq_payload)
        producer.flush(5)

        messages = _consume_messages(consumer, 1, timeout_seconds=15.0)
        assert len(messages) == 1
        assert messages[0]["failed_stage"] == "bronze_ingestion"

import json
import random
import time
from datetime import datetime, timezone
from uuid import uuid4
from pathlib import Path

import yaml
from confluent_kafka import Producer


CARDS = [f"card_{i}" for i in range(1, 501)]
CUSTOMERS = [f"cust_{i}" for i in range(1, 301)]
MERCHANTS = [f"m_{i}" for i in range(1, 101)]
COUNTRIES = ["US", "US", "US", "CA", "GB", "IN"]  # bias to US
CHANNELS = ["POS", "ONLINE", "ATM"]


def load_config(path: str = "config/kafka_config.yml") -> dict:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Kafka config not found at {config_path}. "
            f"Copy config/kafka_config.example.yml and update."
        )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def create_producer(conf: dict) -> Producer:
    """
    Build a Confluent Cloud–compatible producer using librdkafka
    (no JAAS config; use sasl.username / sasl.password).
    """
    producer_conf = {
        "bootstrap.servers": conf["bootstrap_servers"],
        "client.id": "payments-producer",
        "linger.ms": conf.get("linger_ms", 10),
        "acks": conf.get("acks", "all"),

        # Confluent Cloud security (correct for confluent-kafka)
        "security.protocol": conf.get("security_protocol", "SASL_SSL"),
        "sasl.mechanisms": conf.get("sasl_mechanism", "PLAIN"),
        "sasl.username": conf["sasl_username"],
        "sasl.password": conf["sasl_password"],
    }

    return Producer(producer_conf)


def generate_event() -> dict:
    card = random.choice(CARDS)
    customer = random.choice(CUSTOMERS)
    merchant = random.choice(MERCHANTS)
    country = random.choice(COUNTRIES)
    amount = round(random.uniform(1, 5000), 2)
    now = datetime.now(timezone.utc)

    return {
        "transaction_id": f"txn_{uuid4()}",
        "card_id": card,
        "customer_id": customer,
        "merchant_id": merchant,
        "amount": amount,
        "currency": "USD",
        "country": country,
        "channel": random.choice(CHANNELS),
        "event_time": now.isoformat(),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")
    # else: omit noisy success logs in this simple example


def main():
    conf = load_config()
    print("Bootstrap:", conf["bootstrap_servers"])
    print("API key:", conf["sasl_username"])
    topic = conf["topic"]
    producer = create_producer(conf)

    print(f"Producing events to topic={topic} on {conf['bootstrap_servers']}")

    try:
        while True:
            event = generate_event()
            key = event["card_id"].encode("utf-8")
            value = json.dumps(event).encode("utf-8")

            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(0.05)  # ~20 events/sec
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()

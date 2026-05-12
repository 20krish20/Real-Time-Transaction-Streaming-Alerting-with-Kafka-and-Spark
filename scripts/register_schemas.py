"""
Schema Registry Registration Script

Registers the TransactionEvent and SettlementEvent Avro schemas with the
Confluent Schema Registry. Idempotent — safe to re-run; if the schema already
exists and is compatible, the existing schema ID is returned without error.

Compatibility level: BACKWARD (set at registry level in docker-compose.yml).
This means new schema versions can only ADD optional fields (with defaults)
or REMOVE fields — existing consumers continue to work without redeployment.

Usage:
    # Local (docker-compose stack running)
    python scripts/register_schemas.py

    # Confluent Cloud
    KAFKA__SCHEMA_REGISTRY_URL=https://psrc-xxx.us-east-2.aws.confluent.cloud \\
    KAFKA__SCHEMA_REGISTRY_USERNAME=<key> \\
    KAFKA__SCHEMA_REGISTRY_PASSWORD=<secret> \\
    python scripts/register_schemas.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import requests

from pipeline.config import settings

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"

# subject = topic name + "-value" (Confluent TopicNameStrategy)
SCHEMA_SUBJECTS = {
    "transaction_event.avsc": f"{settings.kafka.topic_raw_transactions}-value",
    "settlement_event.avsc": f"{settings.kafka.topic_settlement_expectations}-value",
}


def _registry_request(method: str, path: str, **kwargs) -> requests.Response:
    base = settings.kafka.schema_registry_url.rstrip("/")
    url = f"{base}{path}"
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

    auth = None
    if settings.kafka.schema_registry_username:
        auth = (settings.kafka.schema_registry_username, settings.kafka.schema_registry_password)

    resp = requests.request(method, url, headers=headers, auth=auth, timeout=15, **kwargs)
    return resp


def _check_registry_reachable() -> None:
    try:
        resp = _registry_request("GET", "/subjects")
        resp.raise_for_status()
    except Exception as exc:
        print(f"ERROR: Cannot reach Schema Registry at {settings.kafka.schema_registry_url}")
        print(f"       {exc}")
        print("       Is docker-compose up? Run: docker-compose up -d schema-registry")
        sys.exit(1)


def _set_compatibility(subject: str, level: str = "BACKWARD") -> None:
    resp = _registry_request(
        "PUT",
        f"/config/{subject}",
        json={"compatibility": level},
    )
    if resp.status_code not in (200, 201):
        print(f"  WARN: Could not set compatibility for {subject}: {resp.text}")
    else:
        print(f"  Compatibility set to {level} for subject: {subject}")


def _register_schema(avsc_file: str, subject: str) -> int:
    schema_path = SCHEMAS_DIR / avsc_file
    schema_str = schema_path.read_text()

    # Validate it's valid JSON
    try:
        json.loads(schema_str)
    except json.JSONDecodeError as exc:
        print(f"ERROR: Invalid JSON in {avsc_file}: {exc}")
        sys.exit(1)

    payload = {"schema": schema_str, "schemaType": "AVRO"}
    resp = _registry_request("POST", f"/subjects/{subject}/versions", json=payload)

    if resp.status_code in (200, 201):
        schema_id = resp.json()["id"]
        print(f"  Registered: {avsc_file} -> subject={subject}, schema_id={schema_id}")
        return schema_id
    elif resp.status_code == 409:
        # Schema already exists and is compatible
        print(f"  Already registered (compatible): {avsc_file} -> {subject}")
        # Fetch the existing ID
        check = _registry_request("POST", f"/subjects/{subject}", json=payload)
        return check.json().get("id", -1)
    else:
        print(f"ERROR: Failed to register {avsc_file}: HTTP {resp.status_code} {resp.text}")
        sys.exit(1)


def _verify_schema(subject: str) -> None:
    resp = _registry_request("GET", f"/subjects/{subject}/versions/latest")
    if resp.status_code == 200:
        info = resp.json()
        print(f"  Verified: {subject} v{info['version']} (id={info['id']})")
    else:
        print(f"  WARN: Could not verify {subject}: {resp.text}")


def main() -> None:
    print(f"Schema Registry: {settings.kafka.schema_registry_url}")
    print()

    _check_registry_reachable()

    print("Registering schemas...")
    registered_ids = {}
    for avsc_file, subject in SCHEMA_SUBJECTS.items():
        _set_compatibility(subject)
        schema_id = _register_schema(avsc_file, subject)
        registered_ids[subject] = schema_id

    print()
    print("Verifying registered schemas...")
    for subject in SCHEMA_SUBJECTS.values():
        _verify_schema(subject)

    print()
    print("All schemas registered successfully.")
    print()
    print("Registered subject -> schema ID mapping:")
    for subject, sid in registered_ids.items():
        print(f"  {subject}: {sid}")


if __name__ == "__main__":
    main()

"""
Centralised configuration via Pydantic Settings.

All values are loaded from environment variables (or a .env file).
No secrets ever appear in source code — references like KAFKA__BOOTSTRAP_SERVERS
map to env vars using the double-underscore delimiter Pydantic uses for nested models.

Usage:
    from pipeline.config import settings
    print(settings.kafka.bootstrap_servers)
"""

from __future__ import annotations

from enum import Enum
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    LOCAL = "local"
    STAGING = "staging"
    PRODUCTION = "production"


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA__", env_nested_delimiter="__")

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated Kafka broker list",
    )
    schema_registry_url: str = Field(
        default="http://localhost:8081",
        description="Confluent Schema Registry base URL",
    )
    schema_registry_username: str | None = None
    schema_registry_password: str | None = None

    # Topics
    topic_raw_transactions: str = "raw-transactions"
    topic_settlement_expectations: str = "settlement-expectations"
    topic_reconciliation_alerts: str = "reconciliation-alerts"
    topic_dlq_transactions: str = "dlq-transactions"

    # Consumer group
    consumer_group_bronze: str = "reconciliation-bronze-consumer"
    consumer_group_silver: str = "reconciliation-silver-consumer"

    # Producer
    acks: str = "all"  # idempotent requires acks=all
    enable_idempotence: bool = True
    max_in_flight_requests_per_connection: int = 5
    linger_ms: int = 10
    batch_size: int = 65536  # 64 KB

    # Security (Confluent Cloud / SASL_SSL)
    security_protocol: str = "PLAINTEXT"  # override to SASL_SSL for cloud
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None

    @field_validator("acks")
    @classmethod
    def acks_must_be_valid(cls, v: str) -> str:
        if v not in {"0", "1", "all", "-1"}:
            raise ValueError(f"Invalid acks value: {v}")
        return v


class DeltaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DELTA__", env_nested_delimiter="__")

    base_path: str = Field(
        default="dbfs:/merchant-reconciliation",
        description="Root DBFS / object-store path for all Delta tables",
    )
    bronze_path: str = "bronze"
    silver_path: str = "silver"
    gold_path: str = "gold"

    checkpoint_base: str = Field(
        default="dbfs:/checkpoints/merchant-reconciliation",
        description="Root checkpoint directory for all streaming queries",
    )

    @property
    def bronze_table_path(self) -> str:
        return f"{self.base_path}/{self.bronze_path}"

    @property
    def silver_table_path(self) -> str:
        return f"{self.base_path}/{self.silver_path}"

    @property
    def gold_table_path(self) -> str:
        return f"{self.base_path}/{self.gold_path}"


class SnowflakeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE__", env_nested_delimiter="__")

    account: str = Field(default="", description="Snowflake account identifier")
    user: str = Field(default="", description="Service account username")
    password: str | None = None
    private_key_path: str | None = None  # for key-pair auth (preferred in prod)
    private_key_passphrase: str | None = None

    database: str = "RECONCILIATION_DB"
    schema_name: str = "GOLD"
    warehouse: str = "RECONCILIATION_WH"
    role: str = "RECONCILIATION_ENGINEER"

    # Table names
    table_reconciliation_results: str = "RECONCILIATION_RESULTS"
    table_anomaly_log: str = "ANOMALY_LOG"


class StreamingSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STREAMING__", env_nested_delimiter="__")

    # Watermark durations
    transaction_watermark_minutes: int = Field(
        default=10,
        description=(
            "Watermark on raw-transactions stream. "
            "Events older than this relative to the max seen event time are dropped. "
            "Shorter = lower latency but more late-data loss."
        ),
    )
    settlement_watermark_minutes: int = Field(
        default=30,
        description=(
            "Watermark on settlement-expectations stream. "
            "Settlements often arrive 5–15 min late from payment networks; "
            "30 min gives enough slack without blowing up state store size."
        ),
    )

    # Reconciliation thresholds
    amount_mismatch_tolerance_pct: float = Field(
        default=0.01,
        description="Fractional amount difference above which a mismatch anomaly fires (0.01 = 0.01%)",
    )

    # State TTL (minutes) — evict join state older than this
    join_state_ttl_minutes: int = 60

    # Trigger intervals
    bronze_trigger_seconds: int = 10
    silver_trigger_seconds: int = 15
    gold_trigger_seconds: int = 20

    # Gold freshness SLA — alert if Gold lag exceeds this
    gold_freshness_sla_minutes: int = 5


class ObservabilitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OBS__", env_nested_delimiter="__")

    log_level: str = "INFO"
    prometheus_port: int = 8000
    enable_json_logging: bool = True


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    environment: Environment = Environment.LOCAL

    kafka: KafkaSettings = KafkaSettings()
    delta: DeltaSettings = DeltaSettings()
    snowflake: SnowflakeSettings = SnowflakeSettings()
    streaming: StreamingSettings = StreamingSettings()
    observability: ObservabilitySettings = ObservabilitySettings()

    def kafka_producer_conf(self) -> dict:
        """Return librdkafka producer config dict."""
        conf: dict = {
            "bootstrap.servers": self.kafka.bootstrap_servers,
            "acks": self.kafka.acks,
            "enable.idempotence": str(self.kafka.enable_idempotence).lower(),
            "max.in.flight.requests.per.connection": self.kafka.max_in_flight_requests_per_connection,
            "linger.ms": self.kafka.linger_ms,
            "batch.size": self.kafka.batch_size,
            "compression.type": "snappy",
            "security.protocol": self.kafka.security_protocol,
        }
        if self.kafka.sasl_mechanism:
            conf["sasl.mechanisms"] = self.kafka.sasl_mechanism
            conf["sasl.username"] = self.kafka.sasl_username or ""
            conf["sasl.password"] = self.kafka.sasl_password or ""
        return conf

    def schema_registry_conf(self) -> dict:
        """Return Schema Registry client config."""
        conf: dict = {"url": self.kafka.schema_registry_url}
        if self.kafka.schema_registry_username:
            conf["basic.auth.credentials.source"] = "USER_INFO"
            conf["basic.auth.user.info"] = (
                f"{self.kafka.schema_registry_username}:{self.kafka.schema_registry_password}"
            )
        return conf


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a singleton Settings instance (cached after first call)."""
    return Settings()


# Module-level alias for convenience
settings = get_settings()

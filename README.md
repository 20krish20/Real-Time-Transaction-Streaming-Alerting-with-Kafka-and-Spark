# Merchant Revenue Reconciliation & Anomaly Detection Platform

A production-grade, real-time streaming system that reconciles card transaction records against bank;/' settlement expectations, detects financial anomalies, and provides audit-ready reporting through Snowflake. Built with exactly-once semantics, stateful stream processing, and full observability.

---

## Problem Statement

Payment networks generate millions of card transactions per day. Each transaction must be matched against a corresponding bank settlement record to ensure merchants receive the correct payment. Discrepancies — whether due to amount mismatches, missing settlements, or duplicate submissions — represent direct revenue risk. Manual reconciliation is too slow; this platform detects anomalies in near-real-time.

**Key business metrics:**
- 0.01% amount mismatch tolerance (industry standard for card networks)
- 5-minute SLA from transaction to Gold layer alert
- 30-minute tolerance for late-arriving settlements (bank batch processing reality)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                          │
│                                                                              │
│  Payment Gateway          Acquiring Bank                                     │
│  (POS / Online / ATM)     (Settlement Batch)                                 │
│        │                        │                                            │
│        ▼                        ▼                                            │
│  ┌───────────┐          ┌──────────────────┐                                 │
│  │transaction│          │   settlement     │                                 │
│  │ _producer │          │   _producer      │                                 │
│  │ (Avro)    │          │   (Avro + delays)│                                 │
│  └─────┬─────┘          └────────┬─────────┘                                │
└────────┼────────────────────────┼─────────────────────────────────────────-─┘
         │                        │
         ▼                        ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                    KAFKA (Confluent / local Docker)                         │
│                                                                            │
│  ┌─────────────────────┐    ┌──────────────────────────┐                  │
│  │  raw-transactions   │    │  settlement-expectations  │                  │
│  │  6 partitions       │    │  3 partitions             │                  │
│  │  7-day retention    │    │  30-day retention         │                  │
│  └──────────┬──────────┘    └────────────┬─────────────┘                  │
│             │  dlq-transactions ◄────────── (malformed messages)           │
│             │  reconciliation-alerts ◄────── (anomaly routing)             │
│                                                                            │
│  Schema Registry (Avro — BACKWARD compatible schema evolution)             │
└──────────────────────────────────────────────────────────────────────────-─┘
         │                        │
         ▼                        ▼
┌────────────────────────────────────────────────────────────────────────────┐
│               SPARK STRUCTURED STREAMING (Databricks)                      │
│                                                                            │
│  BRONZE: bronze_ingestion.py                                               │
│  • Avro deserialisation + schema validation (5 quality gates)              │
│  • DLQ routing via foreachBatch for malformed records                      │
│  • Delta append — 10s trigger, checkpoint for exactly-once                 │
│                          │                                                 │
│  SILVER: silver_dedup.py                                                   │
│  • Delta MERGE on (transaction_id, merchant_id) — idempotent dedup        │
│  • Merchant dimension enrichment, timestamp parsing, amount bucketing      │
│  • 15s trigger                                                             │
│                          │                                                 │
│  GOLD: gold_reconciliation.py                                              │
│  • Stream-to-stream join (10-min txn WM, 30-min settlement WM)            │
│  • Anomaly scoring: mismatch_pct, severity (NONE/LOW/MED/HIGH/CRITICAL)   │
│  • Alerts routed to reconciliation-alerts Kafka topic                      │
│  • Delta MERGE — exactly-once Gold write, 20s trigger                      │
└──────────────────────────────────────────────────────────────────────────-─┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   SNOWFLAKE (Gold Serving) │
                    │                           │
                    │  RECONCILIATION_RESULTS   │
                    │  ANOMALY_LOG              │
                    │  V_DAILY_RECON_SUMMARY    │
                    │  V_CRITICAL_ANOMALIES_24H │
                    │                           │
                    │  RBAC:                    │
                    │   RECONCILIATION_ENGINEER │
                    │   RECONCILIATION_ANALYST  │
                    │  Column masking: CARD_ID  │
                    │  Time-travel: 90 days     │
                    └───────────────────────────┘
```

---

## Data Flow & Schema

### TransactionEvent (Avro)
| Field | Type | Notes |
|---|---|---|
| `transaction_id` | string | UUID — globally unique |
| `merchant_id` | string | Acquiring merchant |
| `card_id` | string | Tokenised PAN (PCI-safe, never raw) |
| `amount` | double | Transaction amount |
| `currency` | string | ISO 4217 |
| `channel` | enum | POS / ONLINE / ATM / CONTACTLESS / MOBILE |
| `card_type` | enum | VISA / MASTERCARD / AMEX / DISCOVER / UNIONPAY |
| `event_time` | timestamp-millis | Authoritative event-time clock (epoch ms) |
| `correlation_id` | string | Flows through every layer — distributed tracing |
| `geo_lat`, `geo_lon` | double? | POS/CONTACTLESS only |
| `mcc` | string? | ISO 18245 Merchant Category Code |

### SettlementEvent (Avro)
| Field | Type | Notes |
|---|---|---|
| `settlement_id` | string | Bank-assigned reference |
| `transaction_id` | string | Foreign key to TransactionEvent |
| `merchant_id` | string | Composite join key |
| `expected_amount` | double | Bank's expected settlement |
| `settlement_status` | enum | APPROVED / PENDING / REJECTED / REVERSED / DISPUTED |
| `interchange_fee` | double? | Network fee deducted |
| `net_settlement_amount` | double? | expected_amount − interchange_fee |

### Anomaly Severity Classification
| Severity | Condition |
|---|---|
| `CRITICAL` | mismatch_pct > 5% **or** status in (REJECTED, REVERSED) |
| `HIGH` | mismatch_pct 1–5% **or** status = DISPUTED |
| `MEDIUM` | mismatch_pct 0.01–1% |
| `LOW` | status = PENDING, within amount tolerance |
| `NONE` | Fully reconciled, within 0.01% tolerance |

---

## Production Engineering Decisions (Interview Tradeoffs)

### 1. Spark Structured Streaming vs Apache Flink

**Chose Spark because:**
- Team runs on Databricks — managed runtime eliminates ops overhead for a small platform team
- Delta Lake native integration: ACID transactions, time-travel, OPTIMIZE, and Z-ORDER are first-class in Spark/Databricks
- Watermark-based stateful stream-to-stream joins are mature and production-tested at this exact pattern
- Unified platform: Spark batch, streaming, and ML on the same cluster reduces operational surface area

**When Flink would be the right call:**
- Sub-second latency required (Flink continuous processing vs Spark micro-batches with minimum ~100ms overhead)
- Multi-engine table format requirement (Flink natively supports Iceberg; Delta Flink connector exists but is less mature)
- Complex Event Processing (CEP) for fraud pattern sequences — Flink's CEP API is purpose-built for this

### 2. Stream-to-Stream Join vs Stream-to-Table Join

**Chose stream-to-stream because:**
- Settlements arrive 5–15 minutes *after* the transaction — the settlement row doesn't exist in any table when the transaction arrives
- Stream-to-table (lookup join) requires the lookup side to be pre-populated. This forces either a race condition or an artificial batch delay that defeats the real-time requirement
- Stream-to-stream allows both sides to arrive in any order within the watermark window, matching actual payment network behaviour
- The asymmetric watermarks (10 min transactions, 30 min settlements) encode the business SLA directly in the streaming semantics

**When stream-to-table is the right call:**
- Joining against a slowly-changing dimension (merchant attributes, card risk tier) — Silver does exactly this with a broadcast join on the merchant dimension
- The lookup side is pre-loaded from a batch file (end-of-day bank statement) rather than a real-time stream

### 3. Exactly-Once vs At-Least-Once Semantics

**Chose exactly-once for Gold because:**
- Financial reconciliation requires zero duplicate records — a double-counted settlement creates a phantom credit that triggers false alerts and incorrect payouts
- Implementation stack: Spark Structured Streaming checkpoint (exactly-once Kafka offset tracking) + Delta MERGE on `(transaction_id, merchant_id)` (idempotent sink). Together this forms end-to-end EOS without requiring Kafka transactions on the sink side
- Kafka producer idempotence (`enable.idempotence=true`, `acks=all`, `max.in.flight=5`) prevents duplicate records at the broker level

**Cost of exactly-once:**
- ~10–15% throughput reduction vs at-least-once due to coordination overhead
- Larger checkpoint state (offset tracking per partition)
- Acceptable for this use case; 1,000 TPS is well within the overhead budget

**When at-least-once is the right call:**
- Downstream deduplicates (ClickHouse `ReplacingMergeTree`, Snowflake `MERGE`)
- Metrics aggregation where one extra event barely moves the needle
- Latency is the primary constraint (at-least-once has lower p99 latency)

### 4. Watermark Duration Tradeoff

The watermark defines how long Spark holds state waiting for late events:
- **Shorter watermark** (e.g., 5 min): lower memory, faster Gold output, but late settlements arriving after 5 min are **silently dropped** → missing reconciliations
- **Longer watermark** (e.g., 60 min): captures more late data, but state store grows proportionally → higher memory and checkpoint cost

**Our choice — 10 min (transactions) / 30 min (settlements):**
- 95th percentile settlement delay in payment networks is ~12 min; 30 min captures ~99.5% of settlements
- State store size at 1k TPS: O(30 min × 1,000 events/s × avg_row_size) ≈ 3–5 GB — manageable on `m5d.xlarge` workers
- Gold records lag up to 30 min behind real time — within the 5-minute SLA for *anomaly alerts* (which route through Kafka immediately in `foreachBatch`) but outside it for the Gold Delta snapshot

### 5. Micro-Batch vs Continuous Processing

**Chose micro-batch (default Spark mode) because:**
- The 5-minute Gold freshness SLA is comfortably met by 20-second micro-batches
- Micro-batch provides at-batch-boundary checkpointing — clean recovery with no partial batch re-processing ambiguity
- Continuous processing (Spark's experimental mode) has ~2x overhead per record for epoch coordination and does not support watermark-based stream-to-stream joins (which is a hard requirement here)
- Databricks Photon engine accelerates micro-batch execution; the performance gap vs continuous narrows significantly with Photon

### 6. Delta Lake vs Apache Iceberg

**Chose Delta Lake because:**
- Databricks Runtime ships Delta natively — zero configuration overhead
- `MERGE` (upsert) performance: Databricks-optimised Delta MERGE is significantly faster than Iceberg's MERGE on Spark, especially with the deletion vector optimisation in DBR 13+
- Change Data Feed (`readChangeFeed=true`) powers the Snowflake writer's incremental sync. Iceberg's incremental read equivalent is less mature on Spark
- Databricks UC (Unity Catalog) provides table-level ACLs, column-level masking, and audit logs on Delta tables out of the box

**When Iceberg would win:**
- Multi-engine requirement: same table consumed by Trino, Spark, and Flink simultaneously
- Vendor lock-in to Databricks is a constraint (Iceberg is cloud-agnostic)
- Snowflake's native Iceberg support (`EXTERNAL ICEBERG TABLE`) would eliminate the Spark-to-Snowflake writer entirely

### 7. Snowflake Kafka Connector vs Spark Writing to Snowflake

**Chose Spark connector (bulk COPY) for the Gold Delta → Snowflake path because:**
- Gold Delta is the system of record; Snowflake is a serving layer copy. The Spark connector reads only changed rows via Delta CDF, making syncs cheap
- Spark bulk-loads via internal Snowflake stage (COPY INTO) — 10–100x faster than row-by-row JDBC for large batches
- Allows complex column remapping and masking logic before data lands in Snowflake

**When the Snowflake Kafka Connector is the right call:**
- Direct streaming of the `reconciliation-alerts` topic into `ANOMALY_LOG` (lower latency, no Spark required)
- Raw event archival: landing `raw-transactions` directly into a Snowflake staging schema for compliance retention
- Reducing platform cost: if Spark is overkill for the serving path, the Kafka connector eliminates a job cluster

---

## Realistic Simulation Properties

| Property | Simulated value | Real-world source |
|---|---|---|
| Transaction rate | 1,000 TPS | Mid-size payment processor peak |
| Settlement delay | 5–15 minutes | Bank batch window |
| Amount mismatch rate | 2% | Interchange rounding, FX conversion |
| Missing settlement rate | 0.5% | Bank-side silent drops |
| Duplicate settlement rate | 0.1% | Bank retry storms after network partition |
| Geographic distribution | 60% US (weighted) | US card network dominance |
| MCC distribution | Top-10 MCCs weighted | Grocery, gas, restaurant, travel, retail |
| Amount distribution | $1–$5,000 uniform | Excludes micro-payments and wire transfers |

---

## Project Structure

```
.
├── schemas/
│   ├── transaction_event.avsc       # Avro — backward-compatible evolution
│   └── settlement_event.avsc
├── pipeline/
│   ├── config.py                    # Pydantic settings (env-var driven, no secrets in code)
│   ├── bronze_ingestion.py          # Kafka → Bronze Delta (5 quality gates, DLQ routing)
│   ├── silver_dedup.py              # Bronze → Silver (Delta MERGE dedup, enrichment)
│   ├── gold_reconciliation.py       # Stream-to-stream join, anomaly scoring, alerts
│   └── snowflake_writer.py          # Gold Delta CDF → Snowflake (bulk COPY)
├── producer/
│   ├── transaction_producer.py      # 1,000 TPS Avro producer — realistic geo/MCC data
│   └── settlement_producer.py       # Delayed settlement simulation (2% mismatch, 0.5% missing)
├── quality/expectations/
│   ├── bronze/bronze_suite.py       # Great Expectations Bronze checks
│   └── gold/gold_suite.py           # Great Expectations Gold checks (business rules)
├── snowflake/
│   ├── ddl.sql                      # Tables, views, warehouse, resource monitor, time-travel
│   └── rbac.sql                     # Roles, column masking (CARD_ID), row-level security
├── tests/
│   ├── conftest.py                  # PySpark session fixture + sample DataFrames
│   ├── unit/
│   │   ├── test_reconciliation_logic.py  # 16 tests: severity classification + Bronze validation
│   │   ├── test_config.py                # Settings validation
│   │   └── test_producer_simulation.py  # Statistical simulation property tests
│   └── integration/
│       └── test_kafka_pipeline.py        # testcontainers Kafka E2E tests
├── .github/workflows/
│   ├── ci.yml                       # ruff + mypy + unit tests + Avro schema validation
│   ├── integration-tests.yml        # Docker-based integration tests (PR to main)
│   └── databricks-deploy.yml        # Asset bundle deploy on merge to main
├── docker-compose.yml               # Kafka + ZooKeeper + Schema Registry + Kafka UI
├── databricks.yml                   # 4 streaming jobs as Databricks Asset Bundle
└── pyproject.toml                   # Dependencies, ruff config, mypy, pytest markers
```

---

## Quick Start (Local)

**Prerequisites:** Docker Desktop, Python 3.9+, Java 11+

```bash
# 1. Start the full Kafka stack
docker-compose up -d
# Kafka UI: http://localhost:8080   Schema Registry: http://localhost:8081

# 2. Install Python dependencies
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 3. Run producers (separate terminals)
python -m producer.transaction_producer      # 1,000 TPS
python -m producer.settlement_producer       # delayed settlements with chaos

# 4. Run unit tests (no Docker required)
pytest tests/unit/ -m unit -v

# 5. Run integration tests (requires Docker)
pytest tests/integration/ -m integration -v
```

---

## CI/CD Pipeline

| Workflow | Trigger | Jobs |
|---|---|---|
| `ci.yml` | Every push | ruff lint → mypy → unit tests + coverage → Avro validation |
| `integration-tests.yml` | PR to main | testcontainers Kafka end-to-end |
| `databricks-deploy.yml` | Merge to main | `databricks bundle deploy --target staging`; production via manual dispatch |

---

## Snowflake Usage

```sql
-- Daily reconciliation KPIs (RECONCILIATION_ANALYST role)
SELECT * FROM GOLD.V_DAILY_RECONCILIATION_SUMMARY
WHERE RECONCILIATION_DATE >= DATEADD('DAY', -7, CURRENT_DATE())
ORDER BY ANOMALY_COUNT DESC;

-- Critical anomalies in last 24 hours
SELECT * FROM GOLD.V_CRITICAL_ANOMALIES_LAST_24H;

-- Time-travel audit: what did Gold look like 24 hours ago?
SELECT * FROM GOLD.RECONCILIATION_RESULTS
AT (TIMESTAMP => DATEADD('HOUR', -24, CURRENT_TIMESTAMP()))
WHERE MERCHANT_ID = 'merch_0042'
  AND IS_ANOMALY = TRUE;
```

---

## Observability

- **Structured logging**: every layer emits JSON logs with `correlation_id`, `batch_id`, and `layer` — searchable in Databricks Log Analytics
- **Spark Streaming metrics**: batch duration, processing rate, watermark lag, state store size — exposed via Spark UI and scrapeable by Prometheus
- **Kafka consumer lag**: Kafka UI (local) or Confluent Control Center (cloud) per consumer group
- **Gold freshness SLA**: Databricks SQL alert on `MAX(RECONCILED_AT) < NOW() - 5 MINUTES` — pages on-call if breached

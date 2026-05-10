-- ============================================================
-- Merchant Revenue Reconciliation Platform — Snowflake DDL
--
-- Database: RECONCILIATION_DB
-- Schema:   GOLD
--
-- Tables:
--   RECONCILIATION_RESULTS  — primary fact table (Gold layer)
--   ANOMALY_LOG             — anomaly event log for alerting
--   DIM_MERCHANT            — merchant dimension
--   DIM_CARD                — card dimension
--
-- Features:
--   - Time-travel enabled (90-day retention for audit)
--   - Clustering keys for query performance
--   - Resource monitor on warehouse
--   - Change tracking for incremental ETL
-- ============================================================


-- ── Database & Schema ─────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS RECONCILIATION_DB
    DATA_RETENTION_TIME_IN_DAYS = 90   -- time-travel for audit trail
    COMMENT = 'Merchant Revenue Reconciliation Platform';

USE DATABASE RECONCILIATION_DB;

CREATE SCHEMA IF NOT EXISTS GOLD
    DATA_RETENTION_TIME_IN_DAYS = 90
    COMMENT = 'Gold layer: fully reconciled, audit-ready data';

CREATE SCHEMA IF NOT EXISTS STAGING
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Staging area for Kafka connector ingestion';

USE SCHEMA GOLD;


-- ── Warehouse ─────────────────────────────────────────────────────────────────

CREATE WAREHOUSE IF NOT EXISTS RECONCILIATION_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60               -- suspend after 60s idle (cost control)
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Reconciliation platform query warehouse';

-- Resource monitor: alert at 80% credit usage, suspend at 100%
CREATE RESOURCE MONITOR IF NOT EXISTS RECONCILIATION_MONITOR
    WITH CREDIT_QUOTA = 100
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 80 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE RECONCILIATION_WH SET RESOURCE_MONITOR = RECONCILIATION_MONITOR;


-- ── RECONCILIATION_RESULTS ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS GOLD.RECONCILIATION_RESULTS (
    -- Primary key
    TRANSACTION_ID          VARCHAR(50)     NOT NULL,
    MERCHANT_ID             VARCHAR(30)     NOT NULL,

    -- Transaction details
    CARD_ID                 VARCHAR(50),
    CUSTOMER_ID             VARCHAR(50),
    CHANNEL                 VARCHAR(20),
    CARD_TYPE               VARCHAR(15),
    MCC                     VARCHAR(10),
    COUNTRY                 VARCHAR(2),

    -- Amounts
    TRANSACTION_AMOUNT      NUMBER(18, 4)   NOT NULL,
    SETTLEMENT_AMOUNT       NUMBER(18, 4)   NOT NULL,
    AMOUNT_DELTA            NUMBER(18, 4),
    MISMATCH_PCT            NUMBER(10, 6),
    CURRENCY                VARCHAR(3),

    -- Settlement
    SETTLEMENT_ID           VARCHAR(50),
    SETTLEMENT_STATUS       VARCHAR(20),
    BANK_REFERENCE          VARCHAR(50),
    INTERCHANGE_FEE         NUMBER(12, 4),
    NET_SETTLEMENT_AMOUNT   NUMBER(18, 4),

    -- Anomaly classification
    ANOMALY_SEVERITY        VARCHAR(10),     -- NONE / LOW / MEDIUM / HIGH / CRITICAL
    IS_ANOMALY              BOOLEAN,
    IS_AMOUNT_MISMATCH      BOOLEAN,
    IS_STATUS_ANOMALY       BOOLEAN,

    -- Timestamps
    TXN_EVENT_TS            TIMESTAMP_TZ    NOT NULL,
    SETTLE_EVENT_TS         TIMESTAMP_TZ,
    RECONCILED_AT           TIMESTAMP_TZ,
    SF_LOADED_AT            TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    -- Tracing
    CORRELATION_ID          VARCHAR(50),

    -- Constraints
    CONSTRAINT PK_RECON_RESULTS PRIMARY KEY (TRANSACTION_ID, MERCHANT_ID)
)
    CLUSTER BY (MERCHANT_ID, DATE_TRUNC('DAY', TXN_EVENT_TS))  -- optimise merchant + date queries
    DATA_RETENTION_TIME_IN_DAYS = 90
    CHANGE_TRACKING = TRUE                                       -- for incremental CDC
    COMMENT = 'Gold layer reconciliation results — one row per transaction-settlement pair';


-- ── ANOMALY_LOG ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS GOLD.ANOMALY_LOG (
    ANOMALY_ID              VARCHAR(50)     DEFAULT UUID_STRING() NOT NULL,
    TRANSACTION_ID          VARCHAR(50)     NOT NULL,
    MERCHANT_ID             VARCHAR(30)     NOT NULL,
    ANOMALY_SEVERITY        VARCHAR(10)     NOT NULL,
    MISMATCH_PCT            NUMBER(10, 6),
    SETTLEMENT_STATUS       VARCHAR(20),
    CORRELATION_ID          VARCHAR(50),
    DETECTED_AT             TIMESTAMP_TZ    NOT NULL,
    SF_LOADED_AT            TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ANOMALY_LOG PRIMARY KEY (ANOMALY_ID)
)
    CLUSTER BY (MERCHANT_ID, DATE_TRUNC('DAY', DETECTED_AT))
    DATA_RETENTION_TIME_IN_DAYS = 90
    COMMENT = 'Log of every anomaly detected by the reconciliation engine';


-- ── DIM_MERCHANT ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS GOLD.DIM_MERCHANT (
    MERCHANT_ID             VARCHAR(30)     NOT NULL,
    MERCHANT_NAME           VARCHAR(200),
    MERCHANT_CATEGORY       VARCHAR(50),
    MCC                     VARCHAR(10),
    ACQUIRING_BANK          VARCHAR(100),
    RISK_TIER               VARCHAR(10),    -- LOW / MEDIUM / HIGH
    COUNTRY_OF_REGISTRATION VARCHAR(2),
    ONBOARDING_DATE         DATE,
    IS_ACTIVE               BOOLEAN         DEFAULT TRUE,
    CREATED_AT              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_MERCHANT PRIMARY KEY (MERCHANT_ID)
)
    DATA_RETENTION_TIME_IN_DAYS = 90
    COMMENT = 'Merchant master data dimension';


-- ── DIM_CARD ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS GOLD.DIM_CARD (
    CARD_ID                 VARCHAR(50)     NOT NULL,
    CUSTOMER_ID             VARCHAR(50),
    CARD_TYPE               VARCHAR(15),
    HOME_COUNTRY            VARCHAR(2),
    RISK_TIER               VARCHAR(10),
    ISSUED_DATE             DATE,
    EXPIRY_DATE             DATE,
    IS_ACTIVE               BOOLEAN         DEFAULT TRUE,
    CREATED_AT              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_CARD PRIMARY KEY (CARD_ID)
)
    DATA_RETENTION_TIME_IN_DAYS = 90
    COMMENT = 'Card dimension — tokenised, never raw PAN';


-- ── Useful views ──────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW GOLD.V_DAILY_RECONCILIATION_SUMMARY AS
SELECT
    DATE_TRUNC('DAY', TXN_EVENT_TS)    AS RECONCILIATION_DATE,
    MERCHANT_ID,
    COUNT(*)                            AS TOTAL_TRANSACTIONS,
    SUM(TRANSACTION_AMOUNT)             AS TOTAL_TRANSACTION_AMOUNT,
    SUM(SETTLEMENT_AMOUNT)              AS TOTAL_SETTLEMENT_AMOUNT,
    SUM(AMOUNT_DELTA)                   AS TOTAL_DELTA,
    AVG(MISMATCH_PCT)                   AS AVG_MISMATCH_PCT,
    SUM(CASE WHEN IS_ANOMALY THEN 1 ELSE 0 END)         AS ANOMALY_COUNT,
    SUM(CASE WHEN ANOMALY_SEVERITY = 'CRITICAL' THEN 1 ELSE 0 END) AS CRITICAL_COUNT,
    SUM(CASE WHEN ANOMALY_SEVERITY = 'HIGH'     THEN 1 ELSE 0 END) AS HIGH_COUNT
FROM GOLD.RECONCILIATION_RESULTS
GROUP BY 1, 2
ORDER BY 1 DESC, ANOMALY_COUNT DESC;

COMMENT ON VIEW GOLD.V_DAILY_RECONCILIATION_SUMMARY IS
    'Daily merchant-level reconciliation KPIs — used by RECONCILIATION_ANALYST role';


CREATE OR REPLACE VIEW GOLD.V_CRITICAL_ANOMALIES_LAST_24H AS
SELECT
    TRANSACTION_ID,
    MERCHANT_ID,
    ANOMALY_SEVERITY,
    TRANSACTION_AMOUNT,
    SETTLEMENT_AMOUNT,
    MISMATCH_PCT,
    SETTLEMENT_STATUS,
    CORRELATION_ID,
    DETECTED_AT
FROM GOLD.ANOMALY_LOG
WHERE
    ANOMALY_SEVERITY IN ('CRITICAL', 'HIGH')
    AND DETECTED_AT >= DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
ORDER BY DETECTED_AT DESC;

COMMENT ON VIEW GOLD.V_CRITICAL_ANOMALIES_LAST_24H IS
    'Real-time view of HIGH/CRITICAL anomalies in the last 24 hours';

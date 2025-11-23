# Real-Time Payments Analytics – Architecture

## Overview

This project implements a real-time analytics pipeline for card payments using:

- **Kafka** for event ingestion
- **Spark Structured Streaming on Databricks** for processing
- **Delta Lake** Bronze/Silver layers for replayable storage
- **Snowflake** for serving analytics queries

## Flow

1. A Python producer publishes synthetic payment events to the Kafka topic `payments.raw`.
2. The **Bronze** streaming job (Databricks notebook `01_bronze_from_kafka.py`) reads from Kafka and stores parsed events in the Delta table `payments_bronze`.
3. The **Silver** streaming job (Databricks notebook `02_silver_enrichment_and_flags.py`) enriches those events with card metadata and derives flags such as `is_high_value` and `is_international`, writing to `payments_silver`.
4. A scheduled batch notebook (`03_load_to_snowflake_batch.py`) loads records from `payments_silver` into the Snowflake table `FACT_TRANSACTIONS_STREAMING` for near real-time BI and reporting.

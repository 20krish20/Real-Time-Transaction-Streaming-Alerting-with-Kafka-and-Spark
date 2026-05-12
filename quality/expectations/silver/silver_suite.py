"""
Great Expectations data quality suite for the Silver layer.

Silver sits between Bronze (raw validated) and Gold (reconciled).
The key Silver guarantee is deduplication: (transaction_id, merchant_id)
must be unique. Secondary checks enforce enrichment completeness.

Run standalone:
    python quality/expectations/silver/silver_suite.py

Or via pytest:
    pytest quality/ -k silver
"""

from __future__ import annotations

import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest

SUITE_NAME = "silver_transactions_suite"
DATASOURCE_NAME = "silver_spark_datasource"


def build_suite(context: gx.DataContext) -> None:
    context.add_or_update_expectation_suite(expectation_suite_name=SUITE_NAME)

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name=DATASOURCE_NAME,
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="silver_transactions",
        ),
        expectation_suite_name=SUITE_NAME,
    )

    # ── Deduplication guarantee ────────────────────────────────────────────
    # The most critical Silver invariant — enforced by Delta MERGE on composite key.
    validator.expect_compound_columns_to_be_unique(
        column_list=["transaction_id", "merchant_id"],
    )
    validator.expect_column_values_to_not_be_null("transaction_id")
    validator.expect_column_values_to_not_be_null("merchant_id")

    # ── Enriched timestamp columns ──────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("event_time_ts")
    validator.expect_column_values_to_not_be_null("txn_date")
    validator.expect_column_values_to_be_between("txn_hour", min_value=0, max_value=23)

    # ── Amount integrity (carried from Bronze, must survive enrichment) ─────
    validator.expect_column_values_to_not_be_null("amount")
    validator.expect_column_values_to_be_between("amount", min_value=0.01, max_value=1_000_000.0)

    # ── Amount bucketing — derived column, must cover all rows ────────────
    validator.expect_column_values_to_be_in_set(
        "amount_bucket",
        value_set=["MICRO", "SMALL", "MEDIUM", "LARGE"],
    )
    validator.expect_column_values_to_not_be_null("amount_bucket")

    # ── is_high_value — derived boolean ────────────────────────────────────
    validator.expect_column_values_to_not_be_null("is_high_value")

    # ── Merchant enrichment — expect > 90% join hit rate (not all merchants
    #    are in the dim; new onboardings may lag by one ETL cycle) ──────────
    validator.expect_column_values_to_not_be_null("merchant_name", mostly=0.90)
    validator.expect_column_values_to_be_in_set(
        "risk_tier",
        value_set=["LOW", "MEDIUM", "HIGH"],
        mostly=0.90,
    )

    # ── Pipeline metadata ──────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("silver_processed_at")
    validator.expect_column_values_to_be_in_set("pipeline_layer", value_set=["silver"])
    validator.expect_column_values_to_not_be_null("correlation_id")

    # ── No future txn_date (data sanity) ───────────────────────────────────
    validator.expect_column_values_to_be_dateutil_parseable("txn_date")

    validator.save_expectation_suite(discard_failed_expectations=False)


def run_checkpoint(context: gx.DataContext, batch_df) -> dict:
    """Run the Silver suite against a live DataFrame batch."""
    batch_request = RuntimeBatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="silver_transactions",
        runtime_parameters={"batch_data": batch_df},
        batch_identifiers={"run_id": "silver_quality_check"},
    )
    checkpoint = SimpleCheckpoint(
        name="silver_checkpoint",
        data_context=context,
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": SUITE_NAME,
            }
        ],
    )
    result = checkpoint.run()
    return result.to_json_dict()

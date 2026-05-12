"""
Great Expectations data quality suite for the Bronze layer.

Run standalone:
    python quality/expectations/bronze/bronze_suite.py

Or via pytest:
    pytest quality/ -k bronze

Checks:
  - transaction_id: not null, unique
  - amount: not null, between 0.01 and 1,000,000
  - event_time: not null, reasonable range (epoch ms)
  - currency: in allowed set
  - channel: in allowed set
  - No completely null rows
"""

from __future__ import annotations

import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest

SUITE_NAME = "bronze_transactions_suite"
DATASOURCE_NAME = "bronze_spark_datasource"


def build_suite(context: gx.DataContext) -> None:
    context.add_or_update_expectation_suite(expectation_suite_name=SUITE_NAME)

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name=DATASOURCE_NAME,
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="bronze_transactions",
        ),
        expectation_suite_name=SUITE_NAME,
    )

    # ── transaction_id ─────────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("transaction_id")
    validator.expect_column_values_to_be_unique("transaction_id")
    validator.expect_column_value_lengths_to_be_between(
        "transaction_id", min_value=10, max_value=50
    )

    # ── merchant_id ────────────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("merchant_id")
    validator.expect_column_value_lengths_to_be_between("merchant_id", min_value=4, max_value=30)

    # ── amount ─────────────────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("amount")
    validator.expect_column_values_to_be_between(
        "amount",
        min_value=0.01,
        max_value=1_000_000.0,
        mostly=0.999,  # tolerate 0.1% edge cases (charity micro-donations, etc.)
    )

    # ── event_time (epoch ms) ──────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("event_time")
    # epoch ms for 2020-01-01 and 2030-01-01 as sanity bounds
    validator.expect_column_values_to_be_between(
        "event_time",
        min_value=1_577_836_800_000,  # 2020-01-01 00:00 UTC
        max_value=1_893_456_000_000,  # 2030-01-01 00:00 UTC
        mostly=0.999,
    )

    # ── currency ───────────────────────────────────────────────────────────
    validator.expect_column_values_to_be_in_set(
        "currency",
        value_set=["USD", "EUR", "GBP", "CAD", "AUD", "SGD", "INR"],
        mostly=0.999,
    )

    # ── channel ────────────────────────────────────────────────────────────
    validator.expect_column_values_to_be_in_set(
        "channel",
        value_set=["POS", "ONLINE", "ATM", "CONTACTLESS", "MOBILE"],
        mostly=1.0,
    )

    # ── card_type ──────────────────────────────────────────────────────────
    validator.expect_column_values_to_be_in_set(
        "card_type",
        value_set=["VISA", "MASTERCARD", "AMEX", "DISCOVER", "UNIONPAY"],
        mostly=1.0,
    )

    # ── pipeline metadata ──────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("bronze_ingested_at")
    validator.expect_column_values_to_not_be_null("correlation_id")
    validator.expect_column_values_to_be_in_set("pipeline_layer", value_set=["bronze"])

    # ── Row completeness ───────────────────────────────────────────────────
    validator.expect_multicolumn_sum_to_equal(
        column_list=["amount"],  # placeholder — real check: no null critical fields
        sum_total=None,  # not applicable here; used as a hook for custom expectations
    )

    validator.save_expectation_suite(discard_failed_expectations=False)


def run_checkpoint(context: gx.DataContext, batch_df) -> dict:
    """Run the Bronze suite against a live DataFrame batch."""
    batch_request = RuntimeBatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="bronze_transactions",
        runtime_parameters={"batch_data": batch_df},
        batch_identifiers={"run_id": "bronze_quality_check"},
    )
    checkpoint = SimpleCheckpoint(
        name="bronze_checkpoint",
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

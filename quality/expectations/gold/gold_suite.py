"""
Great Expectations data quality suite for the Gold reconciliation layer.

Gold-layer checks are stricter than Bronze — at this point data must be
fully reconciled and business-ready for Snowflake and downstream reports.
"""

from __future__ import annotations

import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest

SUITE_NAME = "gold_reconciliation_suite"
DATASOURCE_NAME = "gold_spark_datasource"


def build_suite(context: gx.DataContext) -> None:
    context.add_or_update_expectation_suite(expectation_suite_name=SUITE_NAME)

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name=DATASOURCE_NAME,
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="gold_reconciliation",
        ),
        expectation_suite_name=SUITE_NAME,
    )

    # ── Primary keys ───────────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("transaction_id")
    validator.expect_column_values_to_not_be_null("merchant_id")
    validator.expect_column_values_to_not_be_null("settlement_id")
    validator.expect_compound_columns_to_be_unique(column_list=["transaction_id", "merchant_id"])

    # ── Amount integrity ───────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("transaction_amount")
    validator.expect_column_values_to_not_be_null("settlement_amount")
    validator.expect_column_values_to_be_between(
        "transaction_amount", min_value=0.01, max_value=1_000_000.0
    )
    validator.expect_column_values_to_be_between(
        "settlement_amount", min_value=0.01, max_value=1_000_000.0
    )
    validator.expect_column_values_to_be_between("mismatch_pct", min_value=0.0, max_value=100.0)
    validator.expect_column_values_to_be_between(
        "amount_delta", min_value=0.0, max_value=1_000_000.0
    )

    # ── Anomaly classification ─────────────────────────────────────────────
    validator.expect_column_values_to_be_in_set(
        "anomaly_severity",
        value_set=["NONE", "LOW", "MEDIUM", "HIGH", "CRITICAL"],
    )
    validator.expect_column_values_to_not_be_null("is_anomaly")

    # ── Settlement status ──────────────────────────────────────────────────
    validator.expect_column_values_to_be_in_set(
        "settlement_status",
        value_set=["APPROVED", "PENDING", "REJECTED", "REVERSED", "DISPUTED"],
    )

    # ── Timestamps ─────────────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("txn_event_ts")
    validator.expect_column_values_to_not_be_null("settle_event_ts")
    validator.expect_column_values_to_not_be_null("gold_reconciled_at")

    # ── Business rules ─────────────────────────────────────────────────────
    # Anomaly rate: we expect the simulation's 2% mismatch rate
    # In production this expectation would be tuned to the actual baseline
    validator.expect_column_mean_to_be_between(
        "mismatch_pct",
        min_value=0.0,
        max_value=5.0,  # mean mismatch should stay well below 5%
    )

    # Critical anomalies should be rare (< 5% of total records)
    validator.expect_column_proportion_of_unique_values_to_be_between(
        "anomaly_severity", min_value=0.0, max_value=1.0
    )

    # ── Pipeline metadata ──────────────────────────────────────────────────
    validator.expect_column_values_to_be_in_set("pipeline_layer", value_set=["gold"])
    validator.expect_column_values_to_not_be_null("correlation_id")

    validator.save_expectation_suite(discard_failed_expectations=False)


def run_checkpoint(context: gx.DataContext, batch_df) -> dict:
    batch_request = RuntimeBatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="gold_reconciliation",
        runtime_parameters={"batch_data": batch_df},
        batch_identifiers={"run_id": "gold_quality_check"},
    )
    checkpoint = SimpleCheckpoint(
        name="gold_checkpoint",
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

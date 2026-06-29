"""
================================================================================
GOLD LAYER DQ CHECKS — [AWS Glue]
================================================================================
Layer-specific Data Quality config built on the shared framework
(aws/src/common/validations/dq_framework.py). Import and run inside the gold job.

Usage:
    from aws.src.gold.dq.gold_dq import gold_dq_config
    from aws.src.common.validations.dq_framework import DataQualityFramework
    dq = DataQualityFramework(spark, glue_client)
    report = dq.validate(df, gold_dq_config("gold_db.my_table"))
    if report.has_failures:
        dq.publish_metrics(report); raise Exception(report.summary)
Version : 2026-06-28
================================================================================
"""
from aws.src.common.validations.dq_framework import DQConfig, DQCheck, Severity


def gold_dq_config(table_name: str) -> DQConfig:
    """Return the standard gold-layer DQ checks. CHANGE_ME: tune per table."""
    return DQConfig(
        table_name=table_name,
        checks=[
            # Every layer: must not be empty
            DQCheck("row_count", "row_count", Severity.CRITICAL, {"min_count": 1}),
            # Gold: business rule (no negative metrics) + reconciliation vs silver
            DQCheck("no_negative_amount", "business_rule", Severity.HIGH,
                    {"sql": "SELECT * FROM __dq_check_table WHERE amount < 0"}),  # CHANGE_ME
            # DQCheck("reconcile", "reconciliation", Severity.HIGH, {"source_count": <silver_count>}),
        ],
    )

"""
================================================================================
SILVER LAYER DQ CHECKS — [AWS Glue]
================================================================================
Layer-specific Data Quality config built on the shared framework
(aws/src/common/validations/dq_framework.py). Import and run inside the silver job.

Usage:
    from aws.src.silver.dq.silver_dq import silver_dq_config
    from aws.src.common.validations.dq_framework import DataQualityFramework
    dq = DataQualityFramework(spark, glue_client)
    report = dq.validate(df, silver_dq_config("silver_db.my_table"))
    if report.has_failures:
        dq.publish_metrics(report); raise Exception(report.summary)
Version : 2026-06-28
================================================================================
"""
from aws.src.common.validations.dq_framework import DQConfig, DQCheck, Severity


def silver_dq_config(table_name: str) -> DQConfig:
    """Return the standard silver-layer DQ checks. CHANGE_ME: tune per table."""
    return DQConfig(
        table_name=table_name,
        checks=[
            # Every layer: must not be empty
            DQCheck("row_count", "row_count", Severity.CRITICAL, {"min_count": 1}),
            # Silver: business key not null + freshness
            DQCheck("pk_not_null", "completeness", Severity.CRITICAL,
                    {"column": "id", "max_null_pct": 0.0}),          # CHANGE_ME: your PK
            DQCheck("freshness", "freshness", Severity.MEDIUM, {"partition_column": "mnth_id"}),
        ],
    )

"""
================================================================================
BRONZE LAYER DQ CHECKS — [AWS Glue]
================================================================================
Layer-specific Data Quality config built on the shared framework
(aws/src/common/validations/dq_framework.py). Import and run inside the bronze job.

Usage:
    from aws.src.bronze.dq.bronze_dq import bronze_dq_config
    from aws.src.common.validations.dq_framework import DataQualityFramework
    dq = DataQualityFramework(spark, glue_client)
    report = dq.validate(df, bronze_dq_config("bronze_db.my_table"))
    if report.has_failures:
        dq.publish_metrics(report); raise Exception(report.summary)
Version : 2026-06-28
================================================================================
"""
from aws.src.common.validations.dq_framework import DQConfig, DQCheck, Severity


def bronze_dq_config(table_name: str) -> DQConfig:
    """Return the standard bronze-layer DQ checks. CHANGE_ME: tune per table."""
    return DQConfig(
        table_name=table_name,
        checks=[
            # Every layer: must not be empty
            DQCheck("row_count", "row_count", Severity.CRITICAL, {"min_count": 1}),
            # Bronze: raw lineage columns must exist + be populated
            DQCheck("ingest_date_present", "schema", Severity.HIGH,
                    {"expected_columns": ["_ingest_date", "_source_system"]}),
            DQCheck("ingest_date_not_null", "completeness", Severity.HIGH,
                    {"column": "_ingest_date", "max_null_pct": 0.0}),
        ],
    )

"""
================================================================================
SILVER LAYER DQ CHECKS — [Databricks]
================================================================================
Layer-specific DQ config built on databricks/src/common/validations/dq_framework.py.

Usage:
    from databricks.src.silver.dq.silver_dq import silver_dq_config
    from databricks.src.common.validations.dq_framework import DataQualityFramework
    dq = DataQualityFramework(spark)
    report = dq.validate(df, silver_dq_config("main.silver.my_table"))
    if report.has_failures:
        dq.publish_metrics(report); raise Exception(report.summary)
Version : 2026-06-28
================================================================================
"""
from databricks.src.common.validations.dq_framework import DQConfig, DQCheck, Severity


def silver_dq_config(table_name: str) -> DQConfig:
    return DQConfig(
        table_name=table_name,
        checks=[
            DQCheck("row_count", "row_count", Severity.CRITICAL, {"min_count": 1}),
            DQCheck("pk_not_null", "completeness", Severity.CRITICAL,
                    {"column": "id", "max_null_pct": 0.0}),          # CHANGE_ME: your PK
            DQCheck("freshness", "freshness", Severity.MEDIUM, {"partition_column": "mnth_id"}),
        ],
    )

"""
================================================================================
BRONZE LAYER DQ CHECKS — [Databricks]
================================================================================
Layer-specific DQ config built on databricks/src/common/validations/dq_framework.py.

Usage:
    from databricks.src.bronze.dq.bronze_dq import bronze_dq_config
    from databricks.src.common.validations.dq_framework import DataQualityFramework
    dq = DataQualityFramework(spark)
    report = dq.validate(df, bronze_dq_config("main.bronze.my_table"))
    if report.has_failures:
        dq.publish_metrics(report); raise Exception(report.summary)
Version : 2026-06-28
================================================================================
"""
from databricks.src.common.validations.dq_framework import DQConfig, DQCheck, Severity


def bronze_dq_config(table_name: str) -> DQConfig:
    return DQConfig(
        table_name=table_name,
        checks=[
            DQCheck("row_count", "row_count", Severity.CRITICAL, {"min_count": 1}),
            DQCheck("ingest_date_present", "schema", Severity.HIGH,
                    {"expected_columns": ["_ingest_date", "_source_system"]}),
            DQCheck("ingest_date_not_null", "completeness", Severity.HIGH,
                    {"column": "_ingest_date", "max_null_pct": 0.0}),
        ],
    )

"""
================================================================================
GOLD LAYER DQ CHECKS — [Databricks]
================================================================================
Layer-specific DQ config built on databricks/src/common/validations/dq_framework.py.

Usage:
    from databricks.src.gold.dq.gold_dq import gold_dq_config
    from databricks.src.common.validations.dq_framework import DataQualityFramework
    dq = DataQualityFramework(spark)
    report = dq.validate(df, gold_dq_config("main.gold.my_table"))
    if report.has_failures:
        dq.publish_metrics(report); raise Exception(report.summary)
Version : 2026-06-28
================================================================================
"""
from databricks.src.common.validations.dq_framework import DQConfig, DQCheck, Severity


def gold_dq_config(table_name: str) -> DQConfig:
    return DQConfig(
        table_name=table_name,
        checks=[
            DQCheck("row_count", "row_count", Severity.CRITICAL, {"min_count": 1}),
            DQCheck("no_negative_amount", "business_rule", Severity.HIGH,
                    {"sql": "SELECT * FROM __dq WHERE amount < 0"}),  # CHANGE_ME
        ],
    )

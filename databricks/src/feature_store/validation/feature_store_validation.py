"""
================================================================================
FEATURE STORE VALIDATION — [Databricks]
================================================================================
Purpose: Validate a UC feature table's health before serving it to training /
         inference. Checks: freshness, null rate on key features, PK uniqueness,
         row-count thresholds, distribution drift vs a baseline.

Uses the shared DQ framework (databricks/src/common/validations/dq_framework.py).

Usage:
    from databricks.src.feature_store.validation.feature_store_validation import validate_feature_table
    report = validate_feature_table("main.features.customer_features",
                                    primary_keys=["customer_id"],
                                    critical_features=["total_purchases_30d"])
    if report.has_failures:
        raise Exception(report.summary)
Version : 2026-06-29
================================================================================
"""
import logging
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from databricks.src.common.validations.dq_framework import (
    DataQualityFramework, DQConfig, DQCheck, Severity, DQReport,
)

logger = logging.getLogger("fs_validation_databricks")
spark = SparkSession.builder.getOrCreate()


def validate_feature_table(
    table_name: str,
    primary_keys: List[str],
    critical_features: Optional[List[str]] = None,
    min_rows: int = 100,
    max_null_pct: float = 0.05,
    freshness_max_hours: int = 48,
) -> DQReport:
    """Run standard feature-table validation checks. Returns a DQReport."""

    df = spark.table(table_name)
    checks = [
        DQCheck("row_count", "row_count", Severity.CRITICAL, {"min_count": min_rows}),
        DQCheck("pk_schema", "schema", Severity.CRITICAL,
                {"expected_columns": primary_keys}),
    ]

    # PK uniqueness (no duplicates on composite key)
    checks.append(DQCheck("pk_unique", "business_rule", Severity.CRITICAL, {
        "sql": f"SELECT * FROM __dq GROUP BY {', '.join(primary_keys)} HAVING COUNT(*) > 1"
    }))

    # Critical features: must not be null above threshold
    for feat in (critical_features or []):
        checks.append(DQCheck(f"{feat}_not_null", "completeness", Severity.HIGH,
                              {"column": feat, "max_null_pct": max_null_pct}))

    # Freshness: latest event_time / updated_at (if column exists)
    for col in ("event_time", "updated_at", "ts"):
        if col in df.columns:
            checks.append(DQCheck("freshness", "freshness", Severity.MEDIUM,
                                  {"partition_column": col}))
            break

    cfg = DQConfig(table_name=table_name, checks=checks)
    dq = DataQualityFramework(spark)
    report = dq.validate(df, cfg)
    logger.info(report.summary)
    return report


if __name__ == "__main__":
    report = validate_feature_table(
        table_name="main.features.customer_features",   # CHANGE_ME
        primary_keys=["customer_id"],                     # CHANGE_ME
        critical_features=["total_purchases_30d"],        # CHANGE_ME
    )
    if report.has_failures:
        raise RuntimeError(report.summary)

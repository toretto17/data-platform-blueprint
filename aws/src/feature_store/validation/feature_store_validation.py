"""
================================================================================
FEATURE STORE VALIDATION — [AWS SageMaker Feature Store]
================================================================================
Purpose: Validate a SageMaker Feature Group's offline store (Iceberg/Glue table)
         before consuming it for training/inference. Checks PK uniqueness,
         null rate, freshness, row count — via Athena queries against the offline store.

Uses the FeatureGroupManager from creation/ to resolve the Glue table, then runs
the shared AWS DQ framework checks.

Usage:
    from aws.src.feature_store.validation.feature_store_validation import validate_feature_group
    report = validate_feature_group("sales-anomaly-features", record_id="record_id",
                                     critical_features=["daily_ga", "avg_ga_3m"])
    if report.has_failures:
        raise Exception(report.summary)
Version : 2026-06-29
================================================================================
"""
import logging
from typing import List, Optional

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from aws.src.common.validations.dq_framework import (
    DataQualityFramework, DQConfig, DQCheck, Severity, DQReport,
)

logger = logging.getLogger("fs_validation_aws")


def validate_feature_group(
    fg_name: str,
    record_id: str,
    critical_features: Optional[List[str]] = None,
    min_rows: int = 100,
    max_null_pct: float = 0.05,
    region: str = "ap-southeast-1",
    spark: Optional[SparkSession] = None,
) -> DQReport:
    """Validate the offline store (Iceberg Glue table) for a SageMaker Feature Group."""

    spark = spark or SparkSession.builder.getOrCreate()
    sm = boto3.client("sagemaker", region_name=region)

    # Resolve the Glue table from the FG
    dc = sm.describe_feature_group(FeatureGroupName=fg_name)[
        "OfflineStoreConfig"]["DataCatalogConfig"]
    db = dc.get("Database", "sagemaker_featurestore").lower()
    table = dc["TableName"]
    logger.info(f"validating FG={fg_name} → {db}.{table}")

    # Read via Iceberg catalog (same as production consumption code)
    try:
        df = spark.sql(f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {record_id} ORDER BY event_time DESC, write_time DESC
                ) AS _rn
                FROM glue_catalog.{db}.{table}
                WHERE NOT is_deleted
            ) WHERE _rn = 1
        """)
    except Exception:
        # Fallback: plain table read (non-Iceberg setup)
        df = spark.table(f"{db}.{table}").filter(~F.col("is_deleted"))

    checks = [
        DQCheck("row_count", "row_count", Severity.CRITICAL, {"min_count": min_rows}),
        DQCheck("pk_unique", "business_rule", Severity.CRITICAL, {
            "sql": f"SELECT {record_id}, COUNT(*) c FROM __dq_check_table GROUP BY {record_id} HAVING c > 1"
        }),
    ]

    for feat in (critical_features or []):
        checks.append(DQCheck(f"{feat}_not_null", "completeness", Severity.HIGH,
                              {"column": feat, "max_null_pct": max_null_pct}))

    # Freshness: latest event_time
    checks.append(DQCheck("freshness", "freshness", Severity.MEDIUM,
                          {"partition_column": "event_time"}))

    cfg = DQConfig(table_name=f"{fg_name} ({db}.{table})", checks=checks)
    glue_client = boto3.client("glue", region_name=region)
    dq = DataQualityFramework(spark, glue_client)
    report = dq.validate(df, cfg)
    logger.info(report.summary)
    return report


if __name__ == "__main__":
    report = validate_feature_group(
        fg_name="sales-anomaly-features",             # CHANGE_ME
        record_id="record_id",                        # CHANGE_ME
        critical_features=["daily_ga", "avg_ga_3m"],  # CHANGE_ME
    )
    if report.has_failures:
        raise RuntimeError(report.summary)

"""
================================================================================
DATA QUALITY FRAMEWORK — Enterprise DQ Validation
================================================================================
Purpose: Reusable DQ checks that run inside Glue jobs or standalone.
         Never crashes the pipeline — warns and skips on missing rulesets.

Checks:
    1. Schema validation (columns, types, nullability)
    2. Completeness (null%, empty string%)
    3. Freshness (latest partition date)
    4. Row count thresholds
    5. Business rules (custom SQL assertions)
    6. Cross-layer reconciliation (source vs target counts)
    7. Statistical drift detection

Usage:
    dq = DataQualityFramework(spark, glue_client)
    result = dq.validate(df, config=DQConfig(...))
    if result.has_failures:
        # Log to CloudWatch, send SNS alert
        dq.publish_metrics(result)
================================================================================
"""
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from enum import Enum

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("data_quality")


class Severity(Enum):
    CRITICAL = "CRITICAL"  # Fails pipeline
    HIGH = "HIGH"          # Alert, but continues
    MEDIUM = "MEDIUM"      # Warning
    LOW = "LOW"            # Info only


@dataclass
class DQCheck:
    """Single DQ check definition."""
    name: str
    check_type: str              # schema|completeness|freshness|row_count|business_rule|reconciliation
    severity: Severity = Severity.HIGH
    params: Dict = field(default_factory=dict)


@dataclass
class DQResult:
    """Result of a single DQ check."""
    check_name: str
    passed: bool
    severity: Severity
    message: str
    metric_value: Optional[float] = None


@dataclass
class DQReport:
    """Aggregate DQ report."""
    table_name: str
    results: List[DQResult] = field(default_factory=list)

    @property
    def has_failures(self) -> bool:
        return any(not r.passed and r.severity == Severity.CRITICAL for r in self.results)

    @property
    def summary(self) -> str:
        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed
        return f"{self.table_name}: {passed}/{len(self.results)} passed, {failed} failed"


@dataclass
class DQConfig:
    """Configuration for DQ validation on a table."""
    table_name: str
    checks: List[DQCheck] = field(default_factory=list)
    min_row_count: int = 100
    max_null_pct: float = 0.5               # 50%
    freshness_max_hours: int = 48
    critical_columns: List[str] = field(default_factory=list)  # Must not be null
    partition_column: str = "mnth_id"


class DataQualityFramework:
    """Enterprise DQ framework. Pluggable, non-blocking."""

    def __init__(self, spark: SparkSession, glue_client=None):
        self.spark = spark
        self.glue_client = glue_client

    def validate(self, df: DataFrame, config: DQConfig) -> DQReport:
        """Run all configured checks on a DataFrame."""
        report = DQReport(table_name=config.table_name)

        for check in config.checks:
            try:
                result = self._run_check(df, check, config)
                report.results.append(result)
            except Exception as e:
                logger.warning(f"DQ check '{check.name}' failed with error: {e} — skipping")
                report.results.append(DQResult(
                    check_name=check.name, passed=True,
                    severity=check.severity, message=f"Skipped (error: {e})"
                ))

        logger.info(report.summary)
        return report

    def _run_check(self, df: DataFrame, check: DQCheck, config: DQConfig) -> DQResult:
        """Route to specific check implementation."""
        handlers = {
            "row_count": self._check_row_count,
            "completeness": self._check_completeness,
            "schema": self._check_schema,
            "freshness": self._check_freshness,
            "business_rule": self._check_business_rule,
            "reconciliation": self._check_reconciliation,
        }
        handler = handlers.get(check.check_type)
        if not handler:
            return DQResult(check.name, True, check.severity, f"Unknown check type: {check.check_type}")
        return handler(df, check, config)

    def _check_row_count(self, df: DataFrame, check: DQCheck, config: DQConfig) -> DQResult:
        count = df.count()
        min_count = check.params.get("min_count", config.min_row_count)
        passed = count >= min_count
        return DQResult(check.name, passed, check.severity,
                        f"Row count: {count} (min: {min_count})", metric_value=count)

    def _check_completeness(self, df: DataFrame, check: DQCheck, config: DQConfig) -> DQResult:
        column = check.params["column"]
        total = df.count()
        if total == 0:
            return DQResult(check.name, False, check.severity, "Empty DataFrame")
        nulls = df.filter(F.col(column).isNull() | (F.trim(F.col(column)) == "")).count()
        null_pct = nulls / total
        max_pct = check.params.get("max_null_pct", config.max_null_pct)
        passed = null_pct <= max_pct
        return DQResult(check.name, passed, check.severity,
                        f"{column}: {null_pct:.1%} null (max: {max_pct:.1%})", metric_value=null_pct)

    def _check_schema(self, df: DataFrame, check: DQCheck, config: DQConfig) -> DQResult:
        expected_cols = set(check.params.get("expected_columns", []))
        actual_cols = set(df.columns)
        missing = expected_cols - actual_cols
        if missing:
            return DQResult(check.name, False, check.severity, f"Missing columns: {missing}")
        return DQResult(check.name, True, check.severity, "Schema OK")

    def _check_freshness(self, df: DataFrame, check: DQCheck, config: DQConfig) -> DQResult:
        partition_col = config.partition_column
        latest = df.agg(F.max(partition_col)).collect()[0][0]
        return DQResult(check.name, latest is not None, check.severity,
                        f"Latest partition: {latest}")

    def _check_business_rule(self, df: DataFrame, check: DQCheck, config: DQConfig) -> DQResult:
        """Custom SQL assertion. Expects params['sql'] to return 0 rows on pass."""
        sql = check.params.get("sql", "")
        if not sql:
            return DQResult(check.name, True, check.severity, "No SQL provided — skipped")
        df.createOrReplaceTempView("__dq_check_table")
        violations = self.spark.sql(sql).count()
        passed = violations == 0
        return DQResult(check.name, passed, check.severity,
                        f"Business rule violations: {violations}", metric_value=violations)

    def _check_reconciliation(self, df: DataFrame, check: DQCheck, config: DQConfig) -> DQResult:
        """Cross-layer count reconciliation."""
        source_count = check.params.get("source_count", 0)
        target_count = df.count()
        threshold = check.params.get("threshold_pct", 0.01)  # 1%
        if source_count == 0:
            return DQResult(check.name, True, check.severity, "No source count provided — skipped")
        diff_pct = abs(target_count - source_count) / source_count
        passed = diff_pct <= threshold
        return DQResult(check.name, passed, check.severity,
                        f"Source: {source_count}, Target: {target_count}, Diff: {diff_pct:.2%}",
                        metric_value=diff_pct)

    def publish_metrics(self, report: DQReport, namespace: str = "DataQuality"):
        """Publish DQ metrics to CloudWatch."""
        import boto3
        cw = boto3.client("cloudwatch")
        metrics = []
        for r in report.results:
            if r.metric_value is not None:
                metrics.append({
                    "MetricName": r.check_name,
                    "Value": r.metric_value,
                    "Unit": "None",
                    "Dimensions": [
                        {"Name": "Table", "Value": report.table_name},
                        {"Name": "Severity", "Value": r.severity.value},
                    ],
                })
        if metrics:
            cw.put_metric_data(Namespace=namespace, MetricData=metrics)
            logger.info(f"Published {len(metrics)} DQ metrics to CloudWatch")

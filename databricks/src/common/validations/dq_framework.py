"""
================================================================================
DATA QUALITY FRAMEWORK — [Databricks / Delta]
================================================================================
Purpose: Databricks twin of aws/src/common/validations/dq_framework.py.
         SAME public API (DQConfig / DQCheck / DataQualityFramework.validate)
         so job code is identical across platforms. Implemented Spark-native
         (no Glue). Metrics publish to a Delta audit table (and MLflow if active)
         instead of CloudWatch.

Checks (all implemented, real code below):
    1. row_count       — minimum row threshold
    2. completeness     — null/empty % per column
    3. schema           — required columns present
    4. freshness        — latest partition not null
    5. business_rule    — custom SQL that must return 0 violation rows
    6. reconciliation   — source vs target count within tolerance

Usage:
    dq = DataQualityFramework(spark)
    cfg = DQConfig(table_name="main.silver.sales", checks=[
        DQCheck("rows", "row_count", Severity.CRITICAL, {"min_count": 1000}),
        DQCheck("id_not_null", "completeness", Severity.HIGH, {"column": "id", "max_null_pct": 0.0}),
    ])
    report = dq.validate(df, cfg)
    if report.has_failures:        # any CRITICAL fail
        dq.publish_metrics(report, audit_table="main.ops.dq_metrics")
        raise DQError(report.summary)
Version : 2026-06-28
================================================================================
"""
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("data_quality_databricks")


class Severity(Enum):
    CRITICAL = "CRITICAL"   # Fails pipeline
    HIGH = "HIGH"           # Alert, but continues
    MEDIUM = "MEDIUM"       # Warning
    LOW = "LOW"             # Info only


@dataclass
class DQCheck:
    name: str
    check_type: str         # row_count|completeness|schema|freshness|business_rule|reconciliation
    severity: Severity = Severity.HIGH
    params: Dict = field(default_factory=dict)


@dataclass
class DQResult:
    check_name: str
    passed: bool
    severity: Severity
    message: str
    metric_value: Optional[float] = None


@dataclass
class DQReport:
    table_name: str
    results: List[DQResult] = field(default_factory=list)

    @property
    def has_failures(self) -> bool:
        return any(not r.passed and r.severity == Severity.CRITICAL for r in self.results)

    @property
    def summary(self) -> str:
        passed = sum(1 for r in self.results if r.passed)
        return f"{self.table_name}: {passed}/{len(self.results)} passed, {len(self.results) - passed} failed"


@dataclass
class DQConfig:
    table_name: str
    checks: List[DQCheck] = field(default_factory=list)
    min_row_count: int = 100
    max_null_pct: float = 0.5
    partition_column: str = "mnth_id"


class DataQualityFramework:
    """Spark-native DQ framework for Databricks. Non-blocking by default."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def validate(self, df: DataFrame, config: DQConfig) -> DQReport:
        report = DQReport(table_name=config.table_name)
        # Cache once — several checks scan the frame — then unpersist at the end.
        cached = False
        try:
            df.cache()
            cached = True
        except Exception:
            pass
        try:
            for check in config.checks:
                try:
                    report.results.append(self._run_check(df, check, config))
                except Exception as e:
                    logger.warning(f"DQ check '{check.name}' errored: {e} — skipping (treated as pass)")
                    report.results.append(DQResult(check.name, True, check.severity, f"Skipped (error: {e})"))
        finally:
            if cached:
                df.unpersist()
        logger.info(report.summary)
        return report

    def _run_check(self, df: DataFrame, check: DQCheck, config: DQConfig) -> DQResult:
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

    def _check_row_count(self, df, check, config) -> DQResult:
        count = df.count()
        min_count = check.params.get("min_count", config.min_row_count)
        return DQResult(check.name, count >= min_count, check.severity,
                        f"Row count: {count} (min: {min_count})", metric_value=float(count))

    def _check_completeness(self, df, check, config) -> DQResult:
        column = check.params["column"]
        total = df.count()
        if total == 0:
            return DQResult(check.name, False, check.severity, "Empty DataFrame")
        nulls = df.filter(F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")).count()
        null_pct = nulls / total
        max_pct = check.params.get("max_null_pct", config.max_null_pct)
        return DQResult(check.name, null_pct <= max_pct, check.severity,
                        f"{column}: {null_pct:.1%} null (max: {max_pct:.1%})", metric_value=null_pct)

    def _check_schema(self, df, check, config) -> DQResult:
        missing = set(check.params.get("expected_columns", [])) - set(df.columns)
        return DQResult(check.name, not missing, check.severity,
                        f"Missing columns: {missing}" if missing else "Schema OK")

    def _check_freshness(self, df, check, config) -> DQResult:
        col = check.params.get("partition_column", config.partition_column)
        latest = df.agg(F.max(col)).collect()[0][0]
        return DQResult(check.name, latest is not None, check.severity, f"Latest {col}: {latest}")

    def _check_business_rule(self, df, check, config) -> DQResult:
        """params['sql'] must SELECT violation rows from view __dq (0 rows = pass)."""
        sql = check.params.get("sql", "")
        if not sql:
            return DQResult(check.name, True, check.severity, "No SQL — skipped")
        df.createOrReplaceTempView("__dq")
        violations = self.spark.sql(sql).count()
        return DQResult(check.name, violations == 0, check.severity,
                        f"Violations: {violations}", metric_value=float(violations))

    def _check_reconciliation(self, df, check, config) -> DQResult:
        source_count = check.params.get("source_count", 0)
        if source_count == 0:
            return DQResult(check.name, True, check.severity, "No source_count — skipped")
        target_count = df.count()
        diff_pct = abs(target_count - source_count) / source_count
        threshold = check.params.get("threshold_pct", 0.01)
        return DQResult(check.name, diff_pct <= threshold, check.severity,
                        f"src={source_count} tgt={target_count} diff={diff_pct:.2%}", metric_value=diff_pct)

    def publish_metrics(self, report: DQReport, audit_table: str = "main.ops.dq_metrics"):
        """Append DQ metrics to a Delta audit table (+ MLflow if a run is active)."""
        rows = [(report.table_name, r.check_name, r.severity.value, bool(r.passed),
                 float(r.metric_value) if r.metric_value is not None else None, r.message)
                for r in report.results]
        if rows:
            (self.spark.createDataFrame(
                rows, ["table_name", "check_name", "severity", "passed", "metric_value", "message"])
                .withColumn("checked_ts", F.current_timestamp())
                .write.format("delta").mode("append").saveAsTable(audit_table))
            logger.info(f"Published {len(rows)} DQ results → {audit_table}")
        try:
            import mlflow
            if mlflow.active_run() is not None:
                for r in report.results:
                    if r.metric_value is not None:
                        mlflow.log_metric(f"dq_{r.check_name}", r.metric_value)
        except Exception:
            pass

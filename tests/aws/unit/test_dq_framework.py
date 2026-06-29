"""Test the DQ framework checks."""
import pytest
from pyspark.sql import functions as F


def test_row_count_passes(spark, sample_df):
    """DQ row_count check should pass when df has rows."""
    from aws.src.common.validations.dq_framework import DataQualityFramework, DQConfig, DQCheck, Severity
    dq = DataQualityFramework(spark)
    cfg = DQConfig(table_name="test", checks=[
        DQCheck("rows", "row_count", Severity.CRITICAL, {"min_count": 1})])
    report = dq.validate(sample_df, cfg)
    assert not report.has_failures


def test_row_count_fails_on_empty(spark):
    """DQ row_count check should fail on empty df."""
    from aws.src.common.validations.dq_framework import DataQualityFramework, DQConfig, DQCheck, Severity
    empty = spark.createDataFrame([], "id INT, val DOUBLE")
    dq = DataQualityFramework(spark)
    cfg = DQConfig(table_name="test", checks=[
        DQCheck("rows", "row_count", Severity.CRITICAL, {"min_count": 1})])
    report = dq.validate(empty, cfg)
    assert report.has_failures


def test_completeness_check(spark, sample_df):
    """Completeness check should pass when column has no nulls."""
    from aws.src.common.validations.dq_framework import DataQualityFramework, DQConfig, DQCheck, Severity
    dq = DataQualityFramework(spark)
    cfg = DQConfig(table_name="test", checks=[
        DQCheck("pk", "completeness", Severity.HIGH, {"column": "item_id", "max_null_pct": 0.0})])
    report = dq.validate(sample_df, cfg)
    assert not report.has_failures

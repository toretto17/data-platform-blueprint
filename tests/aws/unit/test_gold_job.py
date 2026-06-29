"""
Unit tests template for ETL jobs.
Uses moto for AWS mocking, pytest fixtures for Spark sessions.
"""
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests."""
    return (SparkSession.builder
            .master("local[2]")
            .appName("unit-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())


@pytest.fixture
def sample_silver_df(spark):
    """Sample Silver-layer DataFrame for testing Gold logic."""
    data = [
        ("SITE001", 20260615, 202606, "TKT001", 45.0),
        ("SITE001", 20260615, 202606, "TKT002", 30.0),
        ("SITE002", 20260615, 202606, "TKT003", 60.0),
        ("SITE001", 20260616, 202606, "TKT004", 15.0),
    ]
    return spark.createDataFrame(data, ["site_code", "data_dt", "mnth_id", "ticketid", "down_time"])


@pytest.fixture
def sample_dimension_df(spark):
    """Sample dimension DataFrame (active sites)."""
    return spark.createDataFrame(
        [("SITE001",), ("SITE002",), ("SITE003",)],
        ["site_code"]
    )


class TestGoldAggregation:
    """Test Gold-layer aggregation logic."""

    def test_daily_aggregate_counts(self, spark, sample_silver_df):
        """Verify COUNT(DISTINCT ticketid) per site per day."""
        result = (sample_silver_df
                  .groupBy("site_code", "data_dt", "mnth_id")
                  .agg(F.countDistinct("ticketid").alias("daily_closed_tickets")))

        site1_day1 = result.filter(
            (F.col("site_code") == "SITE001") & (F.col("data_dt") == 20260615)
        ).collect()[0]["daily_closed_tickets"]

        assert site1_day1 == 2  # 2 tickets for SITE001 on June 15

    def test_zero_fill_adds_missing_sites(self, spark, sample_silver_df, sample_dimension_df):
        """Verify zero-fill creates rows for sites with no tickets."""
        # Aggregate
        fact = (sample_silver_df
                .groupBy("site_code", "data_dt", "mnth_id")
                .agg(F.countDistinct("ticketid").alias("daily_closed_tickets")))

        # Zero-fill
        dates = fact.select("data_dt", "mnth_id").distinct()
        skeleton = sample_dimension_df.crossJoin(dates)
        result = skeleton.join(fact, on=["site_code", "data_dt"], how="left").fillna(0)

        # SITE003 should exist with 0 tickets
        site3 = result.filter(F.col("site_code") == "SITE003").collect()
        assert len(site3) > 0
        assert all(row["daily_closed_tickets"] == 0 for row in site3)

    def test_dimension_filter_excludes_inactive(self, spark, sample_silver_df):
        """Verify inner join with dimension drops inactive sites."""
        # Only SITE001 is "active"
        active_sites = spark.createDataFrame([("SITE001",)], ["site_code"])

        filtered = sample_silver_df.join(active_sites, on="site_code", how="inner")
        sites = [r["site_code"] for r in filtered.select("site_code").distinct().collect()]

        assert "SITE001" in sites
        assert "SITE002" not in sites


class TestDataQuality:
    """Test DQ framework."""

    def test_completeness_check_passes(self, spark, sample_silver_df):
        """Non-null column should pass completeness check."""
        from src.common.validations.dq_framework import DataQualityFramework, DQConfig, DQCheck, Severity

        dq = DataQualityFramework(spark)
        config = DQConfig(
            table_name="test_table",
            checks=[DQCheck("site_code_completeness", "completeness", Severity.HIGH, {"column": "site_code", "max_null_pct": 0.1})]
        )
        report = dq.validate(sample_silver_df, config)
        assert report.results[0].passed

    def test_row_count_check_fails_on_empty(self, spark):
        """Empty DataFrame should fail row count check."""
        from src.common.validations.dq_framework import DataQualityFramework, DQConfig, DQCheck, Severity

        empty_df = spark.createDataFrame([], "site_code STRING, data_dt INT")
        dq = DataQualityFramework(spark)
        config = DQConfig(
            table_name="test_table",
            checks=[DQCheck("row_count", "row_count", Severity.CRITICAL, {"min_count": 10})]
        )
        report = dq.validate(empty_df, config)
        assert not report.results[0].passed

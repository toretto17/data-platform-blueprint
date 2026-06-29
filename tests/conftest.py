"""Shared pytest fixtures for all tests."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for unit tests (reused across all tests)."""
    return (SparkSession.builder
            .master("local[2]")
            .appName("unit-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
            .getOrCreate())


@pytest.fixture
def sample_df(spark):
    """Generic sample DataFrame for testing."""
    data = [
        ("ITEM001", 20260601, 202606, 10.0),
        ("ITEM001", 20260602, 202606, 15.0),
        ("ITEM002", 20260601, 202606, 20.0),
    ]
    return spark.createDataFrame(data, ["item_id", "tm_key_day", "mnth_id", "daily_ga"])

"""Test Databricks etl_utils (EarlyExitCheck, DeltaWriter)."""
import pytest


def test_early_exit_empty(spark):
    """EarlyExitCheck.is_empty should return True for empty df."""
    from databricks.src.common.utils.etl_utils import EarlyExitCheck
    empty = spark.createDataFrame([], "id INT")
    assert EarlyExitCheck.is_empty(empty)


def test_early_exit_nonempty(spark, sample_df):
    """EarlyExitCheck.is_empty should return False for non-empty df."""
    from databricks.src.common.utils.etl_utils import EarlyExitCheck
    assert not EarlyExitCheck.is_empty(sample_df)

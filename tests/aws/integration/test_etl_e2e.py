"""
Integration test skeleton: end-to-end Silver→Gold pipeline.
Requires a local Spark session + sample data (see conftest.py).
Run with: pytest tests/aws/integration/ -v
"""
import pytest


@pytest.mark.integration
def test_silver_to_gold_e2e(spark, sample_df, tmp_path):
    """Smoke test: run silver transform → verify output schema + row count."""
    # CHANGE_ME: instantiate your Silver job with sample source, write to tmp_path
    # from aws.src.silver.jobs.silver_job import SilverSalesJob
    # job = SilverSalesJob(...)
    # job.run()
    # result = spark.read.parquet(str(tmp_path / "output"))
    # assert result.count() > 0
    # assert "mnth_id" in result.columns
    pass  # TODO: fill with your actual job test

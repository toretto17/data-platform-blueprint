"""
================================================================================
CUSTOM METRICS PUBLISHER — [AWS CloudWatch / Databricks MLflow]
================================================================================
Purpose: Publish custom pipeline metrics (rows processed, duration, DQ scores)
         to CloudWatch (AWS) or MLflow (Databricks) for dashboarding + alerting.

Usage:
    from monitoring.metrics.custom_metrics import publish_metric
    publish_metric("rows_processed", 12345, dimensions={"table": "silver_sales"})
================================================================================
"""
import os, logging, boto3

logger = logging.getLogger("custom_metrics")
NAMESPACE = os.environ.get("CW_NAMESPACE", "DataPlatform/ETL")
REGION = os.environ.get("REGION", "ap-southeast-1")


def publish_metric(name: str, value: float, dimensions: dict = None, unit: str = "None"):
    """Publish one metric to CloudWatch."""
    dims = [{"Name": k, "Value": str(v)} for k, v in (dimensions or {}).items()]
    boto3.client("cloudwatch", region_name=REGION).put_metric_data(
        Namespace=NAMESPACE, MetricData=[{"MetricName": name, "Value": value, "Unit": unit,
                                          "Dimensions": dims}])
    logger.info(f"metric: {name}={value} {dims}")


def publish_pipeline_summary(job_name: str, rows_in: int, rows_out: int, duration_sec: float, status: str):
    """Convenience: publish a standard set of pipeline metrics."""
    dims = {"JobName": job_name}
    publish_metric("rows_in", rows_in, dims)
    publish_metric("rows_out", rows_out, dims)
    publish_metric("duration_seconds", duration_sec, dims, unit="Seconds")
    publish_metric("success", 1 if status == "SUCCESS" else 0, dims)

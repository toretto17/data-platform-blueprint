"""
================================================================================
MODEL MONITORING + DRIFT — [AWS SageMaker Model Monitor + Custom Drift]
================================================================================
Purpose: Monitor a deployed model for data drift and model quality degradation.
    A) SageMaker Model Monitor (managed): DataQuality + ModelQuality schedules
    B) Custom drift (PSI/KS): same portable math as Databricks twin, logs to S3/CW

Pattern (from production):
    - Model Monitor activated when a package is Approved (via monitoring DDB config)
    - DataQuality: compares input data stats against a baseline (auto CSV schema)
    - Custom drift: compares prediction distributions (SMAPE, PSI) per schedule

This template provides:
    1. DDB monitoring config builder (the contract our Lambda reads)
    2. Manual PSI/KS drift computation (portable)
    3. CloudWatch metric publishing for alerts

Customize: MODEL_PACKAGE_GROUP, BUCKET, ALERT_EMAILS, THRESHOLDS.
Databricks twin: databricks/src/mlops/monitoring/monitoring.py
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Dict, List, Optional
from decimal import Decimal

import numpy as np
import boto3

logger = logging.getLogger("monitoring_aws")


# ===========================================================================
# A) SageMaker Model Monitor config (DDB item — the monitoring contract)
# ===========================================================================
def build_monitoring_config(
    model_package_group: str,
    bucket: str,
    alert_emails: List[str],
    monitoring_types: Optional[List[str]] = None,
    schedule_cron: str = "cron(0 * ? * * *)",
    dataset_header: bool = False,
    smape_threshold: float = 20.0,
    instance_type: str = "ml.m5.xlarge",
) -> dict:
    """Build the DDB item that the deployed `setup-model` Lambda reads when a
    package is Approved. Write this to the `mlops_model_monitoring_config` table.
    (Exact schema from production MODEL_MONITORING_CONTRACT §2)."""
    if not alert_emails:
        raise ValueError("alert_emails required")
    return {
        "model_package_group": model_package_group,
        "monitoring_types": monitoring_types or ["data_quality"],
        "schedule_cron": schedule_cron,
        "dataset_header": dataset_header,
        "smape_threshold": Decimal(str(smape_threshold)),
        "bucket": bucket,
        "alert_emails": alert_emails,
        "instance_count": 1,
        "instance_type": instance_type,
        "volume_size_in_gb": 20,
        "max_runtime_in_seconds": 1800,
    }


# ===========================================================================
# B) Manual PSI / KS drift (portable — same as Databricks twin)
# ===========================================================================
class ManualDriftDetector:
    PSI_THRESHOLD: float = 0.2
    KS_THRESHOLD: float = 0.05

    @staticmethod
    def psi(baseline: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
        eps = 1e-6
        bp = np.histogram_bin_edges(baseline, bins=bins)
        b_pct = np.histogram(baseline, bins=bp)[0] / len(baseline) + eps
        c_pct = np.histogram(current, bins=bp)[0] / len(current) + eps
        return float(np.sum((c_pct - b_pct) * np.log(c_pct / b_pct)))

    @staticmethod
    def ks_test(baseline: np.ndarray, current: np.ndarray) -> float:
        from scipy.stats import ks_2samp
        _, p = ks_2samp(baseline, current)
        return float(p)

    def detect(self, baseline: dict, current: dict, features: List[str]) -> Dict[str, dict]:
        """baseline/current = {feature: np.array}. Returns per-feature drift report."""
        results = {}
        for f in features:
            b, c = baseline.get(f, np.array([])), current.get(f, np.array([]))
            if len(b) == 0 or len(c) == 0:
                results[f] = {"psi": 0, "ks_p": 1, "drifted": False}
                continue
            p = self.psi(b, c)
            ks = self.ks_test(b, c)
            drifted = p > self.PSI_THRESHOLD or ks < self.KS_THRESHOLD
            results[f] = {"psi": round(p, 4), "ks_p": round(ks, 4), "drifted": drifted}
        return results


# ===========================================================================
# C) CloudWatch alert publishing
# ===========================================================================
def publish_drift_metrics(results: Dict[str, dict], namespace: str = "MLOps/Drift",
                          region: str = "ap-southeast-1"):
    """Push drift metrics to CloudWatch (for alarm → SNS → PagerDuty/Slack)."""
    cw = boto3.client("cloudwatch", region_name=region)
    metrics = []
    for feat, vals in results.items():
        metrics.append({"MetricName": f"PSI_{feat}", "Value": vals["psi"], "Unit": "None"})
    if metrics:
        cw.put_metric_data(Namespace=namespace, MetricData=metrics[:20])  # CW batch limit
        logger.info(f"published {len(metrics)} drift metrics → {namespace}")


if __name__ == "__main__":
    # Example: write monitoring config to DDB (one-time setup per model group)
    # config = build_monitoring_config("ChurnModelGroup", "my-bucket", ["ops@co.com"],
    #                                   monitoring_types=["data_quality", "custom_drift"])
    # boto3.resource("dynamodb").Table("mlops_model_monitoring_config").put_item(Item=config)

    # Example: manual drift check
    detector = ManualDriftDetector()
    import numpy as np
    baseline = {"feat1": np.random.normal(0, 1, 1000)}
    current = {"feat1": np.random.normal(0.5, 1, 1000)}  # shifted!
    results = detector.detect(baseline, current, ["feat1"])
    publish_drift_metrics(results)
    print(results)

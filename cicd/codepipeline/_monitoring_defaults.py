"""
Monitoring Defaults — Fallback DDB Row Builder for Model Promotion
==================================================================

PURPOSE:
    When promoting a model to prod, the deploy pipeline mirrors the nonprod monitoring
    config (DynamoDB row) to prod. If that nonprod row doesn't exist (brand-new model,
    accidentally deleted, etc.), this module builds a SAFE DEFAULT row so monitoring
    can still bootstrap.

WHEN IS THIS USED?
    Only on the DEGENERATE PATH:
      - 99% of the time: nonprod DDB row exists → build.py mirrors it → this module is NOT called
      - 1% fallback: nonprod row missing → this module generates defaults → prod monitoring works

WHAT IT PROVIDES:
    - Monitoring schedule (cron expression)
    - Alert email addresses
    - Monitoring types (data quality, model quality, bias, explainability)
    - Job sizing (instance type, volume size)
    - Threshold defaults (metric-specific)
    - Dataset header (column names for data capture)
    - Baseline URIs (predictions, test data, training data — from model package metadata)

HOW TO CONFIGURE:
    1. Define your model family naming patterns in _parse_group()
    2. Set default thresholds in DEFAULT_THRESHOLDS
    3. Set monitoring schedule in DEFAULT_SCHEDULE
    4. Set alert emails in DEFAULT_ALERT_EMAILS
    5. Define dataset headers (column names) per model type in DEFAULT_HEADERS
    6. Configure instance sizing in DEFAULT_INSTANCE

INTEGRATION:
    Called by build.py → write_monitoring_ddb_row() when get_item() returns empty.
    The output dict is written directly to the monitoring DynamoDB table.

SCHEMA:
    The DDB row schema must match what your monitoring Step Function expects.
    See your monitoring SF definition for the exact field names.
"""
from __future__ import annotations

import json
import logging
import re
from decimal import Decimal
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION — CHANGE THESE TO MATCH YOUR PROJECT
# ═══════════════════════════════════════════════════════════════════════════════

# Default monitoring schedule (cron format for SageMaker Monitoring Schedule)
# CHANGE: Set your desired monitoring frequency
DEFAULT_SCHEDULE = "cron(0 2 * * ? *)"  # Daily at 02:00 UTC

# Default alert recipients when monitoring detects drift/violations
# CHANGE: Your team's alert email(s)
DEFAULT_ALERT_EMAILS = ["ml-alerts@example.com"]  # CHANGE_ME

# Default metric threshold (model-quality gate)
# CHANGE: Set based on your model's acceptable performance range
# For regression: could be RMSE, MAE, MAPE
# For classification: could be F1, AUC, accuracy
DEFAULT_THRESHOLDS = {
    "default": Decimal("0.15"),       # 15% — generic fallback
    "forecast_ga": Decimal("0.12"),   # CHANGE: your GA forecast threshold
    "forecast_m1": Decimal("0.18"),   # CHANGE: your M1 forecast threshold
    "anomaly": Decimal("0.10"),       # CHANGE: your anomaly threshold
}

# Default instance type for monitoring jobs
# CHANGE: Size based on your data volume per monitoring run
DEFAULT_INSTANCE = {
    "instance_type": "ml.m5.xlarge",
    "volume_size_gb": 30,
}

# Default monitoring types to enable
# Options: DataQuality, ModelQuality, ModelBias, ModelExplainability
DEFAULT_MONITORING_TYPES = ["DataQuality", "ModelQuality"]

# Default dataset header (column names in the data capture CSV)
# CHANGE: Must match your model's input/output schema exactly
DEFAULT_HEADERS = {
    "forecast": "item_id,timestamp,target,prediction,lower,upper",  # CHANGE_ME
    "anomaly": "item_id,timestamp,score,percentile,flag",           # CHANGE_ME
}

# Label column name (for ModelQuality monitoring)
LABEL_COLUMN = "target"  # CHANGE_ME


# ═══════════════════════════════════════════════════════════════════════════════
# MODEL GROUP NAME PARSING — CHANGE PATTERNS TO MATCH YOUR NAMING CONVENTION
# ═══════════════════════════════════════════════════════════════════════════════

# CHANGE: These regex patterns must match YOUR ModelPackageGroup naming convention.
# Production example: "SalesForecastTolGAPackageGroup" → (forecast, tol, ga)
# Your naming might be: "MyProject-ProductA-MetricX" → adjust regex accordingly.

_FORECAST_RE = re.compile(
    r"^(?:CHANGE_ME)Forecast([\w]+?)(GA|M1|[\w]+)PackageGroup$", re.IGNORECASE
)
_ANOMALY_RE = re.compile(
    r"^(?:CHANGE_ME)Anomaly([\w]+?)(GA|M1|[\w]+)PackageGroup$", re.IGNORECASE
)


def _parse_group(group_name: str) -> Optional[tuple[str, str, str]]:
    """
    Parse a ModelPackageGroup name into (model_type, product, target).

    CHANGE: Update the regex patterns above and this function to match
    YOUR group naming convention.

    Returns:
        tuple: (model_type, product, target) e.g. ("forecast", "tol", "ga")
        None: if the group name doesn't match any known pattern

    Examples (adjust to your naming):
        "SalesForecastTolGAPackageGroup"  → ("forecast", "tol", "ga")
        "TolPerTierGAPackageGroup"        → ("anomaly", "tol", "ga")
        "MyModelProductAMetricX"          → ("forecast", "producta", "metricx")
    """
    # Try forecast pattern
    m = _FORECAST_RE.match(group_name)
    if m:
        product = m.group(1).lower()
        target = m.group(2).lower()
        return ("forecast", product, target)

    # Try anomaly pattern
    m = _ANOMALY_RE.match(group_name)
    if m:
        product = m.group(1).lower()
        target = m.group(2).lower()
        return ("anomaly", product, target)

    logger.warning("Could not parse group name pattern: %s", group_name)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# BASELINE URI LOOKUP — Reads from Model Package CustomerMetadataProperties
# ═══════════════════════════════════════════════════════════════════════════════

def _lookup_baseline_uris(
    group_name: str,
    model_type: str,
    target: str,
    sm_client,
) -> dict:
    """
    Attempt to read baseline URIs from the latest model package's
    CustomerMetadataProperties (CMP).

    WHY: The training/register step writes these URIs to the model package
    so the monitoring setup knows where to find baseline data.

    Returns dict with available keys:
        - train_raw_wo_label_uri: S3 path to training data (without label column)
        - predictions_uri: S3 path to baseline predictions
        - test_data_uri: S3 path to test/validation data
        - threshold: metric threshold from evaluation.json

    Returns empty dict on failure (best-effort, never raises).
    """
    try:
        # Find latest approved package
        response = sm_client.list_model_packages(
            ModelPackageGroupName=group_name,
            ModelApprovalStatus="Approved",
            SortBy="CreationTime",
            SortOrder="Descending",
            MaxResults=1,
        )
        packages = response.get("ModelPackageSummaryList", [])
        if not packages:
            return {}

        # Read CustomerMetadataProperties
        pkg_arn = packages[0]["ModelPackageArn"]
        pkg_detail = sm_client.describe_model_package(ModelPackageName=pkg_arn)
        cmp = pkg_detail.get("CustomerMetadataProperties", {})

        # Extract URI fields
        out = {}
        for key in ("train_raw_wo_label_uri", "predictions_uri", "test_data_uri"):
            if key in cmp:
                out[key] = cmp[key]

        # Try to read threshold from evaluation.json
        eval_uri = cmp.get("evaluation_uri")
        if eval_uri:
            try:
                s3 = boto3.client("s3")
                bucket = eval_uri.replace("s3://", "").split("/")[0]
                key = "/".join(eval_uri.replace("s3://", "").split("/")[1:])
                obj = s3.get_object(Bucket=bucket, Key=key)
                eval_data = json.loads(obj["Body"].read())
                # CHANGE: Your evaluation.json field name for the threshold
                threshold_key = f"{target}_threshold"
                if threshold_key in eval_data:
                    out["threshold"] = Decimal(str(eval_data[threshold_key]))
            except Exception as e:
                logger.debug("Could not read evaluation.json: %s", e)

        return out

    except Exception as e:
        logger.warning("Baseline URI lookup failed for %s: %s", group_name, e)
        return {}


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN FUNCTION — Builds the default monitoring DDB row
# ═══════════════════════════════════════════════════════════════════════════════

def build_default_monitoring_row(
    group_name: str,
    sm_client=None,
    environment: str = "prod",
) -> Optional[dict]:
    """
    Build a default monitoring DynamoDB row for a model package group.

    Called by build.py when the nonprod monitoring row doesn't exist.
    Returns a dict ready to be written to the monitoring DDB table.

    Args:
        group_name: ModelPackageGroup name (e.g., "SalesForecastTolGAPackageGroup")
        sm_client: boto3 SageMaker client (optional, created if not provided)
        environment: Target environment ("prod" or "nonprod")

    Returns:
        dict: Complete monitoring DDB row, or None if group pattern unknown

    CHANGE: Adjust the output dict keys to match YOUR monitoring DDB table schema.
    """
    parsed = _parse_group(group_name)
    if not parsed:
        logger.error(
            "Unknown group name pattern: %s — cannot build default monitoring row. "
            "Either register the model first (register.py) or add the pattern to "
            "_monitoring_defaults.py::_parse_group().",
            group_name,
        )
        return None

    model_type, product, target = parsed
    logger.info(
        "Building default monitoring row for %s (type=%s, product=%s, target=%s)",
        group_name, model_type, product, target,
    )

    # Lookup baseline URIs from model package CMP (best-effort)
    if sm_client is None:
        sm_client = boto3.client("sagemaker")
    uri_overrides = _lookup_baseline_uris(group_name, model_type, target, sm_client)

    # Determine threshold
    threshold_key = f"{model_type}_{target}" if f"{model_type}_{target}" in DEFAULT_THRESHOLDS else "default"
    threshold = uri_overrides.get("threshold", DEFAULT_THRESHOLDS[threshold_key])

    # Build the row
    # CHANGE: These field names must match your monitoring DDB table schema exactly.
    row = {
        "model_package_group": group_name,                       # Partition key
        "environment": environment,
        "model_type": model_type,
        "product": product,
        "target": target,
        "monitoring_types": DEFAULT_MONITORING_TYPES,
        "schedule": DEFAULT_SCHEDULE,
        "alert_emails": DEFAULT_ALERT_EMAILS,
        "instance_type": DEFAULT_INSTANCE["instance_type"],
        "volume_size_gb": DEFAULT_INSTANCE["volume_size_gb"],
        "threshold": threshold,
        "dataset_header": DEFAULT_HEADERS.get(model_type, ""),
        "label_column": LABEL_COLUMN if model_type == "forecast" else "",
    }

    # Add baseline URIs if available
    for key in ("train_raw_wo_label_uri", "predictions_uri", "test_data_uri"):
        if key in uri_overrides:
            row[key] = uri_overrides[key]

    logger.info(
        "Default monitoring row built for %s (threshold=%s, schedule=%s, URIs=%s)",
        group_name,
        threshold,
        DEFAULT_SCHEDULE,
        list(uri_overrides.keys()) or "none (will use register.py later)",
    )

    return row

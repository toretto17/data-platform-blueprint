"""
================================================================================
MODEL REGISTRY + PROMOTION — [AWS SageMaker Model Registry]
================================================================================
Purpose: Register a model package in a SageMaker Model Package Group, approve it
         (triggers monitoring setup), and retrieve the latest approved model.

Pattern (from our production code):
    - One group per model (e.g. "TOLPerTierGAPackageGroup")
    - Register with CustomerMetadataProperties (carries monitoring contract)
    - Approval triggers the drift Step Function (via EventBridge)
    - get_latest_approved_model() used at inference time

Customize: MODEL_PACKAGE_GROUP, IMAGE_URI, ROLE_ARN.
Databricks twin: databricks/src/mlops/registry/registry.py (UC + MLflow aliases).
Version : 2026-06-29
================================================================================
"""
import json
import logging
from typing import Optional

import boto3

logger = logging.getLogger("registry_aws")


class ModelRegistryAWS:
    MODEL_PACKAGE_GROUP: str = "CHANGE_ME_ModelGroup"
    REGION: str = "ap-southeast-1"

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        self.sm = boto3.client("sagemaker", region_name=self.REGION)

    def ensure_group(self, description: str = ""):
        """Create the Model Package Group if it doesn't exist (idempotent)."""
        try:
            self.sm.describe_model_package_group(ModelPackageGroupName=self.MODEL_PACKAGE_GROUP)
        except self.sm.exceptions.ClientError:
            self.sm.create_model_package_group(
                ModelPackageGroupName=self.MODEL_PACKAGE_GROUP,
                ModelPackageGroupDescription=description or self.MODEL_PACKAGE_GROUP)
            logger.info(f"created group: {self.MODEL_PACKAGE_GROUP}")

    def register(self, model_data_url: str, image_uri: str,
                 approval_status: str = "PendingManualApproval",
                 metrics: Optional[dict] = None,
                 customer_metadata: Optional[dict] = None) -> str:
        """Register a new model version (package) in the group. Returns package ARN."""
        self.ensure_group()
        kwargs = dict(
            ModelPackageGroupName=self.MODEL_PACKAGE_GROUP,
            InferenceSpecification={
                "Containers": [{"Image": image_uri, "ModelDataUrl": model_data_url}],
                "SupportedContentTypes": ["text/csv", "application/json"],
                "SupportedResponseMIMETypes": ["application/json"],
            },
            ModelApprovalStatus=approval_status,
        )
        if metrics:
            kwargs["ModelMetrics"] = {
                "ModelQuality": {"Statistics": {"ContentType": "application/json",
                                                "S3Uri": metrics.get("s3_uri", "")}}
            }
        if customer_metadata:
            kwargs["CustomerMetadataProperties"] = {k: str(v) for k, v in customer_metadata.items()}
        resp = self.sm.create_model_package(**kwargs)
        arn = resp["ModelPackageArn"]
        logger.info(f"registered {self.MODEL_PACKAGE_GROUP} → {arn} (status={approval_status})")
        return arn

    def approve(self, model_package_arn: str):
        """Approve a package (triggers monitoring / CD pipeline)."""
        self.sm.update_model_package(ModelPackageArn=model_package_arn,
                                     ModelApprovalStatus="Approved")
        logger.info(f"approved: {model_package_arn}")

    def get_latest_approved(self) -> str:
        """Get latest Approved package ARN (used at inference time)."""
        resp = self.sm.list_model_packages(
            ModelPackageGroupName=self.MODEL_PACKAGE_GROUP,
            ModelApprovalStatus="Approved",
            SortBy="CreationTime", SortOrder="Descending", MaxResults=1)
        pkgs = resp.get("ModelPackageSummaryList", [])
        if not pkgs:
            raise RuntimeError(f"No Approved packages in {self.MODEL_PACKAGE_GROUP}")
        return pkgs[0]["ModelPackageArn"]

    def get_latest_version_number(self) -> int:
        """Highest version number in the group."""
        resp = self.sm.list_model_packages(
            ModelPackageGroupName=self.MODEL_PACKAGE_GROUP,
            SortBy="CreationTime", SortOrder="Descending", MaxResults=1)
        pkgs = resp.get("ModelPackageSummaryList", [])
        return int(pkgs[0]["ModelPackageArn"].split("/")[-1]) if pkgs else 0


if __name__ == "__main__":
    reg = ModelRegistryAWS({"model_package_group": "ChurnModelGroup"})  # CHANGE_ME
    # arn = reg.register(model_data_url="s3://...", image_uri="...", approval_status="Approved")
    print(f"latest approved: {reg.get_latest_approved()}")

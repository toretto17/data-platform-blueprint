"""
================================================================================
BATCH + REAL-TIME INFERENCE — [AWS SageMaker]
================================================================================
Purpose: Score data using a registered SageMaker model.
    • Batch: SageMaker Batch Transform (scales automatically, no persistent infra)
    • Real-time: SageMaker Endpoint (persistent, low-latency)

Pattern (from production — batch_transform.py):
    1. Resolve model package (latest approved from Model Registry group)
    2. Create a SageMaker Model resource
    3. Submit Batch Transform (or create/update endpoint)
    4. Poll for completion
    5. Read output from S3

Cost-effective options:
    - Batch: use spot instances (--max-pay <bid>) for up to 90% savings
    - Realtime: Serverless Inference (auto-scale, pay per invocation) OR
                managed scaling with min_instance_count=0 (new feature)
    - Optional data capture (BatchDataCaptureConfig) for monitoring

Customize: MODEL_PACKAGE_GROUP, input/output S3, instance_type.
Databricks twin: databricks/src/mlops/inference/inference.py
Version : 2026-06-29
================================================================================
"""
import logging
import time
from typing import Optional

import boto3

logger = logging.getLogger("inference_aws")


class BatchTransformAWS:
    """Submit a SageMaker Batch Transform job."""

    MODEL_PACKAGE_GROUP: str = "CHANGE_ME_ModelGroup"
    INPUT_S3: str = "s3://CHANGE_ME/inference/input/"
    OUTPUT_S3: str = "s3://CHANGE_ME/inference/output/"
    INSTANCE_TYPE: str = "ml.m5.xlarge"
    INSTANCE_COUNT: int = 1
    REGION: str = "ap-southeast-1"

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        self.sm = boto3.client("sagemaker", region_name=self.REGION)

    def _resolve_model_arn(self) -> str:
        """Get the latest Approved model package ARN from the registry group."""
        resp = self.sm.list_model_packages(
            ModelPackageGroupName=self.MODEL_PACKAGE_GROUP,
            ModelApprovalStatus="Approved",
            SortBy="CreationTime", SortOrder="Descending", MaxResults=1)
        pkgs = resp.get("ModelPackageSummaryList", [])
        if not pkgs:
            raise RuntimeError(f"No Approved model in {self.MODEL_PACKAGE_GROUP}")
        return pkgs[0]["ModelPackageArn"]

    def _create_model(self, model_package_arn: str, model_name: str):
        """Create a SageMaker Model from a model package."""
        self.sm.create_model(
            ModelName=model_name,
            PrimaryContainer={"ModelPackageName": model_package_arn},
            ExecutionRoleArn=f"arn:aws:iam::CHANGE_ME:role/SageMakerExecRole",  # CHANGE_ME
        )
        logger.info(f"model created: {model_name}")

    def submit(self, job_name: Optional[str] = None) -> str:
        """Submit Batch Transform and poll to completion. Returns output S3 path."""
        import uuid
        job_name = job_name or f"bt-{uuid.uuid4().hex[:8]}"
        model_arn = self._resolve_model_arn()
        model_name = f"model-{job_name}"
        self._create_model(model_arn, model_name)

        self.sm.create_transform_job(
            TransformJobName=job_name,
            ModelName=model_name,
            TransformInput={"DataSource": {"S3DataSource": {
                "S3DataType": "S3Prefix", "S3Uri": self.INPUT_S3}},
                "ContentType": "text/csv"},
            TransformOutput={"S3OutputPath": self.OUTPUT_S3},
            TransformResources={"InstanceType": self.INSTANCE_TYPE,
                                "InstanceCount": self.INSTANCE_COUNT},
        )
        logger.info(f"Batch Transform submitted: {job_name}")

        # Poll
        while True:
            desc = self.sm.describe_transform_job(TransformJobName=job_name)
            status = desc["TransformJobStatus"]
            if status in ("Completed", "Failed", "Stopped"):
                break
            time.sleep(30)
        if status != "Completed":
            raise RuntimeError(f"Batch Transform {status}: {desc.get('FailureReason')}")
        logger.info(f"Batch Transform complete: {self.OUTPUT_S3}")
        # Cleanup model
        try:
            self.sm.delete_model(ModelName=model_name)
        except Exception:
            pass
        return self.OUTPUT_S3


class RealtimeEndpointAWS:
    """Create/update a SageMaker real-time endpoint (or Serverless Inference)."""

    ENDPOINT_NAME: str = "CHANGE_ME-endpoint"
    MODEL_PACKAGE_GROUP: str = "CHANGE_ME_ModelGroup"
    INSTANCE_TYPE: str = "ml.m5.large"                    # for provisioned
    SERVERLESS: bool = True                                # cost-effective default
    SERVERLESS_MEMORY: int = 2048                          # MB
    SERVERLESS_MAX_CONCURRENCY: int = 5
    REGION: str = "ap-southeast-1"

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        self.sm = boto3.client("sagemaker", region_name=self.REGION)

    def create_or_update(self):
        """Create/update an inference endpoint. Idempotent."""
        # Resolve model
        resp = self.sm.list_model_packages(
            ModelPackageGroupName=self.MODEL_PACKAGE_GROUP,
            ModelApprovalStatus="Approved", SortBy="CreationTime",
            SortOrder="Descending", MaxResults=1)
        model_arn = resp["ModelPackageSummaryList"][0]["ModelPackageArn"]

        model_name = f"{self.ENDPOINT_NAME}-model"
        try:
            self.sm.create_model(ModelName=model_name,
                                 PrimaryContainer={"ModelPackageName": model_arn},
                                 ExecutionRoleArn="arn:aws:iam::CHANGE_ME:role/SageMakerExecRole")
        except self.sm.exceptions.ClientError:
            pass  # already exists

        # Endpoint config
        cfg_name = f"{self.ENDPOINT_NAME}-cfg"
        variant = {"VariantName": "AllTraffic", "ModelName": model_name, "InitialVariantWeight": 1}
        if self.SERVERLESS:
            variant["ServerlessConfig"] = {
                "MemorySizeInMB": self.SERVERLESS_MEMORY,
                "MaxConcurrency": self.SERVERLESS_MAX_CONCURRENCY}
        else:
            variant["InstanceType"] = self.INSTANCE_TYPE
            variant["InitialInstanceCount"] = 1
        try:
            self.sm.create_endpoint_config(EndpointConfigName=cfg_name,
                                            ProductionVariants=[variant])
        except self.sm.exceptions.ClientError:
            pass

        try:
            self.sm.create_endpoint(EndpointName=self.ENDPOINT_NAME, EndpointConfigName=cfg_name)
            logger.info(f"endpoint creating: {self.ENDPOINT_NAME}")
        except self.sm.exceptions.ClientError:
            self.sm.update_endpoint(EndpointName=self.ENDPOINT_NAME, EndpointConfigName=cfg_name)
            logger.info(f"endpoint updating: {self.ENDPOINT_NAME}")


if __name__ == "__main__":
    # Batch
    bt = BatchTransformAWS({"model_package_group": "MyModelGroup",
                             "input_s3": "s3://bucket/input/", "output_s3": "s3://bucket/output/"})
    bt.submit()

"""
================================================================================
MLOPS TRAINING PIPELINE TEMPLATE — SageMaker
================================================================================
Purpose: Template for model training pipelines using SageMaker.
         Supports: preprocess → train → evaluate → register → promote.

Patterns extracted from production:
    - Config tiers: A (recipe/code), B (per-run DDB tunables), C (per-deploy infra)
    - BYOC images for custom algorithms
    - Batch Transform for inference
    - Model Registry for promotion gates
    - MLflow integration for experiment tracking

Usage:
    1. Define your model config (hyperparams, instance types)
    2. Implement preprocess/train/evaluate steps
    3. Register pipeline with SageMaker
    4. Trigger via Step Function or manual

Key Decision: SageMaker Pipelines (managed DAG) vs Step Functions (custom orchestration)
    - Use SM Pipelines for standard train→evaluate→register flows
    - Use Step Functions when you need Glue + SageMaker + Lambda in one DAG
================================================================================
"""
import os
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mlops_training")


# ============================================================================
# CONFIGURATION
# ============================================================================
@dataclass
class ModelConfig:
    """Model training configuration. Override per model."""
    model_name: str = "CHANGE_ME"                   # e.g., "sales-anomaly", "sales-forecast"
    model_package_group: str = "CHANGE_ME-models"   # Model Registry group

    # Training
    instance_type: str = "ml.m5.xlarge"
    instance_count: int = 1
    max_runtime_seconds: int = 14400                # 4 hours
    hyperparameters: Dict[str, str] = field(default_factory=dict)

    # Inference
    inference_instance_type: str = "ml.m5.large"
    batch_transform_instance: str = "ml.m5.xlarge"
    batch_transform_concurrency: int = 4

    # Data
    train_data_s3: str = ""        # s3://bucket/path/to/training/data
    validation_data_s3: str = ""   # s3://bucket/path/to/validation/data
    output_s3: str = ""            # s3://bucket/path/to/output

    # Evaluation gate
    min_accuracy: float = 0.8
    max_mape: float = 0.15

    # BYOC Image (if custom algorithm)
    image_uri: Optional[str] = None  # e.g., "{account}.dkr.ecr.{region}.amazonaws.com/my-model:latest"


@dataclass
class InfraConfig:
    """Infrastructure identity (Tier C — per-deploy, env-overridable)."""
    account_id: str = os.getenv("MLOPS_ACCOUNT_ID", "CHANGE_ME")
    region: str = os.getenv("MLOPS_REGION", "ap-southeast-1")
    env_slug: str = os.getenv("MLOPS_ENV_SLUG", "nonprod")
    role_arn: str = os.getenv("MLOPS_ROLE_ARN", "CHANGE_ME")

    @property
    def bucket_prefix(self):
        return f"s3-CHANGE_PROJECT-CHANGE_FEATURE-{self.env_slug}"

    @property
    def ml_bucket(self):
        return f"{self.bucket_prefix}-ml-results-{self.account_id}"

    @property
    def feature_bucket(self):
        return f"{self.bucket_prefix}-feature-{self.account_id}"


# ============================================================================
# PIPELINE STEPS (implement your logic)
# ============================================================================
class BaseTrainingPipeline:
    """
    Base training pipeline. Override:
        - preprocess(): data prep
        - train(): model training
        - evaluate(): model evaluation
        - register(): model registration
    """

    def __init__(self, model_config: ModelConfig, infra_config: InfraConfig):
        self.model_cfg = model_config
        self.infra_cfg = infra_config

    def preprocess(self) -> str:
        """Preprocess data. Returns S3 path to processed data.
        Override with your feature engineering logic.
        """
        raise NotImplementedError

    def train(self, processed_data_s3: str) -> str:
        """Train model. Returns S3 path to model artifact.
        Override with your training logic.
        """
        raise NotImplementedError

    def evaluate(self, model_artifact_s3: str) -> Dict[str, float]:
        """Evaluate model. Returns metrics dict.
        Override with your evaluation logic.
        """
        raise NotImplementedError

    def register(self, model_artifact_s3: str, metrics: Dict[str, float]) -> str:
        """Register model in Model Registry. Returns model package ARN."""
        import boto3
        sm = boto3.client("sagemaker", region_name=self.infra_cfg.region)

        # Check evaluation gate
        if not self._passes_gate(metrics):
            logger.warning(f"Model failed evaluation gate: {metrics}")
            return ""

        response = sm.create_model_package(
            ModelPackageGroupName=self.model_cfg.model_package_group,
            ModelPackageDescription=f"{self.model_cfg.model_name} - metrics: {metrics}",
            InferenceSpecification={
                "Containers": [{
                    "Image": self.model_cfg.image_uri or "CHANGE_ME",
                    "ModelDataUrl": model_artifact_s3,
                }],
                "SupportedTransformInstanceTypes": [self.model_cfg.batch_transform_instance],
                "SupportedContentTypes": ["application/json"],
                "SupportedResponseMIMETypes": ["application/json"],
            },
            ModelApprovalStatus="PendingManualApproval",
            CustomerMetadataProperties={k: str(v) for k, v in metrics.items()},
        )
        arn = response["ModelPackageArn"]
        logger.info(f"Registered model: {arn}")
        return arn

    def _passes_gate(self, metrics: Dict[str, float]) -> bool:
        """Evaluation gate. Override for custom logic."""
        # Example: check accuracy threshold
        accuracy = metrics.get("accuracy", 0)
        return accuracy >= self.model_cfg.min_accuracy

    def run(self):
        """Execute full pipeline."""
        logger.info(f"Starting training pipeline: {self.model_cfg.model_name}")

        processed = self.preprocess()
        artifact = self.train(processed)
        metrics = self.evaluate(artifact)

        logger.info(f"Evaluation metrics: {metrics}")
        model_arn = self.register(artifact, metrics)

        if model_arn:
            logger.info(f"Pipeline complete. Model: {model_arn}")
        else:
            logger.warning("Pipeline complete but model not registered (failed gate)")

        return {"model_arn": model_arn, "metrics": metrics}


# ============================================================================
# BATCH INFERENCE TEMPLATE
# ============================================================================
class BaseBatchInference:
    """Template for batch inference using SageMaker Batch Transform."""

    def __init__(self, model_config: ModelConfig, infra_config: InfraConfig):
        self.model_cfg = model_config
        self.infra_cfg = infra_config

    def get_latest_approved_model(self) -> str:
        """Get latest approved model package ARN from registry."""
        import boto3
        sm = boto3.client("sagemaker", region_name=self.infra_cfg.region)
        response = sm.list_model_packages(
            ModelPackageGroupName=self.model_cfg.model_package_group,
            ModelApprovalStatus="Approved",
            SortBy="CreationTime",
            SortOrder="Descending",
            MaxResults=1,
        )
        packages = response.get("ModelPackageSummaryList", [])
        if not packages:
            raise ValueError(f"No approved models in {self.model_cfg.model_package_group}")
        return packages[0]["ModelPackageArn"]

    def run_batch_transform(self, input_s3: str, output_s3: str,
                            model_name: Optional[str] = None) -> str:
        """Run batch transform. Returns output S3 path."""
        import boto3
        sm = boto3.client("sagemaker", region_name=self.infra_cfg.region)

        model_arn = model_name or self.get_latest_approved_model()
        job_name = f"{self.model_cfg.model_name}-bt-{int(__import__('time').time())}"

        sm.create_transform_job(
            TransformJobName=job_name,
            ModelName=model_arn,
            TransformInput={
                "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": input_s3}},
                "ContentType": "application/json",
                "SplitType": "Line",
            },
            TransformOutput={"S3OutputPath": output_s3, "AssembleWith": "Line"},
            TransformResources={
                "InstanceType": self.model_cfg.batch_transform_instance,
                "InstanceCount": self.model_cfg.batch_transform_concurrency,
            },
            MaxConcurrentTransforms=self.model_cfg.batch_transform_concurrency,
            MaxPayloadInMB=6,
        )
        logger.info(f"Started batch transform: {job_name}")
        return output_s3

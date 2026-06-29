"""
================================================================================
MODEL DEPLOYMENT + ROLLBACK — [AWS SageMaker]
================================================================================
Purpose: Deploy a registered model to production (endpoint OR batch-only) and
         provide a safe rollback mechanism if issues are detected post-deploy.

Pattern (production best practice):
    1. Deploy = approve the model package (triggers monitoring setup + endpoint update)
    2. Canary/shadow: route % traffic to new model (UpdateEndpoint VariantWeight)
    3. Rollback: re-approve the previous version OR update endpoint to old config

Key best practices:
    - Never deploy directly to 100% traffic — use canary (10-25%) first
    - Keep the previous model version ARN for instant rollback
    - Rollback = update endpoint config to point at previous model (< 60s)
    - All deployments tracked in an audit/DDB table

Customize: ENDPOINT_NAME, MODEL_PACKAGE_GROUP, CANARY_WEIGHT, ROLE_ARN.
Databricks twin: databricks/src/mlops/deployment/deployment.py
Version : 2026-06-29
================================================================================
"""
import logging
import time
from typing import Optional

import boto3

logger = logging.getLogger("deployment_aws")


class ModelDeploymentAWS:
    # ---- CHANGE_ME ----
    MODEL_PACKAGE_GROUP: str = "CHANGE_ME_ModelGroup"
    ENDPOINT_NAME: str = "CHANGE_ME-endpoint"
    ROLE_ARN: str = "arn:aws:iam::CHANGE_ME:role/SageMakerExecRole"
    INSTANCE_TYPE: str = "ml.m5.large"
    CANARY_WEIGHT: float = 0.1                       # 10% traffic to new model initially
    REGION: str = "ap-southeast-1"

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        self.sm = boto3.client("sagemaker", region_name=self.REGION)

    def _latest_approved_arn(self) -> str:
        resp = self.sm.list_model_packages(
            ModelPackageGroupName=self.MODEL_PACKAGE_GROUP,
            ModelApprovalStatus="Approved", SortBy="CreationTime",
            SortOrder="Descending", MaxResults=1)
        pkgs = resp.get("ModelPackageSummaryList", [])
        if not pkgs:
            raise RuntimeError(f"No Approved model in {self.MODEL_PACKAGE_GROUP}")
        return pkgs[0]["ModelPackageArn"]

    def _previous_approved_arn(self) -> Optional[str]:
        """Second-latest Approved (for rollback)."""
        resp = self.sm.list_model_packages(
            ModelPackageGroupName=self.MODEL_PACKAGE_GROUP,
            ModelApprovalStatus="Approved", SortBy="CreationTime",
            SortOrder="Descending", MaxResults=2)
        pkgs = resp.get("ModelPackageSummaryList", [])
        return pkgs[1]["ModelPackageArn"] if len(pkgs) > 1 else None

    def _create_model(self, arn: str, name: str):
        try:
            self.sm.create_model(ModelName=name,
                                 PrimaryContainer={"ModelPackageName": arn},
                                 ExecutionRoleArn=self.ROLE_ARN)
        except self.sm.exceptions.ClientError:
            pass  # already exists

    # ---- Deploy (canary: new model gets CANARY_WEIGHT, old keeps rest) ----
    def deploy_canary(self):
        """Deploy latest approved model as a canary variant alongside the existing one."""
        new_arn = self._latest_approved_arn()
        new_model = f"{self.ENDPOINT_NAME}-new"
        self._create_model(new_arn, new_model)

        cfg_name = f"{self.ENDPOINT_NAME}-canary-cfg"
        old_model = f"{self.ENDPOINT_NAME}-model"  # existing model name
        self.sm.create_endpoint_config(
            EndpointConfigName=cfg_name,
            ProductionVariants=[
                {"VariantName": "Champion", "ModelName": old_model,
                 "InitialVariantWeight": 1 - self.CANARY_WEIGHT,
                 "InstanceType": self.INSTANCE_TYPE, "InitialInstanceCount": 1},
                {"VariantName": "Canary", "ModelName": new_model,
                 "InitialVariantWeight": self.CANARY_WEIGHT,
                 "InstanceType": self.INSTANCE_TYPE, "InitialInstanceCount": 1},
            ])
        self.sm.update_endpoint(EndpointName=self.ENDPOINT_NAME, EndpointConfigName=cfg_name)
        logger.info(f"canary deployed: {self.CANARY_WEIGHT*100:.0f}% → {new_model}")

    def promote_canary(self):
        """Shift 100% traffic to the canary (it becomes the new Champion)."""
        # Update variant weights to 100% canary
        self.sm.update_endpoint_weights_and_capacities(
            EndpointName=self.ENDPOINT_NAME,
            DesiredWeightsAndCapacities=[
                {"VariantName": "Canary", "DesiredWeight": 1.0},
                {"VariantName": "Champion", "DesiredWeight": 0.0},
            ])
        logger.info("canary promoted to 100% — old champion at 0%")

    def rollback(self):
        """Instant rollback: shift traffic back to Champion (or re-deploy previous version)."""
        try:
            self.sm.update_endpoint_weights_and_capacities(
                EndpointName=self.ENDPOINT_NAME,
                DesiredWeightsAndCapacities=[
                    {"VariantName": "Champion", "DesiredWeight": 1.0},
                    {"VariantName": "Canary", "DesiredWeight": 0.0},
                ])
            logger.info("ROLLBACK: 100% traffic → Champion (previous model)")
        except Exception as e:
            logger.error(f"rollback via weight shift failed ({e}); re-deploying previous version")
            prev = self._previous_approved_arn()
            if prev:
                self._create_model(prev, f"{self.ENDPOINT_NAME}-model")
                cfg = f"{self.ENDPOINT_NAME}-rollback-cfg"
                self.sm.create_endpoint_config(
                    EndpointConfigName=cfg,
                    ProductionVariants=[{"VariantName": "Champion",
                                         "ModelName": f"{self.ENDPOINT_NAME}-model",
                                         "InitialVariantWeight": 1.0,
                                         "InstanceType": self.INSTANCE_TYPE,
                                         "InitialInstanceCount": 1}])
                self.sm.update_endpoint(EndpointName=self.ENDPOINT_NAME, EndpointConfigName=cfg)
                logger.info("ROLLBACK: re-deployed previous approved model version")

    # ---- Batch-only deploy (no endpoint — just approve the package) ----
    def approve_for_batch(self, model_package_arn: Optional[str] = None):
        """For batch-only workflows: approve the latest package (triggers monitoring)."""
        arn = model_package_arn or self._latest_approved_arn()
        self.sm.update_model_package(ModelPackageArn=arn, ModelApprovalStatus="Approved")
        logger.info(f"approved (batch-only deploy): {arn}")


if __name__ == "__main__":
    deployer = ModelDeploymentAWS({"endpoint_name": "churn-v1",
                                    "model_package_group": "ChurnModelGroup"})
    # deployer.deploy_canary()
    # ... monitor for a period ...
    # deployer.promote_canary()   # OR deployer.rollback()

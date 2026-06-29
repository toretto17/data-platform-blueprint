"""
================================================================================
ML PIPELINE — End-to-End Orchestration  [AWS SageMaker Pipelines @step]
================================================================================
Purpose: Orchestrate the full ML lifecycle as a SageMaker Pipeline:
         train → evaluate → (condition gate) → register → (optional deploy)

Modern pattern (verified AWS docs):
    - @step decorator: convert any Python function into a pipeline step
    - ConditionStep: gate registration on evaluation metrics
    - Pipeline: assemble steps into a DAG, upsert, trigger

@step advantages over classic TrainingStep/ProcessingStep:
    - Write plain Python (no container boilerplate)
    - Test locally (then runs remotely at scale)
    - Automatic DAG resolution from function call graph

Customize (CHANGE_ME):
    - train(), evaluate(), register() function bodies
    - Pipeline name, instance_type, dependencies
    - ConditionStep threshold

Platform notes: SageMaker Python SDK v2.200+; requires @step support (2024+).
Databricks twin: databricks/src/mlops/pipelines/ml_pipeline.py (Databricks Workflows).
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Tuple

logger = logging.getLogger("ml_pipeline_aws")


def build_pipeline():
    """Build and return a SageMaker Pipeline (upsert to create/update it)."""
    from sagemaker.workflow.pipeline import Pipeline
    from sagemaker.workflow.condition_step import ConditionStep
    from sagemaker.workflow.conditions import ConditionGreaterThanOrEqualTo
    from sagemaker.workflow.functions import JsonGet
    from sagemaker.workflow.pipeline_context import PipelineSession
    from sagemaker import get_execution_role
    from sagemaker.workflow.step_outputs import get_step

    # Pipeline session (lazy execution — no jobs submitted until pipeline runs)
    pipeline_session = PipelineSession()
    role = get_execution_role()

    # ---- STEP 1: Train ----
    from sagemaker.workflow.function_step import step

    @step(
        name="Train",
        instance_type="ml.m5.xlarge",                    # CHANGE_ME
        keep_alive_period_in_seconds=300,                 # warm pool (faster restarts)
    )
    def train() -> Tuple[str, str]:
        """Train model, return (model_artifact_s3, eval_metrics_s3)."""
        # CHANGE_ME: your training logic here
        # This runs INSIDE a SageMaker managed container
        import json, os, joblib
        from sklearn.ensemble import GradientBoostingClassifier
        import pandas as pd

        # Read training data (passed via pipeline parameters or hardcoded S3 path)
        train_data = pd.read_csv("s3://CHANGE_ME/data/train.csv")   # CHANGE_ME
        X = train_data.drop(columns=["label"])
        y = train_data["label"]

        model = GradientBoostingClassifier(n_estimators=100, random_state=42)
        model.fit(X, y)

        # Save model artifact
        output_dir = "/opt/ml/model"
        os.makedirs(output_dir, exist_ok=True)
        joblib.dump(model, f"{output_dir}/model.pkl")
        model_uri = "s3://CHANGE_ME/models/model.tar.gz"           # CHANGE_ME

        # Evaluate inline (or in separate step)
        from sklearn.metrics import f1_score
        preds = model.predict(X)
        f1 = float(f1_score(y, preds, average="weighted"))
        metrics_uri = "s3://CHANGE_ME/metrics/evaluation.json"      # CHANGE_ME
        # (In production, upload to S3 — simplified here)

        return model_uri, json.dumps({"f1": f1})

    # ---- STEP 2: Evaluate (condition gate) ----
    @step(name="Evaluate", instance_type="ml.m5.large")
    def evaluate(model_uri: str, metrics_json: str) -> bool:
        """Parse metrics, return True if thresholds pass."""
        import json
        metrics = json.loads(metrics_json)
        passed = metrics.get("f1", 0) >= 0.6              # CHANGE_ME threshold
        return passed

    # ---- STEP 3: Register (only if gate passes) ----
    @step(name="Register", instance_type="ml.m5.large")
    def register(model_uri: str, passed: bool) -> str:
        """Register the model in SageMaker Model Registry if eval passed."""
        if not passed:
            return "SKIPPED"
        import boto3
        sm = boto3.client("sagemaker")
        # CHANGE_ME: actual registration logic (see registry.py for production version)
        return "REGISTERED"

    # ---- Assemble DAG ----
    train_result = train()
    model_uri = train_result[0]
    metrics_json = train_result[1]
    eval_passed = evaluate(model_uri, metrics_json)
    register_result = register(model_uri, eval_passed)

    # Build pipeline
    pipeline = Pipeline(
        name="CHANGE_ME-ml-pipeline",                     # CHANGE_ME
        steps=[get_step(register_result)],                # leaf step — SDK traces back to train
        sagemaker_session=pipeline_session,
    )
    return pipeline


def upsert_and_run():
    """Create/update pipeline definition + start an execution."""
    pipeline = build_pipeline()
    pipeline.upsert(role_arn="arn:aws:iam::CHANGE_ME:role/SageMakerPipelineRole")  # CHANGE_ME
    execution = pipeline.start()
    logger.info(f"pipeline execution started: {execution.arn}")
    return execution


if __name__ == "__main__":
    upsert_and_run()

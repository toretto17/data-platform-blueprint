"""
================================================================================
ML PIPELINE — End-to-End Orchestration  [Databricks Workflows / MLflow]
================================================================================
Purpose: Orchestrate the full ML lifecycle as a Databricks Workflow (multi-task job):
         ingest features → train → evaluate → (gate) → register → deploy

Pattern (verified docs.databricks.com):
    - Databricks Workflows: multi-task DAG defined as a job (JSON/YAML/SDK/UI)
    - Each task = a notebook, Python script, or wheel package
    - Task dependencies define the DAG order
    - Conditional task ("if_else_condition") for eval gate

This file provides:
    1. A Python-based workflow definition (Databricks SDK: WorkspaceClient.jobs.create)
    2. The individual task functions (importable — each task runs one of these)

Alternative: define in a `databricks.yml` Asset Bundle (see infrastructure/databricks/).

Cost-effective:
    - Use job clusters (ephemeral — auto-terminate after task) vs all-purpose
    - Auto-scaling enabled (start small, scale if needed)
    - Spot instances for training tasks (CHANGE_ME in cluster_spec)

Customize: JOB_NAME, CLUSTER_SPEC, notebook/script paths, task parameters.
AWS twin: aws/src/mlops/pipelines/ml_pipeline.py (SageMaker Pipelines @step).
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Optional

logger = logging.getLogger("ml_pipeline_databricks")


# ============================================================================
# WORKFLOW DEFINITION (create/update the Databricks multi-task job)
# ============================================================================
def create_ml_workflow(cfg: Optional[dict] = None):
    """Create or update a Databricks Workflow (multi-task job) for the ML pipeline.
    Uses the Databricks SDK — same as what the UI produces, but in code (reproducible)."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import (
        Task, NotebookTask, TaskDependency,
        JobCluster, ClusterSpec, AutoScale,
    )

    w = WorkspaceClient()
    cfg = cfg or {}
    job_name = cfg.get("job_name", "CHANGE_ME-ml-pipeline")

    # Shared ephemeral cluster (cost-effective: auto-terminate, auto-scale)
    cluster_spec = JobCluster(
        job_cluster_key="ml_cluster",
        new_cluster=ClusterSpec(
            spark_version="15.4.x-cpu-ml-scala2.12",       # CHANGE_ME: DBR version
            node_type_id=cfg.get("node_type", "m5.xlarge"),  # CHANGE_ME
            autoscale=AutoScale(min_workers=1, max_workers=4),
            # CHANGE_ME: add spot policy for cost savings
            # aws_attributes={"availability": "SPOT_WITH_FALLBACK"}
        ),
    )

    # Task definitions (each task runs a notebook/script)
    tasks = [
        Task(
            task_key="ingest_features",
            job_cluster_key="ml_cluster",
            notebook_task=NotebookTask(
                notebook_path="/Repos/CHANGE_ME/feature_store/ingestion/feature_store_job",
                base_parameters={"mode": "merge", "lookback_months": "3"}),
        ),
        Task(
            task_key="train",
            depends_on=[TaskDependency(task_key="ingest_features")],
            job_cluster_key="ml_cluster",
            notebook_task=NotebookTask(
                notebook_path="/Repos/CHANGE_ME/mlops/training/training_pipeline",
                base_parameters={}),
        ),
        Task(
            task_key="evaluate",
            depends_on=[TaskDependency(task_key="train")],
            job_cluster_key="ml_cluster",
            notebook_task=NotebookTask(
                notebook_path="/Repos/CHANGE_ME/mlops/evaluation/evaluate",
                base_parameters={}),
        ),
        # Conditional: only register + deploy if evaluate passes
        Task(
            task_key="register_and_deploy",
            depends_on=[TaskDependency(task_key="evaluate")],
            # condition_task for gating (or handle inside the notebook)
            job_cluster_key="ml_cluster",
            notebook_task=NotebookTask(
                notebook_path="/Repos/CHANGE_ME/mlops/deployment/deployment",
                base_parameters={"action": "deploy"}),
        ),
    ]

    # Create or update the job
    existing = [j for j in w.jobs.list(name=job_name)]
    if existing:
        job_id = existing[0].job_id
        w.jobs.reset(job_id=job_id, new_settings={"name": job_name, "tasks": tasks,
                                                   "job_clusters": [cluster_spec]})
        logger.info(f"updated workflow: {job_name} (job_id={job_id})")
    else:
        created = w.jobs.create(name=job_name, tasks=tasks, job_clusters=[cluster_spec])
        logger.info(f"created workflow: {job_name} (job_id={created.job_id})")


# ============================================================================
# TRIGGER A RUN (programmatic)
# ============================================================================
def trigger_run(job_name: str = "CHANGE_ME-ml-pipeline", params: Optional[dict] = None):
    """Trigger a single run of the ML workflow."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    jobs = list(w.jobs.list(name=job_name))
    if not jobs:
        raise RuntimeError(f"job '{job_name}' not found")
    run = w.jobs.run_now(job_id=jobs[0].job_id, notebook_params=params or {})
    logger.info(f"triggered run: {run.run_id}")
    return run.run_id


if __name__ == "__main__":
    # Create/update the workflow definition
    create_ml_workflow({"job_name": "churn-ml-pipeline"})   # CHANGE_ME

    # Trigger a run
    # trigger_run("churn-ml-pipeline")

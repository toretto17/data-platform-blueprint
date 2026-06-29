"""
================================================================================
ORCHESTRATION — Databricks Workflows + Scheduling  [Databricks]
================================================================================
Purpose: Programmatically create/update multi-task Databricks Workflows (jobs)
         for ETL pipelines, with cron scheduling.

Contents:
    - WorkflowManager: create/update/trigger/poll a Databricks Workflow job
    - build_etl_workflow(): build a linear ETL pipeline (ingest→silver→gold→consumption)

Best practices:
    - Job clusters (ephemeral) for cost — auto-terminate after tasks
    - Auto-scaling (min=1, max=N) to handle variable load
    - Use task dependencies for DAG ordering (not sequential execution)
    - Spot/preemptible for non-critical tasks (cost savings up to 90%)
    - Alerts on failure (email/Slack webhook in job settings)

Customize: JOB_NAME, CLUSTER_SPEC, notebook paths, schedule cron.
AWS twin: aws/src/orchestration/orchestration.py (Step Functions + EventBridge)
Version : 2026-06-29
================================================================================
"""
import logging
from typing import List, Optional

logger = logging.getLogger("orchestration_databricks")


class WorkflowManager:
    """Create/update/trigger Databricks Workflow jobs via SDK."""

    def __init__(self):
        from databricks.sdk import WorkspaceClient
        self.w = WorkspaceClient()

    def create_or_update(self, job_name: str, tasks: list, cluster_spec: dict,
                         schedule_cron: Optional[str] = None):
        """Create or update a Databricks job. `tasks` = list of Task objects (SDK).
        schedule_cron: Quartz cron e.g. '0 0 18 * * ?' (6PM daily)."""
        from databricks.sdk.service.jobs import CronSchedule
        existing = list(self.w.jobs.list(name=job_name))
        settings = {"name": job_name, "tasks": tasks, "job_clusters": [cluster_spec]}
        if schedule_cron:
            settings["schedule"] = CronSchedule(quartz_cron_expression=schedule_cron,
                                                 timezone_id="UTC")
        if existing:
            self.w.jobs.reset(job_id=existing[0].job_id, new_settings=settings)
            logger.info(f"updated workflow: {job_name}")
        else:
            self.w.jobs.create(**settings)
            logger.info(f"created workflow: {job_name}")

    def trigger(self, job_name: str, params: Optional[dict] = None) -> int:
        """Trigger a run. Returns run_id."""
        jobs = list(self.w.jobs.list(name=job_name))
        if not jobs:
            raise RuntimeError(f"job '{job_name}' not found")
        run = self.w.jobs.run_now(job_id=jobs[0].job_id, notebook_params=params or {})
        logger.info(f"triggered {job_name}: run_id={run.run_id}")
        return run.run_id

    def poll_run(self, run_id: int, poll_interval: int = 30) -> str:
        """Poll until terminal. Returns result_state (SUCCESS/FAILED/...)."""
        import time
        while True:
            run = self.w.jobs.get_run(run_id)
            state = run.state
            if state.result_state:
                logger.info(f"run {run_id} → {state.result_state.value}")
                return state.result_state.value
            time.sleep(poll_interval)


def build_etl_workflow(steps: List[dict], cluster_node_type: str = "m5.xlarge",
                       dbr_version: str = "15.4.x-scala2.12") -> tuple:
    """Build task + cluster definitions for a linear ETL pipeline.
    steps: [{"name": "silver", "notebook": "/Repos/.../silver_job"}]
    Returns (tasks, cluster_spec) ready for WorkflowManager.create_or_update()."""
    from databricks.sdk.service.jobs import (
        Task, NotebookTask, TaskDependency, JobCluster, ClusterSpec, AutoScale,
    )
    cluster = JobCluster(
        job_cluster_key="etl_cluster",
        new_cluster=ClusterSpec(
            spark_version=dbr_version,
            node_type_id=cluster_node_type,
            autoscale=AutoScale(min_workers=1, max_workers=4)))
    tasks = []
    for i, s in enumerate(steps):
        deps = [TaskDependency(task_key=steps[i-1]["name"])] if i > 0 else None
        tasks.append(Task(
            task_key=s["name"],
            depends_on=deps,
            job_cluster_key="etl_cluster",
            notebook_task=NotebookTask(
                notebook_path=s["notebook"],
                base_parameters=s.get("params", {}))))
    return tasks, cluster


if __name__ == "__main__":
    # Build + deploy an ETL pipeline workflow
    tasks, cluster = build_etl_workflow([
        {"name": "silver", "notebook": "/Repos/CHANGE_ME/silver/jobs/silver_job"},
        {"name": "gold", "notebook": "/Repos/CHANGE_ME/gold/marts/gold_job"},
        {"name": "consumption", "notebook": "/Repos/CHANGE_ME/consumption/jobs/consumption_job"},
    ])
    # wm = WorkflowManager()
    # wm.create_or_update("my-etl-pipeline", tasks, cluster, schedule_cron="0 0 18 * * ?")
    # wm.trigger("my-etl-pipeline")

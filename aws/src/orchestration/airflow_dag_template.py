"""
================================================================================
AIRFLOW DAG TEMPLATE — Cross-platform ETL orchestration  [Optional]
================================================================================
Purpose: If your team uses Apache Airflow (MWAA / Cloud Composer / self-hosted),
         this is a starter DAG for a Silver→Gold→Consumption ETL pipeline.
         Uses the Airflow GlueJobOperator (AWS) or DatabricksSubmitRunOperator (DBX).

Note: Step Functions (AWS) / Databricks Workflows (DBX) are the PRIMARY
      orchestration tools in this template. Airflow is an ALTERNATIVE for teams
      already on Airflow or needing cross-cloud orchestration.

Customize: DAG_ID, tasks, schedule, connections.
Version : 2026-06-29
================================================================================
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

# CHANGE_ME: pick your operator based on platform
# AWS: from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
# Databricks: from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

default_args = {
    "owner": "data-engineering",          # CHANGE_ME
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["CHANGE_ME@company.com"],
}

with DAG(
    dag_id="CHANGE_ME_etl_pipeline",      # CHANGE_ME
    default_args=default_args,
    description="ETL: Silver → Gold → Consumption",
    schedule_interval="0 18 * * *",        # CHANGE_ME: daily at 6PM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "production"],
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # CHANGE_ME: replace DummyOperator with actual operators
    # Example AWS:
    # silver = GlueJobOperator(task_id="silver", job_name="glue_..._silver_sales",
    #                          script_args={"--MODE": "append"}, aws_conn_id="aws_default")
    # Example Databricks:
    # silver = DatabricksSubmitRunOperator(task_id="silver",
    #     json={"existing_cluster_id": "CHANGE_ME", "notebook_task": {"notebook_path": "/Repos/.../silver_job"}})

    silver = DummyOperator(task_id="silver")   # CHANGE_ME
    gold = DummyOperator(task_id="gold")       # CHANGE_ME
    consumption = DummyOperator(task_id="consumption")  # CHANGE_ME

    start >> silver >> gold >> consumption >> end

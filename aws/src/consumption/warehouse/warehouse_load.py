"""
================================================================================
WAREHOUSE LOAD — Consumption → Amazon Redshift  [AWS]
================================================================================
Purpose: Load a consumption table (Parquet on S3, registered in Glue Catalog)
         into Amazon Redshift for BI. Implements the production "Spectrum → native"
         pattern (no data copy script needed — Spectrum reads the Glue Catalog;
         a stored statement does DELETE + INSERT into the native table).

Flow (real-world pattern):
    Glue writes consumption Parquet → Glue Catalog updated
      → Redshift external schema (Spectrum) reads the Catalog (no sync needed)
        → this job runs: DELETE target rows for the load window, then
           INSERT INTO <native> SELECT FROM <spectrum external table>

Why this pattern: avoids a brittle "mirror" job; Redshift always sees the latest
S3 data through Spectrum; the native table is refreshed transactionally.

Execution: uses the Redshift Data API (boto3 redshift-data) — no JDBC driver,
no cluster credentials in code (uses Secrets Manager OR IAM/temporary creds).

Customize (CHANGE_ME):
    - CLUSTER_ID / WORKGROUP (serverless), DATABASE, SECRET_ARN
    - EXTERNAL_SCHEMA (Spectrum), EXTERNAL_TABLE, NATIVE_SCHEMA, NATIVE_TABLE
    - LOAD_WINDOW_COL + value (e.g. mnth_id) for idempotent DELETE+INSERT

Platform notes:
    - AWS Redshift (provisioned via --CLUSTER_ID, or serverless via --WORKGROUP).
    - Databricks twin: databricks/src/consumption/warehouse/warehouse_load.py
      (builds Databricks SQL serving tables / materialized views instead).
Version : 2026-06-28
================================================================================
"""
import sys
import time
import logging

import boto3
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("warehouse_load_aws")


class RedshiftLoad:
    def __init__(self, args: dict):
        self.region = args.get("REGION", "ap-southeast-1")
        self.database = args["DATABASE"]                     # CHANGE_ME
        self.cluster_id = args.get("CLUSTER_ID")             # provisioned (or use workgroup)
        self.workgroup = args.get("WORKGROUP")               # serverless
        self.secret_arn = args.get("SECRET_ARN")             # for provisioned w/ secret auth
        self.external_schema = args["EXTERNAL_SCHEMA"]       # Spectrum schema (reads Glue Catalog)
        self.external_table = args["EXTERNAL_TABLE"]         # Glue/Spectrum table name
        self.native_schema = args["NATIVE_SCHEMA"]           # target native schema
        self.native_table = args["NATIVE_TABLE"]             # target native table
        self.window_col = args.get("LOAD_WINDOW_COL")        # e.g. mnth_id (idempotent reload)
        self.window_val = args.get("LOAD_WINDOW_VAL")        # e.g. 202606
        # Optional explicit column list (comma-separated). Strongly recommended:
        # relying on SELECT * requires identical column ORDER between the Spectrum
        # external table and the native table. An explicit list is order-safe.
        self.column_list = [c.strip() for c in args.get("COLUMN_LIST", "").split(",") if c.strip()]
        self.client = boto3.client("redshift-data", region_name=self.region)

    # ---- 0. one-time DDL helpers (run once, or guard with IF NOT EXISTS) ----
    def ddl_create_external_schema(self, glue_database: str, iam_role_arn: str) -> str:
        """Create the Spectrum external schema that points at the Glue Catalog DB."""
        return f"""
        CREATE EXTERNAL SCHEMA IF NOT EXISTS {self.external_schema}
        FROM DATA CATALOG DATABASE '{glue_database}'
        IAM_ROLE '{iam_role_arn}'
        CREATE EXTERNAL DATABASE IF NOT EXISTS;
        """

    # ---- 1. the load statement (idempotent DELETE + INSERT) ----
    def build_load_sql(self) -> str:
        """Refresh the native table for the load window from the Spectrum table.
        Idempotent: deletes the window first so re-runs don't duplicate."""
        target = f"{self.native_schema}.{self.native_table}"
        source = f"{self.external_schema}.{self.external_table}"
        if self.window_col and self.window_val:
            where = f"WHERE {self.window_col} = {self.window_val}"
        else:
            where = ""  # full refresh
        # Order-safe INSERT when an explicit column list is provided; otherwise SELECT *
        # (which requires identical column order between the external and native tables).
        if self.column_list:
            cols = ", ".join(self.column_list)
            insert_clause = f"INSERT INTO {target} ({cols})\n        SELECT {cols} FROM {source} {where}"
        else:
            insert_clause = f"INSERT INTO {target}\n        SELECT * FROM {source} {where}"
        # Use a transaction so BI never sees a half-loaded table.
        return f"""
        BEGIN;
        DELETE FROM {target} {where};
        {insert_clause};
        COMMIT;
        """

    # ---- 2. execute via Redshift Data API + poll ----
    def execute(self, sql: str):
        kwargs = {"Database": self.database, "Sql": sql}
        if self.workgroup:
            kwargs["WorkgroupName"] = self.workgroup          # serverless
        else:
            kwargs["ClusterIdentifier"] = self.cluster_id     # provisioned
            if self.secret_arn:
                kwargs["SecretArn"] = self.secret_arn
        resp = self.client.execute_statement(**kwargs)
        stmt_id = resp["Id"]
        logger.info(f"submitted statement {stmt_id}")
        # Poll to completion (no arbitrary sleeps — check status)
        while True:
            desc = self.client.describe_statement(Id=stmt_id)
            status = desc["Status"]
            if status in ("FINISHED", "FAILED", "ABORTED"):
                break
            time.sleep(2)
        if status != "FINISHED":
            raise RuntimeError(f"Redshift statement {stmt_id} {status}: {desc.get('Error')}")
        logger.info(f"statement {stmt_id} FINISHED ({desc.get('ResultRows', 0)} rows affected)")

    def run(self):
        sql = self.build_load_sql()
        logger.info(f"load SQL:\n{sql}")
        self.execute(sql)
        logger.info("Redshift load complete.")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME"]) if "--JOB_NAME" in sys.argv else {}
    for i, a in enumerate(sys.argv):
        if a.startswith("--") and i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith("--"):
            args[a[2:]] = sys.argv[i + 1]
    # CHANGE_ME defaults
    args.setdefault("DATABASE", "analytics")
    args.setdefault("EXTERNAL_SCHEMA", "spectrum_consumption")
    args.setdefault("EXTERNAL_TABLE", "sales_mart")
    args.setdefault("NATIVE_SCHEMA", "bi")
    args.setdefault("NATIVE_TABLE", "sales_mart")
    args.setdefault("WORKGROUP", "default")        # or set CLUSTER_ID for provisioned
    RedshiftLoad(args).run()

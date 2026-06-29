"""
================================================================================
SNOWFLAKE LOAD — Consumption → Snowflake  [AWS Glue + Spark Snowflake connector]
================================================================================
Purpose: Write a consumption DataFrame to Snowflake using the Spark-Snowflake
         connector. Credentials come from Secrets Manager (never hardcoded).

Connector setup (Glue job params):
    --extra-jars  s3://<bucket>/jars/spark-snowflake_2.12-<ver>.jar,
                  s3://<bucket>/jars/snowflake-jdbc-<ver>.jar

Write modes:
    • overwrite : replace the Snowflake table
    • append    : add rows
    • merge     : staged upsert (load to a temp table, then MERGE) — see _merge()

Customize (CHANGE_ME): SECRET_ID, SF_DATABASE, SF_SCHEMA, SF_WAREHOUSE, TABLE, KEYS.

Platform notes: identical connector API on Databricks (same file name there).
Version : 2026-06-28
================================================================================
"""
import sys
import json
import logging

import boto3
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("snowflake_load_aws")

SF_FORMAT = "net.snowflake.spark.snowflake"


class SnowflakeLoad:
    def __init__(self):
        self.args = getResolvedOptions(sys.argv, ["JOB_NAME", "SECRET_ID", "TABLE"])
        for i, a in enumerate(sys.argv):
            if a.startswith("--") and i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith("--"):
                self.args.setdefault(a[2:], sys.argv[i + 1])
        self.region = self.args.get("REGION", "ap-southeast-1")
        self.table = self.args["TABLE"]
        self.mode = self.args.get("MODE", "overwrite")        # overwrite|append|merge
        self.keys = [k for k in self.args.get("KEYS", "").split(",") if k]
        sc = SparkContext.getOrCreate()
        self.gc = GlueContext(sc)
        self.spark = self.gc.spark_session
        self.job = Job(self.gc)
        self.job.init(self.args["JOB_NAME"], self.args)
        self.sf_options = self._build_options()

    def _build_options(self) -> dict:
        """Pull Snowflake connection details from Secrets Manager."""
        sm = boto3.client("secretsmanager", region_name=self.region)
        s = json.loads(sm.get_secret_value(SecretId=self.args["SECRET_ID"])["SecretString"])
        return {
            "sfURL": s["url"],                # e.g. abc-xy12345.snowflakecomputing.com  CHANGE_ME secret
            "sfUser": s["user"],
            "sfPassword": s["password"],
            "sfDatabase": s.get("database", self.args.get("SF_DATABASE", "ANALYTICS")),
            "sfSchema": s.get("schema", self.args.get("SF_SCHEMA", "PUBLIC")),
            "sfWarehouse": s.get("warehouse", self.args.get("SF_WAREHOUSE", "COMPUTE_WH")),
            "sfRole": s.get("role", self.args.get("SF_ROLE", "")),
        }

    def write(self, df: DataFrame):
        if self.mode in ("overwrite", "append"):
            (df.write.format(SF_FORMAT).options(**self.sf_options)
               .option("dbtable", self.table).mode(self.mode).save())
            logger.info(f"[{self.mode}] wrote → Snowflake {self.table}")
        elif self.mode == "merge":
            self._merge(df)
        else:
            raise ValueError("MODE must be overwrite|append|merge")

    def _merge(self, df: DataFrame):
        """Staged upsert: write to a temp table, then run MERGE via the connector's
        Utils.runQuery (executes SQL in Snowflake)."""
        if not self.keys:
            raise ValueError("merge mode requires --KEYS")
        stage = f"{self.table}__stage"
        (df.write.format(SF_FORMAT).options(**self.sf_options)
           .option("dbtable", stage).mode("overwrite").save())
        on = " AND ".join([f"t.{k} = s.{k}" for k in self.keys])
        set_cols = ", ".join([f"t.{c} = s.{c}" for c in df.columns])
        cols = ", ".join(df.columns)
        vals = ", ".join([f"s.{c}" for c in df.columns])
        merge_sql = f"""
            MERGE INTO {self.table} t USING {stage} s ON {on}
            WHEN MATCHED THEN UPDATE SET {set_cols}
            WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals});
        """
        sc = self.spark.sparkContext
        # net.snowflake.spark.snowflake.Utils.runQuery executes SQL inside Snowflake
        sf_utils = sc._jvm.net.snowflake.spark.snowflake.Utils
        java_map = sc._jvm.java.util.HashMap()
        for k, v in self.sf_options.items():
            java_map.put(k, v)
        sf_utils.runQuery(java_map, merge_sql)
        sf_utils.runQuery(java_map, f"DROP TABLE IF EXISTS {stage}")
        logger.info(f"[merge] upserted → Snowflake {self.table} on {self.keys}")

    def run(self, df: DataFrame):
        self.write(df)
        self.job.commit()


if __name__ == "__main__":
    job = SnowflakeLoad()
    # CHANGE_ME: read your consumption data
    df = job.spark.table("insights_consumption_layer.sales_mart")
    job.run(df)

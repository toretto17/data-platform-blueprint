"""
================================================================================
INCREMENTAL LOAD — watermark-based  [AWS Glue + Delta on S3]
================================================================================
Purpose : Load only NEW/CHANGED rows since the last run (high-watermark). Modes:
          append (inserts) or upsert (MERGE on keys). AWS twin of
          databricks/src/de_patterns/incremental_load.py.

Watermark options on AWS:
    • Glue Job Bookmarks  — automatic file-level incremental (set transformation_ctx).
    • Explicit watermark   — store last value in DynamoDB/S3 (shown here, portable).

BOTH apply styles: Spark SQL MERGE (temp view) + DeltaTable API. Append uses
plain Delta append.

Glue params: --datalake-formats delta + Delta spark confs (see other de_patterns).
Customize: SOURCE, TARGET_PATH/TABLE, WATERMARK_COL, KEYS, WRITE_MODE.
Version : 2026-06-28
================================================================================
"""
import sys
import json
import logging
from typing import List, Optional

import boto3
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("incremental_load_aws")


class IncrementalLoadAWS:
    SOURCE_TABLE: str = "bronze_db.events"        # CHANGE_ME (Glue Catalog) or use SOURCE_PATH
    TARGET_TABLE: str = "silver_db.events"        # CHANGE_ME
    TARGET_PATH: str = "s3://CHANGE_ME/silver/events/"
    WATERMARK_COL: str = "updated_at"             # CHANGE_ME
    KEYS: List[str] = ["id"]                      # for upsert mode
    WRITE_MODE: str = "append"                    # append | upsert
    WM_DDB_TABLE: str = "etl_watermarks"          # DynamoDB control table
    REGION: str = "ap-southeast-1"

    def __init__(self):
        self.args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        sc = SparkContext.getOrCreate()
        self.gc = GlueContext(sc)
        self.spark = self.gc.spark_session
        self.spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        self.spark.conf.set("spark.sql.catalog.spark_catalog",
                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        self.job = Job(self.gc)
        self.job.init(self.args["JOB_NAME"], self.args)
        self.ddb = boto3.client("dynamodb", region_name=self.REGION)

    # ---- watermark in DynamoDB ----
    def get_watermark(self) -> Optional[str]:
        try:
            r = self.ddb.get_item(TableName=self.WM_DDB_TABLE,
                                  Key={"table_name": {"S": self.TARGET_TABLE}})
            return r.get("Item", {}).get("watermark", {}).get("S")
        except Exception as e:
            logger.warning(f"watermark read failed ({e}); treating as first run")
            return None

    def set_watermark(self, value: str):
        self.ddb.put_item(TableName=self.WM_DDB_TABLE, Item={
            "table_name": {"S": self.TARGET_TABLE}, "watermark": {"S": str(value)}})
        logger.info(f"watermark set {self.TARGET_TABLE}={value}")

    # ---- read increment ----
    def read_increment(self, wm: Optional[str]) -> DataFrame:
        df = self.spark.table(self.SOURCE_TABLE)
        if wm is not None:
            df = df.filter(F.col(self.WATERMARK_COL) > F.lit(wm))
        logger.info(f"reading rows where {self.WATERMARK_COL} > {wm}")
        return df

    def _ensure_target(self, sample: DataFrame):
        from delta.tables import DeltaTable
        if not DeltaTable.isDeltaTable(self.spark, self.TARGET_PATH):
            sample.limit(0).write.format("delta").mode("overwrite").save(self.TARGET_PATH)
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.TARGET_TABLE} "
                           f"USING DELTA LOCATION '{self.TARGET_PATH}'")

    # ---- write ----
    def write(self, inc: DataFrame):
        self._ensure_target(inc)
        if self.WRITE_MODE == "append":
            inc.write.format("delta").mode("append").save(self.TARGET_PATH)
            logger.info(f"[append] → {self.TARGET_PATH}")
        elif self.WRITE_MODE == "upsert":
            inc.createOrReplaceTempView("v_inc_up")
            on = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
            self.spark.sql(f"""
                MERGE INTO delta.`{self.TARGET_PATH}` t USING v_inc_up s ON {on}
                WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
            logger.info(f"[upsert] MERGE → {self.TARGET_PATH}")
        else:
            raise ValueError("WRITE_MODE must be append|upsert")

    def run(self):
        wm = self.get_watermark()
        inc = self.read_increment(wm)
        if len(inc.head(1)) == 0:
            logger.info("No new rows — nothing to load.")
            self.job.commit()
            return
        new_wm = inc.agg(F.max(self.WATERMARK_COL)).collect()[0][0]
        self.write(inc)
        self.set_watermark(str(new_wm))
        self.job.commit()
        logger.info(f"Incremental load complete. New watermark={new_wm}")


if __name__ == "__main__":
    job = IncrementalLoadAWS()
    job.SOURCE_TABLE = "bronze_db.events"          # CHANGE_ME
    job.TARGET_TABLE = "silver_db.events"
    job.TARGET_PATH = "s3://CHANGE_ME/silver/events/"
    job.WRITE_MODE = "append"
    job.run()

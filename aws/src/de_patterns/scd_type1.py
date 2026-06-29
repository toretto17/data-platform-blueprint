"""
================================================================================
SCD TYPE 1 — Overwrite-in-place (no history)  [AWS Glue + Delta on S3]
================================================================================
Purpose : SCD Type 1 — overwrite the target row when the source changes. Latest
          value only, no history. AWS twin of databricks/src/de_patterns/scd_type1.py.

BOTH styles: Spark SQL MERGE (temp view) + DeltaTable Python API.

Glue setup (job parameters):
    --datalake-formats delta
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

Customize: KEYS, TARGET_TABLE, TARGET_PATH, DEDUP_ORDER_COL.
Version : 2026-06-28
================================================================================
"""
import sys
import logging
from typing import List

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("scd1_aws")


class SCD1AWS:
    KEYS: List[str] = ["id"]                       # CHANGE_ME
    TARGET_TABLE: str = "silver_db.dim_customer"   # CHANGE_ME (Glue Catalog)
    TARGET_PATH: str = "s3://CHANGE_ME/silver/dim_customer/"
    DEDUP_ORDER_COL: str = "updated_at"

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

    def dedup_source(self, src: DataFrame) -> DataFrame:
        w = Window.partitionBy(*self.KEYS).orderBy(F.col(self.DEDUP_ORDER_COL).desc())
        return src.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    def _ensure_target(self, sample: DataFrame):
        from delta.tables import DeltaTable
        if not DeltaTable.isDeltaTable(self.spark, self.TARGET_PATH):
            sample.limit(0).write.format("delta").mode("overwrite").save(self.TARGET_PATH)
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.TARGET_TABLE} "
                           f"USING DELTA LOCATION '{self.TARGET_PATH}'")
            logger.info(f"Created target {self.TARGET_TABLE}")

    def apply_sql(self, src: DataFrame):
        self._ensure_target(src)
        src.createOrReplaceTempView("v_scd1_src")
        on = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
        self.spark.sql(f"""
            MERGE INTO delta.`{self.TARGET_PATH}` t
            USING v_scd1_src s
            ON {on}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(f"[sql] SCD1 MERGE → {self.TARGET_PATH}")

    def apply_delta_api(self, src: DataFrame):
        from delta.tables import DeltaTable
        self._ensure_target(src)
        tgt = DeltaTable.forPath(self.spark, self.TARGET_PATH)
        cond = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
        (tgt.alias("t").merge(src.alias("s"), cond)
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        logger.info(f"[delta-api] SCD1 MERGE → {self.TARGET_PATH}")

    def run(self, src: DataFrame, use_sql: bool = True):
        latest = self.dedup_source(src)
        (self.apply_sql if use_sql else self.apply_delta_api)(latest)
        self.job.commit()


if __name__ == "__main__":
    job = SCD1AWS()
    job.KEYS = ["customer_id"]                          # CHANGE_ME
    job.TARGET_TABLE = "silver_db.dim_customer"
    job.TARGET_PATH = "s3://CHANGE_ME/silver/dim_customer/"
    src = job.spark.read.format("delta").load("s3://CHANGE_ME/bronze/customers/")  # CHANGE_ME
    job.run(src, use_sql=True)

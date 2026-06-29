"""
================================================================================
SCD TYPE 2 — History with effective dates  [AWS Glue + Delta on S3]
================================================================================
Purpose : SCD Type 2 — close the current row + insert a new version when a
          tracked column changes. AWS twin of databricks/src/de_patterns/scd_type2.py.
          Uses the same canonical "two-step MERGE" (mergeKey trick).

BOTH styles: Spark SQL MERGE (temp views) + DeltaTable Python API.

Control columns on target: is_current BOOLEAN, effective_start DATE, effective_end DATE.

Glue setup (job parameters):
    --datalake-formats delta
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

Customize: KEYS, TRACKED_COLS, TARGET_TABLE, TARGET_PATH.
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
logger = logging.getLogger("scd2_aws")


class SCD2AWS:
    KEYS: List[str] = ["id"]                          # CHANGE_ME
    TRACKED_COLS: List[str] = ["name", "address"]     # CHANGE_ME
    TARGET_TABLE: str = "silver_db.dim_customer"
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

    def _dedup(self, src: DataFrame) -> DataFrame:
        w = Window.partitionBy(*self.KEYS).orderBy(F.col(self.DEDUP_ORDER_COL).desc())
        return src.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    def _create_target_if_missing(self, src: DataFrame):
        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(self.spark, self.TARGET_PATH):
            return
        first = (self._dedup(src)
                 .withColumn("is_current", F.lit(True))
                 .withColumn("effective_start", F.current_date())
                 .withColumn("effective_end", F.lit(None).cast("date")))
        first.write.format("delta").mode("overwrite").save(self.TARGET_PATH)
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.TARGET_TABLE} "
                       f"USING DELTA LOCATION '{self.TARGET_PATH}'")
        logger.info(f"Created SCD2 target {self.TARGET_TABLE} (first load)")

    def _change_sql(self, a: str = "t", b: str = "s") -> str:
        # NULL-safe inequality (<=>) so NULL transitions count as changes.
        return " OR ".join([f"NOT ({a}.{c} <=> {b}.{c})" for c in self.TRACKED_COLS])

    def apply_sql(self, src: DataFrame):
        self._create_target_if_missing(src)
        latest = self._dedup(src)
        latest.createOrReplaceTempView("v_scd2_src")
        key_join = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
        change = self._change_sql()

        # Staged set: normal-keyed (close old / matched) + NULL-keyed (insert new) for changed keys.
        self.spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW v_scd2_staged AS
            SELECT s.*, CONCAT_WS('|', {", ".join(f"s.{k}" for k in self.KEYS)}) AS _merge_key
            FROM v_scd2_src s
            UNION ALL
            SELECT s.*, CAST(NULL AS STRING) AS _merge_key
            FROM v_scd2_src s
            JOIN delta.`{self.TARGET_PATH}` t
              ON {key_join} AND t.is_current = true
            WHERE {change}
        """)

        tgt_merge_key = "CONCAT_WS('|', " + ", ".join(f"t.{k}" for k in self.KEYS) + ")"
        biz_cols = latest.columns
        insert_cols = biz_cols + ["is_current", "effective_start", "effective_end"]
        insert_vals = [f"s.{c}" for c in biz_cols] + ["true", "current_date()", "NULL"]
        self.spark.sql(f"""
            MERGE INTO delta.`{self.TARGET_PATH}` t
            USING v_scd2_staged s
            ON {tgt_merge_key} = s._merge_key AND t.is_current = true
            WHEN MATCHED AND ({change}) THEN
                UPDATE SET t.is_current = false, t.effective_end = current_date()
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(insert_cols)}) VALUES ({", ".join(insert_vals)})
        """)
        logger.info(f"[sql] SCD2 two-step MERGE → {self.TARGET_PATH}")

    def apply_delta_api(self, src: DataFrame):
        from delta.tables import DeltaTable
        self._create_target_if_missing(src)
        latest = self._dedup(src)
        tgt = DeltaTable.forPath(self.spark, self.TARGET_PATH)
        current = tgt.toDF().filter(F.col("is_current") == True)  # noqa: E712
        change_expr = F.expr(self._change_sql("c", "s"))
        changed = (latest.alias("s").join(current.alias("c"), self.KEYS)
                   .filter(change_expr).select("s.*"))
        staged = (latest.withColumn("_merge_key", F.concat_ws("|", *[F.col(k) for k in self.KEYS]))
                  .unionByName(changed.withColumn("_merge_key", F.lit(None).cast("string"))))
        cond = "concat_ws('|', " + ",".join([f"t.{k}" for k in self.KEYS]) + ") = s._merge_key AND t.is_current = true"
        biz_cols = latest.columns
        insert_map = {**{c: f"s.{c}" for c in biz_cols},
                      "is_current": "true", "effective_start": "current_date()", "effective_end": "null"}
        (tgt.alias("t").merge(staged.alias("s"), cond)
            .whenMatchedUpdate(condition=self._change_sql(), set={
                "is_current": "false", "effective_end": "current_date()"})
            .whenNotMatchedInsert(values=insert_map)
            .execute())
        logger.info(f"[delta-api] SCD2 two-step MERGE → {self.TARGET_PATH}")

    def run(self, src: DataFrame, use_sql: bool = True):
        (self.apply_sql if use_sql else self.apply_delta_api)(src)
        self.job.commit()


if __name__ == "__main__":
    job = SCD2AWS()
    job.KEYS = ["customer_id"]                          # CHANGE_ME
    job.TRACKED_COLS = ["name", "address", "tier"]      # CHANGE_ME
    job.TARGET_TABLE = "silver_db.dim_customer"
    job.TARGET_PATH = "s3://CHANGE_ME/silver/dim_customer/"
    src = job.spark.read.format("delta").load("s3://CHANGE_ME/bronze/customers/")  # CHANGE_ME
    job.run(src, use_sql=True)

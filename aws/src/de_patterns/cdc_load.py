"""
================================================================================
CDC LOAD — Change Data Capture  [AWS Glue + Delta Lake on S3]
================================================================================
Purpose : Capture row-level changes (insert/update/delete) and apply them to a
          TARGET Delta table on S3 — incrementally, idempotently.

Two common AWS CDC sources are supported here (pick in `read_changes_*`):
    A) AWS DMS CDC files  — DMS writes change files with an `Op` column:
           Op = 'I' (insert), 'U' (update), 'D' (delete)
       Read incrementally with Glue Job Bookmarks (only new files each run).
    B) Delta Change Data Feed on a Glue/Delta source table (Glue 4.0/5.0 support
       Delta via --datalake-formats=delta). Same readChangeFeed API as Databricks.

This file gives BOTH styles for the apply step:
    • Spark SQL MERGE  (create a temp view first, then MERGE INTO)
    • DeltaTable Python API MERGE

Glue setup (job parameters):
    --datalake-formats delta
    --conf  spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
    --conf  spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
    --job-bookmark-option job-bookmark-enable        (for DMS-file incremental reads)

Pattern:
    1. Read changes (DMS files via bookmarks  OR  Delta CDF since last version)
    2. Normalize change type → a common `_op` column (I/U/D)
    3. Collapse to net latest change per key
    4. MERGE into target Delta table (upsert + delete)

Customize (search CHANGE_ME / TODO):
    - KEYS, SOURCE (path/table), TARGET_TABLE / TARGET_PATH, ORDER_COL

Platform notes:
    - AWS Glue 5.x / Spark 3.5 / Delta 3.x on S3.
    - Databricks twin: databricks/src/de_patterns/cdc_load.py
Version : 2026-06-28
================================================================================
"""
import sys
import logging
from typing import List, Optional

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("cdc_load_aws")


class CDCLoadAWS:
    """Apply CDC changes (DMS files or Delta CDF) → TARGET Delta table via MERGE."""

    # ---- CHANGE_ME ----
    KEYS: List[str] = ["id"]
    SOURCE_TYPE: str = "dms"                       # "dms" | "cdf"
    SOURCE_PATH: str = "s3://CHANGE_ME/cdc/dms/customers/"   # DMS change files (for dms)
    SOURCE_TABLE: str = "src_db.customers_cdc"     # Delta source (for cdf)
    TARGET_TABLE: str = "silver_db.customers"      # Glue Catalog target (Delta)
    TARGET_PATH: str = "s3://CHANGE_ME/silver/customers/"
    ORDER_COL: str = "_change_seq"                 # tiebreaker for latest change

    def __init__(self):
        self.args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        sc = SparkContext.getOrCreate()
        self.gc = GlueContext(sc)
        self.spark = self.gc.spark_session
        # Delta + dynamic overwrite
        self.spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        self.spark.conf.set("spark.sql.catalog.spark_catalog",
                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        self.job = Job(self.gc)
        self.job.init(self.args["JOB_NAME"], self.args)

    # ========================================================================
    # 1. READ CHANGES
    # ========================================================================
    def read_changes(self) -> DataFrame:
        if self.SOURCE_TYPE == "dms":
            return self._read_dms_files()
        if self.SOURCE_TYPE == "cdf":
            return self._read_delta_cdf()
        raise SystemExit(f"Unsupported SOURCE_TYPE {self.SOURCE_TYPE} (dms|cdf)")

    def _read_dms_files(self) -> DataFrame:
        """A) Read DMS CDC files incrementally via Glue Job Bookmarks.
        DMS change files carry an `Op` column: I=insert, U=update, D=delete.
        Bookmarks ensure only NEW files are read each run (transformation_ctx)."""
        dyf = self.gc.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [self.SOURCE_PATH], "recurse": True},
            format="parquet",                          # CHANGE_ME if DMS writes csv
            transformation_ctx="dms_cdc_read",          # ← enables bookmark tracking
        )
        df = dyf.toDF()
        # Normalize DMS Op → common _op (I/U/D). DMS full-load rows may have no Op → treat as I.
        df = df.withColumn("_op", F.coalesce(F.col("Op"), F.lit("I")))
        logger.info(f"[dms] read change files from {self.SOURCE_PATH}")
        return df

    def _read_delta_cdf(self, starting_version: int = 0,
                        ending_version: Optional[int] = None) -> DataFrame:
        """B) Read Delta Change Data Feed from a Delta source table (same API as Databricks).
        Requires delta.enableChangeDataFeed=true on the source."""
        reader = (self.spark.read
                  .option("readChangeFeed", "true")
                  .option("startingVersion", starting_version))
        if ending_version is not None:
            reader = reader.option("endingVersion", ending_version)
        df = reader.table(self.SOURCE_TABLE)
        # Normalize CDF _change_type → common _op
        df = (df.filter(F.col("_change_type") != "update_preimage")
                .withColumn("_op", F.when(F.col("_change_type") == "delete", F.lit("D"))
                                    .otherwise(F.lit("U")))   # insert/update_postimage → upsert
                .withColumn(self.ORDER_COL, F.col("_commit_version")))
        logger.info(f"[cdf] read CDF from {self.SOURCE_TABLE} v{starting_version}..{ending_version or 'latest'}")
        return df

    # ========================================================================
    # 2. COLLAPSE TO NET LATEST CHANGE PER KEY
    # ========================================================================
    def latest_change_per_key(self, changes: DataFrame) -> DataFrame:
        """Keep the final state per key using ORDER_COL as the tiebreaker.
        If your DMS files have no sequence column, add one (e.g. ingest order /
        a monotonically increasing id) — CHANGE_ME."""
        order_col = self.ORDER_COL if self.ORDER_COL in changes.columns else None
        if order_col is None:
            changes = changes.withColumn("_change_seq", F.monotonically_increasing_id())
            order_col = "_change_seq"
        w = Window.partitionBy(*self.KEYS).orderBy(F.col(order_col).desc())
        return (changes.withColumn("_rn", F.row_number().over(w))
                       .filter(F.col("_rn") == 1).drop("_rn"))

    # ========================================================================
    # 3. APPLY VIA MERGE — Spark SQL (temp view) + DeltaTable API variants
    # ========================================================================
    def apply_merge_sql(self, latest: DataFrame):
        """Create a temp view, then MERGE INTO the target Delta table.
        Upserts I/U, deletes D."""
        latest.createOrReplaceTempView("v_cdc_latest")
        on = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
        # Ensure target exists (first run): create empty Delta table from schema.
        self._ensure_target(latest)
        self.spark.sql(f"""
            MERGE INTO delta.`{self.TARGET_PATH}` t
            USING v_cdc_latest s
            ON {on}
            WHEN MATCHED AND s._op = 'D' THEN DELETE
            WHEN MATCHED AND s._op IN ('I','U') THEN UPDATE SET *
            WHEN NOT MATCHED AND s._op IN ('I','U') THEN INSERT *
        """)
        logger.info(f"[sql] MERGE applied → {self.TARGET_PATH}")

    def apply_merge_delta_api(self, latest: DataFrame):
        """DeltaTable Python API equivalent of apply_merge_sql."""
        from delta.tables import DeltaTable
        self._ensure_target(latest)
        tgt = DeltaTable.forPath(self.spark, self.TARGET_PATH)
        cond = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
        (tgt.alias("t")
            .merge(latest.alias("s"), cond)
            .whenMatchedDelete(condition="s._op = 'D'")
            .whenMatchedUpdateAll(condition="s._op IN ('I','U')")
            .whenNotMatchedInsertAll(condition="s._op IN ('I','U')")
            .execute())
        logger.info(f"[delta-api] MERGE applied → {self.TARGET_PATH}")

    def _ensure_target(self, sample: DataFrame):
        """Create the target Delta table (empty) on first run so MERGE has a target."""
        from delta.tables import DeltaTable
        if not DeltaTable.isDeltaTable(self.spark, self.TARGET_PATH):
            biz_cols = [c for c in sample.columns if not c.startswith("_") and c != "Op"]
            (sample.select(*biz_cols).limit(0)
                   .write.format("delta").mode("overwrite").save(self.TARGET_PATH))
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.TARGET_TABLE}
                USING DELTA LOCATION '{self.TARGET_PATH}'
            """)
            logger.info(f"Created empty target Delta table {self.TARGET_TABLE}")

    # ========================================================================
    # 4. RUN
    # ========================================================================
    def run(self):
        try:
            changes = self.read_changes()
            if len(changes.head(1)) == 0:
                logger.info("No new changes — nothing to apply.")
                self.job.commit()
                return
            latest = self.latest_change_per_key(changes)
            # Keep only business columns + _op for the MERGE (drop CDF/DMS metadata).
            drop_meta = [c for c in ("_change_type", "_commit_version", "_commit_timestamp", "Op")
                         if c in latest.columns]
            latest = latest.drop(*drop_meta)
            self.apply_merge_sql(latest)
            self.job.commit()                          # commits the bookmark too (DMS mode)
            logger.info("CDC load complete.")
        except Exception as e:
            logger.error(f"CDC load failed: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    job = CDCLoadAWS()
    job.KEYS = ["customer_id"]                          # CHANGE_ME
    job.SOURCE_TYPE = "dms"                             # CHANGE_ME: dms | cdf
    job.SOURCE_PATH = "s3://CHANGE_ME/cdc/dms/customers/"
    job.TARGET_TABLE = "silver_db.customers"
    job.TARGET_PATH = "s3://CHANGE_ME/silver/customers/"
    job.run()

"""
================================================================================
SCD TYPE 1 — Overwrite-in-place (no history)  [Databricks / Delta]
================================================================================
Purpose : Slowly Changing Dimension Type 1. When a source row changes, OVERWRITE
          the existing target row — keep only the latest value, no history.

Use SCD1 when: you only care about the current value (e.g. a corrected typo in a
customer name) and don't need to know what it used to be.

BOTH styles provided:
    • SQL MERGE        (create temp view first → MERGE INTO)
    • DeltaTable API   (whenMatchedUpdateAll / whenNotMatchedInsertAll)

Pattern:
    1. Read the latest source snapshot (one row per key — dedup first if needed).
    2. MERGE into target: update matched keys, insert new keys.

Customize: KEYS, SOURCE (df/table), TARGET_TABLE.

Platform notes: DBR 15.x LTS+, Delta 3.x, UC.
AWS twin: aws/src/de_patterns/scd_type1.py
Version : 2026-06-28
================================================================================
"""
import logging
from typing import List

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger("scd1_databricks")
spark = SparkSession.builder.getOrCreate()


class SCD1Databricks:
    KEYS: List[str] = ["id"]                  # CHANGE_ME
    TARGET_TABLE: str = "main.silver.dim_customer"  # CHANGE_ME
    DEDUP_ORDER_COL: str = "updated_at"       # used to pick latest row per key in source

    # ---- 1. prepare source: one latest row per key ----
    def dedup_source(self, src: DataFrame) -> DataFrame:
        """If the source has multiple rows per key, keep the most recent."""
        w = Window.partitionBy(*self.KEYS).orderBy(F.col(self.DEDUP_ORDER_COL).desc())
        return src.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # ---- 2a. apply via SQL MERGE ----
    def apply_sql(self, src: DataFrame):
        """Create temp view, then MERGE INTO (overwrite matched, insert new)."""
        src.createOrReplaceTempView("v_scd1_src")
        on = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
        spark.sql(f"""
            MERGE INTO {self.TARGET_TABLE} t
            USING v_scd1_src s
            ON {on}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(f"[sql] SCD1 MERGE → {self.TARGET_TABLE}")

    # ---- 2b. apply via DeltaTable API ----
    def apply_delta_api(self, src: DataFrame):
        from delta.tables import DeltaTable
        if not spark.catalog.tableExists(self.TARGET_TABLE):
            src.write.format("delta").saveAsTable(self.TARGET_TABLE)
            logger.info(f"[delta-api] created {self.TARGET_TABLE} (first load)")
            return
        tgt = DeltaTable.forName(spark, self.TARGET_TABLE)
        cond = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
        (tgt.alias("t").merge(src.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        logger.info(f"[delta-api] SCD1 MERGE → {self.TARGET_TABLE}")

    def run(self, src: DataFrame, use_sql: bool = True):
        latest = self.dedup_source(src)
        (self.apply_sql if use_sql else self.apply_delta_api)(latest)


if __name__ == "__main__":
    # Example
    job = SCD1Databricks()
    job.KEYS = ["customer_id"]                       # CHANGE_ME
    job.TARGET_TABLE = "main.silver.dim_customer"    # CHANGE_ME
    src = spark.table("main.bronze.customers")       # CHANGE_ME
    job.run(src, use_sql=True)

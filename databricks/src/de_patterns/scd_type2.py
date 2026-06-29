"""
================================================================================
SCD TYPE 2 — Track full history with effective dates  [Databricks / Delta]
================================================================================
Purpose : Slowly Changing Dimension Type 2. When a tracked attribute changes,
          CLOSE the current row (set end_date, is_current=false) and INSERT a new
          row (is_current=true). You keep the complete history of every version.

Tracked columns drive change detection: if any of them differs from the current
row, a new version is created. Non-tracked columns can update in place (optional).

This uses the canonical Databricks "two-step MERGE" (a.k.a. the mergeKey trick):
    A single MERGE cannot both UPDATE the old row AND INSERT a new row for the
    same source key. So we UNION two record streams:
      1) rows keyed normally  → will CLOSE the current version (UPDATE)
      2) rows keyed as NULL   → will never match → INSERT the new version
    One MERGE then does both.

BOTH styles provided: SQL MERGE (temp view) and DeltaTable API.

Target table extra columns (SCD2 control columns):
    is_current BOOLEAN, effective_start DATE/TIMESTAMP, effective_end DATE/TIMESTAMP

Customize: KEYS, TRACKED_COLS, TARGET_TABLE.

Platform notes: DBR 15.x LTS+, Delta 3.x, UC.
AWS twin: aws/src/de_patterns/scd_type2.py
Version : 2026-06-28
================================================================================
"""
import logging
from typing import List

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger("scd2_databricks")
spark = SparkSession.builder.getOrCreate()


class SCD2Databricks:
    KEYS: List[str] = ["id"]                          # CHANGE_ME business key
    TRACKED_COLS: List[str] = ["name", "address"]     # CHANGE_ME attrs that trigger a new version
    TARGET_TABLE: str = "main.silver.dim_customer"    # CHANGE_ME
    DEDUP_ORDER_COL: str = "updated_at"

    # ---- 0. one-time: create target with SCD2 control columns ----
    def create_target_if_missing_sql(self, src: DataFrame):
        if spark.catalog.tableExists(self.TARGET_TABLE):
            return
        # First load: every row is the current version.
        first = (self._dedup(src)
                 .withColumn("is_current", F.lit(True))
                 .withColumn("effective_start", F.current_date())
                 .withColumn("effective_end", F.lit(None).cast("date")))
        first.write.format("delta").saveAsTable(self.TARGET_TABLE)
        logger.info(f"Created SCD2 target {self.TARGET_TABLE} (first load)")

    def _dedup(self, src: DataFrame) -> DataFrame:
        w = Window.partitionBy(*self.KEYS).orderBy(F.col(self.DEDUP_ORDER_COL).desc())
        return src.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    def _change_condition(self) -> str:
        """SQL boolean: any tracked column differs between source (s) and target (t)."""
        # NULL-safe inequality so NULL→value and value→NULL count as changes.
        return " OR ".join([f"NOT (t.{c} <=> s.{c})" for c in self.TRACKED_COLS])

    # ---- apply via SQL (two-step merge) ----
    def apply_sql(self, src: DataFrame):
        self.create_target_if_missing_sql(src)
        latest = self._dedup(src)
        latest.createOrReplaceTempView("v_scd2_src")
        key_join = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
        change = self._change_condition()

        # Build the staged set: normal-keyed (close old) + NULL-keyed (insert new) for changed rows.
        key_eq_cols = ", ".join(self.KEYS)
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW v_scd2_staged AS
            -- 1) rows that will CLOSE the current version (and also serve as the matched update)
            SELECT s.*, CONCAT_WS('|', {", ".join(f"s.{k}" for k in self.KEYS)}) AS _merge_key
            FROM v_scd2_src s
            UNION ALL
            -- 2) NULL merge key → guaranteed NOT MATCHED → INSERT the new version,
            --    but ONLY for keys whose tracked columns actually changed
            SELECT s.*, CAST(NULL AS STRING) AS _merge_key
            FROM v_scd2_src s
            JOIN {self.TARGET_TABLE} t
              ON {key_join} AND t.is_current = true
            WHERE {change}
        """)

        # The MERGE: match on the composite merge key vs current rows.
        tgt_merge_key = "CONCAT_WS('|', " + ", ".join(f"t.{k}" for k in self.KEYS) + ")"
        biz_cols = [c for c in latest.columns]
        insert_cols = biz_cols + ["is_current", "effective_start", "effective_end"]
        insert_vals = [f"s.{c}" for c in biz_cols] + ["true", "current_date()", "NULL"]
        spark.sql(f"""
            MERGE INTO {self.TARGET_TABLE} t
            USING v_scd2_staged s
            ON {tgt_merge_key} = s._merge_key AND t.is_current = true
            WHEN MATCHED AND ({change}) THEN
                UPDATE SET t.is_current = false, t.effective_end = current_date()
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(insert_cols)}) VALUES ({", ".join(insert_vals)})
        """)
        logger.info(f"[sql] SCD2 two-step MERGE → {self.TARGET_TABLE}")

    # ---- apply via DeltaTable API (same two-step logic) ----
    def apply_delta_api(self, src: DataFrame):
        from delta.tables import DeltaTable
        self.create_target_if_missing_sql(src)
        latest = self._dedup(src)
        tgt = DeltaTable.forName(spark, self.TARGET_TABLE)
        current = tgt.toDF().filter(F.col("is_current") == True)  # noqa: E712

        change_expr = F.expr(" OR ".join([f"NOT (c.{c} <=> s.{c})" for c in self.TRACKED_COLS]))
        # rows whose tracked cols changed vs current
        changed = (latest.alias("s").join(current.alias("c"), self.KEYS)
                   .filter(change_expr).select("s.*"))

        staged = (latest.withColumn("_merge_key", F.concat_ws("|", *[F.col(k) for k in self.KEYS]))
                  .unionByName(changed.withColumn("_merge_key", F.lit(None).cast("string"))))

        cond = "concat_ws('|', " + ",".join([f"t.{k}" for k in self.KEYS]) + ") = s._merge_key AND t.is_current = true"
        biz_cols = latest.columns
        insert_map = {**{c: f"s.{c}" for c in biz_cols},
                      "is_current": "true", "effective_start": "current_date()", "effective_end": "null"}
        change_sql = " OR ".join([f"NOT (t.{c} <=> s.{c})" for c in self.TRACKED_COLS])
        (tgt.alias("t").merge(staged.alias("s"), cond)
            .whenMatchedUpdate(condition=change_sql,
                               set={"is_current": "false", "effective_end": "current_date()"})
            .whenNotMatchedInsert(values=insert_map)
            .execute())
        logger.info(f"[delta-api] SCD2 two-step MERGE → {self.TARGET_TABLE}")

    def run(self, src: DataFrame, use_sql: bool = True):
        (self.apply_sql if use_sql else self.apply_delta_api)(src)


if __name__ == "__main__":
    job = SCD2Databricks()
    job.KEYS = ["customer_id"]                        # CHANGE_ME
    job.TRACKED_COLS = ["name", "address", "tier"]    # CHANGE_ME
    job.TARGET_TABLE = "main.silver.dim_customer"     # CHANGE_ME
    src = spark.table("main.bronze.customers")        # CHANGE_ME
    job.run(src, use_sql=True)

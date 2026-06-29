"""
================================================================================
CDC LOAD — Change Data Capture via Delta Change Data Feed (CDF)  [Databricks]
================================================================================
Purpose : Capture row-level changes (insert/update/delete) from a SOURCE Delta
          table and apply them to a TARGET table — incrementally, idempotently.

This file gives you BOTH styles for every step:
    • PySpark DataFrame API   (functions ending _pyspark)
    • SQL                     (functions ending _sql; create temp views first)

Delta CDF facts (verified against Databricks docs, 2026):
    • Enable on a table:  TBLPROPERTIES (delta.enableChangeDataFeed = true)
    • Batch read:         spark.read.option("readChangeFeed","true")
                               .option("startingVersion", v)         # or startingTimestamp
                               .option("endingVersion", v)           # optional, inclusive
                               .table("<tbl>")
    • SQL batch read:     SELECT * FROM table_changes('<tbl>', <start>[, <end>])
    • Streaming read:     spark.readStream.option("readChangeFeed","true").table("<tbl>")
    • CDF metadata cols:  _change_type  ∈ {insert, update_preimage, update_postimage, delete}
                          _commit_version (long), _commit_timestamp (timestamp)
    • Start/end versions are INCLUSIVE. startingVersion required for batch.

Pattern:
    1. Ensure CDF is enabled on the SOURCE table (one-time).
    2. Read changes since the last processed version (batch) OR stream with checkpoint.
    3. Collapse to the NET latest change per key (drop update_preimage; keep last postimage/delete).
    4. MERGE into target: upsert inserts/updates, delete deletes.
    5. Persist the new "last processed version" watermark (batch mode).

Customize (search CHANGE_ME / TODO):
    - KEYS              : business primary key column(s)
    - SOURCE_TABLE      : the CDF-enabled source Delta table (UC 3-level name)
    - TARGET_TABLE      : the table you maintain
    - ORDER_COL         : tiebreaker for "latest change" (default _commit_version)

Platform notes:
    - Databricks Runtime 15.x LTS+, Delta 3.x, Unity Catalog.
    - AWS twin: aws/src/de_patterns/cdc_load.py (Glue + Delta MERGE / DMS CDC files).
Version : 2026-06-28
================================================================================
"""
import logging
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger("cdc_load_databricks")
spark = SparkSession.builder.getOrCreate()


class CDCLoadDatabricks:
    """Apply CDF changes from SOURCE_TABLE → TARGET_TABLE via Delta MERGE."""

    # ---- CHANGE_ME: configure for your tables ----
    KEYS: List[str] = ["id"]                     # business primary key(s)
    SOURCE_TABLE: str = "main.bronze.cdc_source"  # CDF-enabled source
    TARGET_TABLE: str = "main.silver.cdc_target"  # table you maintain
    ORDER_COL: str = "_commit_version"            # latest-change tiebreaker

    # ========================================================================
    # 0. ENABLE CDF ON SOURCE (one-time)
    # ========================================================================
    def enable_cdf_sql(self):
        """Turn on Change Data Feed for the source table (idempotent)."""
        spark.sql(f"""
            ALTER TABLE {self.SOURCE_TABLE}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
        logger.info(f"CDF enabled on {self.SOURCE_TABLE}")

    # ========================================================================
    # 1. READ CHANGES — batch (PySpark + SQL variants)
    # ========================================================================
    def read_changes_pyspark(self, starting_version: int, ending_version: Optional[int] = None) -> DataFrame:
        """Batch-read CDF rows from starting_version (inclusive) → ending_version (inclusive)."""
        reader = (spark.read
                  .option("readChangeFeed", "true")
                  .option("startingVersion", starting_version))
        if ending_version is not None:
            reader = reader.option("endingVersion", ending_version)
        df = reader.table(self.SOURCE_TABLE)
        logger.info(f"[pyspark] read CDF {self.SOURCE_TABLE} v{starting_version}..{ending_version or 'latest'}")
        return df

    def read_changes_sql(self, starting_version: int, ending_version: Optional[int] = None) -> DataFrame:
        """Same as above using the table_changes() SQL TVF.
        SQL step: build the change set as a temp view you can inspect/query."""
        end = f", {ending_version}" if ending_version is not None else ""
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW v_changes AS
            SELECT * FROM table_changes('{self.SOURCE_TABLE}', {starting_version}{end})
        """)
        logger.info(f"[sql] CREATE TEMP VIEW v_changes via table_changes('{self.SOURCE_TABLE}', {starting_version}{end})")
        return spark.table("v_changes")

    # ========================================================================
    # 2. COLLAPSE TO NET LATEST CHANGE PER KEY
    # ========================================================================
    def latest_change_per_key_pyspark(self, changes: DataFrame) -> DataFrame:
        """Keep only the final state per key:
        - drop 'update_preimage' (the before-image, not needed for apply)
        - among insert/update_postimage/delete, keep the highest _commit_version."""
        relevant = changes.filter(F.col("_change_type") != "update_preimage")
        w = Window.partitionBy(*self.KEYS).orderBy(
            F.col(self.ORDER_COL).desc(), F.col("_commit_timestamp").desc())
        return (relevant
                .withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn"))

    def latest_change_per_key_sql(self) -> DataFrame:
        """SQL variant. Requires temp view v_changes (from read_changes_sql)."""
        keys = ", ".join(self.KEYS)
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW v_latest AS
            SELECT * EXCEPT(_rn) FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY {keys}
                           ORDER BY {self.ORDER_COL} DESC, _commit_timestamp DESC
                       ) AS _rn
                FROM v_changes
                WHERE _change_type <> 'update_preimage'
            ) WHERE _rn = 1
        """)
        logger.info("[sql] CREATE TEMP VIEW v_latest (net latest change per key)")
        return spark.table("v_latest")

    # ========================================================================
    # 3. APPLY VIA MERGE (PySpark API + SQL variants)
    # ========================================================================
    def apply_merge_sql(self, staged_view: str = "v_latest"):
        """Upsert + delete in one MERGE. `staged_view` must hold the net latest change
        per key (with _change_type). Inserts/updates upsert; deletes remove."""
        on = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
        spark.sql(f"""
            MERGE INTO {self.TARGET_TABLE} t
            USING {staged_view} s
            ON {on}
            WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
            WHEN MATCHED AND s._change_type IN ('insert','update_postimage') THEN UPDATE SET *
            WHEN NOT MATCHED AND s._change_type IN ('insert','update_postimage') THEN INSERT *
        """)
        logger.info(f"[sql] MERGE applied → {self.TARGET_TABLE}")

    def apply_merge_pyspark(self, latest: DataFrame):
        """PySpark DeltaTable MERGE equivalent of apply_merge_sql."""
        from delta.tables import DeltaTable
        # Drop CDF metadata except _change_type before writing business columns.
        biz_cols = [c for c in latest.columns if c not in ("_commit_version", "_commit_timestamp")]
        staged = latest.select(*biz_cols)
        tgt = DeltaTable.forName(spark, self.TARGET_TABLE)
        cond = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
        (tgt.alias("t")
            .merge(staged.alias("s"), cond)
            .whenMatchedDelete(condition="s._change_type = 'delete'")
            .whenMatchedUpdateAll(condition="s._change_type IN ('insert','update_postimage')")
            .whenNotMatchedInsertAll(condition="s._change_type IN ('insert','update_postimage')")
            .execute())
        logger.info(f"[pyspark] MERGE applied → {self.TARGET_TABLE}")

    # ========================================================================
    # 4a. BATCH ORCHESTRATION (with version watermark)
    # ========================================================================
    def run_batch(self, last_processed_version: Optional[int]):
        """Process all changes committed after last_processed_version.
        Returns the new watermark (max _commit_version seen)."""
        start = (last_processed_version + 1) if last_processed_version is not None else 0
        changes = self.read_changes_pyspark(starting_version=start)
        if len(changes.head(1)) == 0:
            logger.info("No new CDF changes — nothing to apply.")
            return last_processed_version
        latest = self.latest_change_per_key_pyspark(changes)
        # Register as a temp view so we can use the SQL MERGE (or call apply_merge_pyspark).
        latest.createOrReplaceTempView("v_latest")
        self.apply_merge_sql("v_latest")
        new_wm = changes.agg(F.max("_commit_version")).collect()[0][0]
        logger.info(f"CDC batch complete. New watermark version = {new_wm}")
        return new_wm

    # ========================================================================
    # 4b. STREAMING ORCHESTRATION (checkpoint = automatic version tracking)
    # ========================================================================
    def run_streaming(self, checkpoint_path: str):
        """Incrementally process CDF with Structured Streaming. The checkpoint
        tracks the processed version automatically (no manual watermark needed).
        Uses trigger(availableNow=True) to drain all available changes then stop."""
        changes = (spark.readStream
                   .option("readChangeFeed", "true")
                   .table(self.SOURCE_TABLE))

        def _apply_batch(micro_df: DataFrame, batch_id: int):
            if len(micro_df.head(1)) == 0:
                return
            latest = self.latest_change_per_key_pyspark(micro_df)
            latest.createOrReplaceTempView("v_latest_stream")
            self.apply_merge_sql("v_latest_stream")
            logger.info(f"[stream] batch {batch_id} applied")

        (changes.writeStream
            .foreachBatch(_apply_batch)
            .option("checkpointLocation", checkpoint_path)   # CHANGE_ME: durable checkpoint
            .trigger(availableNow=True)
            .start()
            .awaitTermination())
        logger.info("CDC streaming run complete.")


# ============================================================================
# EXAMPLE USAGE (delete / adapt)
# ============================================================================
if __name__ == "__main__":
    job = CDCLoadDatabricks()
    job.KEYS = ["customer_id"]                      # CHANGE_ME
    job.SOURCE_TABLE = "main.bronze.customers_cdc"  # CHANGE_ME
    job.TARGET_TABLE = "main.silver.customers"      # CHANGE_ME

    # One-time: enable CDF on the source
    job.enable_cdf_sql()

    # Option A — batch (manage watermark yourself, e.g. via MetadataFreshnessManager)
    # new_version = job.run_batch(last_processed_version=None)

    # Option B — streaming (checkpoint tracks version automatically; recommended)
    job.run_streaming(checkpoint_path="s3://CHANGE_ME/_checkpoints/customers_cdc")

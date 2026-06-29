"""
================================================================================
ETL UTILITIES — [Databricks / Delta / Unity Catalog]
================================================================================
Purpose : Databricks twin of aws/src/common/utils/etl_utils.py. Same building
          blocks (early-exit, freshness, write strategies) but implemented with
          Delta + Unity Catalog instead of Glue Catalog + Parquet.

Contents:
    - EarlyExitCheck       : O(1) emptiness check (never use .count() for this)
    - MetadataFreshnessManager : skip reprocessing using a Delta marker table
    - DeltaWriter          : write strategies (append, dynamic overwrite, merge/upsert)
    - get_writer(strategy) : factory matching the AWS API

Keep the public API identical to the AWS module so job code is portable.
Version : 2026-06-28
================================================================================
"""
import logging
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("etl_utils_databricks")
spark = SparkSession.builder.getOrCreate()


# ============================================================================
# EARLY EXIT — O(1), never .count()
# ============================================================================
class EarlyExitCheck:
    @staticmethod
    def is_empty(df: DataFrame) -> bool:
        """True if the DataFrame has no rows. Stops at first row (cheap)."""
        return len(df.head(1)) == 0


# ============================================================================
# FRESHNESS — skip reprocessing using a small Delta marker table
# ============================================================================
class MetadataFreshnessManager:
    """
    Tracks the last processed watermark in a Delta marker table so daily jobs
    can skip when there's nothing new.

    marker table schema: (job_name STRING, watermark STRING, updated_ts TIMESTAMP)
    """

    def __init__(self, marker_table: str):
        self.marker_table = marker_table          # e.g. "main.ops.etl_watermarks"  CHANGE_ME
        self._ensure_table()

    def _ensure_table(self):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.marker_table} (
                job_name STRING, watermark STRING, updated_ts TIMESTAMP
            ) USING DELTA
        """)

    def get_watermark(self, job_name: str) -> Optional[str]:
        row = (spark.table(self.marker_table)
               .filter(F.col("job_name") == job_name)
               .orderBy(F.col("updated_ts").desc())
               .limit(1).collect())
        return row[0]["watermark"] if row else None

    def set_watermark(self, job_name: str, watermark: str):
        spark.sql(f"DELETE FROM {self.marker_table} WHERE job_name = '{job_name}'")
        (spark.createDataFrame([(job_name, str(watermark))], ["job_name", "watermark"])
              .withColumn("updated_ts", F.current_timestamp())
              .write.mode("append").saveAsTable(self.marker_table))
        logger.info(f"watermark set: {job_name}={watermark}")

    def is_fresh(self, job_name: str, source_max: str) -> bool:
        """True if we've already processed up to source_max (skip)."""
        last = self.get_watermark(job_name)
        return bool(last and source_max and last >= source_max)


# ============================================================================
# WRITE STRATEGIES (Delta)
# ============================================================================
class DeltaWriter:
    """Pluggable Delta write strategies. `table` is a UC 3-level name."""

    def write(self, df: DataFrame, table: str, *, partition_col: Optional[str] = None,
              mode: str = "append", merge_keys: Optional[List[str]] = None, **_):
        if mode == "merge":
            return self._merge(df, table, merge_keys, partition_col)
        return self._write_native(df, table, partition_col, mode)

    def _write_native(self, df, table, partition_col, mode):
        """append OR dynamic-partition overwrite."""
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        w = df.write.format("delta").mode(mode).option("mergeSchema", "true")
        if partition_col:
            w = w.partitionBy(partition_col)
        w.saveAsTable(table)
        logger.info(f"Delta {mode} write → {table}")

    def _merge(self, df, table, merge_keys, partition_col):
        """Upsert via Delta MERGE. Creates table on first run."""
        if not merge_keys:
            raise ValueError("merge mode requires merge_keys")
        if not spark.catalog.tableExists(table):
            self._write_native(df, table, partition_col, "append")
            return
        df.createOrReplaceTempView("_src")
        on = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        spark.sql(f"""
            MERGE INTO {table} t
            USING _src s
            ON {on}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(f"Delta MERGE upsert → {table} on {merge_keys}")


def get_writer(strategy: str = "delta") -> DeltaWriter:
    """Factory — matches the AWS get_writer() API. Only 'delta' on Databricks."""
    return DeltaWriter()

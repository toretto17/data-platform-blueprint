"""
================================================================================
FRESHNESS MANAGER — [Databricks]
================================================================================
Purpose: Twin of aws/src/common/metadata/freshness.py. Decide whether to run,
         comparing source vs target freshness. Uses Delta table history and a
         Delta watermark table (no S3 markers needed on the Lakehouse).

Checks:
    • max_partition()        : max partition value of a table
    • last_commit_timestamp(): when a Delta table was last written (DESCRIBE HISTORY)
    • watermark table        : persisted last-processed value per target

Verified API: DeltaTable.forName(spark, t).history() → DataFrame with
              version, timestamp, operation (latest row = most recent commit).

Usage:
    fm = FreshnessManager()
    src_max = fm.max_partition("main.bronze.sales", "mnth_id")
    if fm.is_current("main.silver.sales", src_max):
        return  # skip
    ...do work...
    fm.set_watermark("main.silver.sales", src_max)
Version : 2026-06-28
================================================================================
"""
import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("freshness_databricks")
spark = SparkSession.builder.getOrCreate()


class FreshnessManager:
    def __init__(self, watermark_table: str = "main.ops.watermarks"):
        self.watermark_table = watermark_table
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.watermark_table}
                      (table_name STRING, watermark STRING, updated_ts TIMESTAMP) USING DELTA""")

    def max_partition(self, table: str, partition_col: str) -> Optional[str]:
        v = spark.table(table).agg(F.max(partition_col)).collect()[0][0]
        return str(v) if v is not None else None

    def last_commit_timestamp(self, table: str):
        """Timestamp of the most recent write to a Delta table (via history())."""
        from delta.tables import DeltaTable
        hist = DeltaTable.forName(spark, table).history(1)   # latest 1 commit
        row = hist.select("timestamp").collect()
        return row[0]["timestamp"] if row else None

    # ---- watermark table ----
    def get_watermark(self, target: str) -> Optional[str]:
        r = (spark.table(self.watermark_table).filter(F.col("table_name") == target)
             .orderBy(F.col("updated_ts").desc()).limit(1).collect())
        return r[0]["watermark"] if r else None

    def set_watermark(self, target: str, value: str):
        spark.sql(f"DELETE FROM {self.watermark_table} WHERE table_name = '{target}'")
        (spark.createDataFrame([(target, str(value))], ["table_name", "watermark"])
         .withColumn("updated_ts", F.current_timestamp())
         .write.mode("append").saveAsTable(self.watermark_table))
        logger.info(f"watermark {target} = {value}")

    def is_current(self, target: str, source_max: Optional[str]) -> bool:
        last = self.get_watermark(target)
        return bool(last and source_max and last >= source_max)

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


class DataOptimizer:
    """
    In-code decision helpers for FILE SIZING and SKEW (salting).

    PUBLIC API IS IDENTICAL to aws/src/common/utils/etl_utils.DataOptimizer so job
    code stays portable across trees. Only the file-sizing DEFAULT differs by platform:
    on Databricks/Delta, file sizing is handled by Auto Optimize
    (delta.autoOptimize.optimizeWrite + autoCompact) and OPTIMIZE/ZORDER, so
    right_size_output() is a no-op for delta/databricks and you should NOT shuffle
    before write. The skew/salting helpers are the same plain-PySpark logic.

    See docs/architecture/PARTITIONING_FILE_SIZING_AND_TABLE_FORMATS.md (§3, §4).

    Golden numbers:
        target file size    : 256 MB (accept 128 MB – 1 GB)
        skew_ratio          : > 3   → significant skew
        null_pct on key     : > 80% → treat as skew (salt or filter-and-union)
    """

    TARGET_FILE_BYTES = 256 * 1024 * 1024      # 256 MB
    SKEW_RATIO_THRESHOLD = 3.0
    NULL_PCT_THRESHOLD = 80.0

    # ------------------------------------------------------------------ #
    # FILE SIZING                                                         #
    # ------------------------------------------------------------------ #
    @staticmethod
    def right_size_output(df: DataFrame,
                          target_file_bytes: int = TARGET_FILE_BYTES,
                          avg_row_bytes: Optional[int] = None,
                          row_count: Optional[int] = None,
                          platform: str = "databricks") -> DataFrame:
        """
        Resize output partitions to hit ~target_file_bytes per file.

        On Databricks/Delta this is a NO-OP by design — Auto Optimize (optimizeWrite +
        autoCompact) sizes files on write and OPTIMIZE/ZORDER compacts afterwards.
        The signature matches the AWS twin so the same job code works in both trees.

        DECISION (only when platform is spark_native/glue_catalog):
            target_files = ceil(total_bytes / target_file_bytes)
            if current > target_files*2 : coalesce(target_files)    # shrink, no shuffle
            elif current < target_files  : repartition(target_files) # grow, shuffle
            else                         : leave as-is
        """
        if platform in ("delta", "iceberg", "databricks"):
            # Table formats size files via table properties + compaction (Auto Optimize).
            return df

        current = max(1, df.rdd.getNumPartitions())
        if row_count is not None:
            rb = avg_row_bytes if avg_row_bytes else 200
            total_bytes = max(1, row_count * rb)
            target_files = max(1, -(-total_bytes // target_file_bytes))  # ceil div
        else:
            target_files = max(1, current // 4)

        if current > target_files * 2:
            return df.coalesce(int(target_files))
        if current < target_files:
            return df.repartition(int(target_files))
        return df

    # ------------------------------------------------------------------ #
    # SKEW DETECTION                                                      #
    # ------------------------------------------------------------------ #
    @classmethod
    def detect_skew(cls, df: DataFrame, key_cols: List[str]) -> dict:
        """
        Profile skew on key_cols. Returns:
            {skew_ratio, null_pct, is_skewed, recommend_salt, reason}
        Triggers a shuffle+aggregation — run during a tuning pass, cache the verdict.
        """
        key = key_cols[0] if len(key_cols) == 1 else F.concat_ws("|", *key_cols)
        counts = df.groupBy(key.alias("_k") if hasattr(key, "alias") else key).count()
        stats = counts.agg(F.max("count").alias("mx"), F.avg("count").alias("av")).head(1)[0]
        mx, av = (stats["mx"] or 0), (stats["av"] or 1)
        skew_ratio = float(mx) / float(av) if av else 0.0

        total = df.count()
        null_cond = F.lit(False)
        for c in key_cols:
            null_cond = null_cond | F.col(c).isNull()
        null_rows = df.filter(null_cond).count()
        null_pct = (null_rows * 100.0 / total) if total else 0.0

        is_skewed = skew_ratio > cls.SKEW_RATIO_THRESHOLD or null_pct > cls.NULL_PCT_THRESHOLD
        return {
            "skew_ratio": round(skew_ratio, 2),
            "null_pct": round(null_pct, 2),
            "is_skewed": is_skewed,
            "recommend_salt": skew_ratio > cls.SKEW_RATIO_THRESHOLD or null_pct > cls.NULL_PCT_THRESHOLD,
            "reason": (
                f"skew_ratio={skew_ratio:.1f} (>{cls.SKEW_RATIO_THRESHOLD}) "
                f"or null_pct={null_pct:.1f}% (>{cls.NULL_PCT_THRESHOLD}%)"
                if is_skewed else "within thresholds — rely on AQE skewJoin"
            ),
        }

    # ------------------------------------------------------------------ #
    # SALTING                                                             #
    # ------------------------------------------------------------------ #
    @staticmethod
    def salt_join(large: DataFrame, small: DataFrame, join_key: str,
                  salt_n: int = 16, how: str = "inner") -> DataFrame:
        """
        Salted join for a skewed large side. Prefer AQE skewJoin FIRST; salt only when
        detect_skew().recommend_salt is True.
        """
        large_s = large.withColumn("_salt", (F.rand() * salt_n).cast("int"))
        small_s = small.withColumn(
            "_salt", F.explode(F.array([F.lit(i) for i in range(salt_n)]))
        )
        return large_s.join(small_s, on=[join_key, "_salt"], how=how).drop("_salt")

    @staticmethod
    def salt_aggregate(df: DataFrame, group_cols: List[str],
                       agg_col: str, agg_fn: str = "sum",
                       salt_n: int = 16) -> DataFrame:
        """
        Two-stage salted aggregation for a skewed group key.
        Only SUM/COUNT/MIN/MAX are safe to two-stage (associative). NOT for AVG/median.
        """
        fn = getattr(F, agg_fn)
        salted = df.withColumn("_salt", (F.rand() * salt_n).cast("int"))
        stage1 = salted.groupBy(*group_cols, "_salt").agg(fn(agg_col).alias("_partial"))
        return stage1.groupBy(*group_cols).agg(fn("_partial").alias(agg_col))

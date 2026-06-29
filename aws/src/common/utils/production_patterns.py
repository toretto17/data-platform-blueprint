"""
================================================================================
PRODUCTION PATTERNS — Common patterns from real-world data pipelines
================================================================================
Patterns:
    1. Merge with existing data (incremental append + partition overwrite)
    2. Cache/Unpersist (memory management for multi-use DataFrames)
    3. ROW_NUMBER dedup (latest record per key — PIT queries)
    4. Retry for flaky catalog operations (MSCK REPAIR, Glue API)
    5. Optional argument parsing (DDB may not have all args)
    6. Structured JSON logging with correlation ID
    7. History/Backfill mode (same job, different behavior)
================================================================================
"""
import sys
import time
import json
import uuid
import logging
from typing import List, Optional, Callable
from functools import wraps

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

logger = logging.getLogger("production_patterns")


# ============================================================================
# 1. MERGE WITH EXISTING — Incremental append without duplicates
# ============================================================================
def merge_with_existing(spark: SparkSession, new_df: DataFrame, target_path: str,
                        partition_col: str, affected_partitions: List) -> DataFrame:
    """
    Merge new data with existing target. Pattern:
        - Read existing data EXCLUDING partitions being rewritten
        - Union with new data
        - Write back (dynamic partition overwrite handles the rest)

    Why not just overwrite?
        - If new_df only has data for 2 months, overwriting would DELETE all other months.
        - This pattern preserves historical data while updating specific partitions.

    Usage:
        affected = [202606, 202605]  # months being reprocessed
        final = merge_with_existing(spark, new_df, target_path, "mnth_id", affected)
        final.write.mode("overwrite").partitionBy("mnth_id").parquet(target_path)

    Platform equivalents:
        - Delta: MERGE INTO (built-in, no manual merge needed)
        - Iceberg: overwritePartitions() (atomic per-partition replace)
        - Databricks: .mode("overwrite").option("replaceWhere", "mnth_id IN (...)") 
        - Spark/Glue: This manual pattern below
    """
    try:
        existing = spark.read.parquet(target_path)
        # Keep only partitions NOT being overwritten
        keep_condition = ~F.col(partition_col).isin(affected_partitions)
        preserved = existing.filter(keep_condition)
        merged = preserved.unionByName(new_df, allowMissingColumns=True)
        logger.info(f"Merged: preserved {preserved.rdd.getNumPartitions()} partitions + new data")
        return merged
    except Exception:
        logger.info("No existing data (first run) — returning new data only")
        return new_df


# ============================================================================
# 2. CACHE / UNPERSIST — Memory management for reused DataFrames
# ============================================================================
def with_cache(df: DataFrame, action: Callable[[DataFrame], any]) -> any:
    """
    Cache a DataFrame, perform action(s), then unpersist.
    Prevents recomputation when a DF is used multiple times.

    Usage:
        result = with_cache(expensive_df, lambda df: (
            df.groupBy("site").count(),  # first use
            df.filter(...).write(...)     # second use — no recompute
        ))

    When to cache:
        ✅ DataFrame used 2+ times downstream (joins, writes, counts)
        ✅ Expensive computation (many joins, window functions)
        ❌ DataFrame used only once (cache adds overhead)
        ❌ DataFrame larger than available memory (spill to disk = slow)

    Platform notes:
        - Databricks: Photon auto-caches intermediate results (less manual caching needed)
        - Delta cache: CACHE SELECT * FROM table (disk-based, survives job restart)
        - Spark: .cache() = MEMORY_AND_DISK by default
    """
    df.cache()
    try:
        # Materialize cache (trigger computation)
        df.count()  # Exception: we use .count() HERE because caching needs materialization
        result = action(df)
        return result
    finally:
        df.unpersist()


# ============================================================================
# 3. ROW_NUMBER DEDUP — Get latest record per key (PIT queries)
# ============================================================================
def dedup_latest(df: DataFrame, partition_keys: List[str],
                 order_cols: List[str], descending: bool = True) -> DataFrame:
    """
    Keep only the latest row per key using ROW_NUMBER window.

    Common uses:
        - SCD Type 1: keep latest version of each entity
        - Feature Store PIT: latest feature values per record_id
        - Dedup source data: remove duplicates from CDC feed

    Args:
        partition_keys: columns forming the unique entity (e.g., ["customer_id"])
        order_cols: columns to determine "latest" (e.g., ["event_time", "write_time"])
        descending: True = keep row with MAX order_cols (latest)

    Usage:
        # Keep latest record per customer
        clean = dedup_latest(raw_df, ["customer_id"], ["updated_at"])

        # Feature Store PIT dedup
        fs_clean = dedup_latest(fs_df, ["record_id"], ["event_time", "write_time"])

    Platform equivalents:
        - Delta: MERGE with dedup in source CTE
        - Iceberg: merge-on-read handles this for time-travel queries
        - Databricks: Delta Change Data Feed (CDF) for CDC streams
        - SQL: standard ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ... DESC)
    """
    order_exprs = [F.col(c).desc() if descending else F.col(c).asc() for c in order_cols]
    window = Window.partitionBy(*partition_keys).orderBy(*order_exprs)
    return (df
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn"))


# ============================================================================
# 4. RETRY — For flaky catalog/API operations
# ============================================================================
def retry_operation(func: Callable, max_retries: int = 3, delay_seconds: float = 2.0,
                    description: str = "operation") -> any:
    """
    Retry a flaky operation with exponential backoff.

    Common flaky operations:
        - MSCK REPAIR TABLE (Glue catalog eventual consistency)
        - Glue API calls (throttling)
        - S3 eventual consistency (rare in 2024+ but exists for overwrites)
        - SageMaker API (rate limits)

    Usage:
        retry_operation(
            lambda: spark.sql("MSCK REPAIR TABLE db.my_table"),
            max_retries=3, description="MSCK REPAIR"
        )

    Platform notes:
        - Delta/Iceberg: no MSCK needed (atomic commits)
        - Databricks: Unity Catalog is strongly consistent (no retries needed)
        - AWS Glue: Glue Catalog is eventually consistent for partitions
    """
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"{description} failed after {max_retries} retries: {e}")
                raise
            wait = delay_seconds * (2 ** attempt)
            logger.warning(f"{description} attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)


# ============================================================================
# 5. OPTIONAL ARGUMENT PARSING — DDB config may not have all args
# ============================================================================
def parse_args_safe(argv: list, required: List[str], optional: List[str] = None) -> dict:
    """
    Parse Glue job arguments with optional fields that may not exist.
    getResolvedOptions crashes if an arg is missing. This handles gracefully.

    Usage:
        args = parse_args_safe(sys.argv,
            required=["JOB_NAME", "TARGET_BUCKET", "TARGET_DATABASE"],
            optional=["LOOKBACK_DAYS", "FORCE_RUN", "INITIAL_LOAD", "data_date"]
        )
        lookback = int(args.get("LOOKBACK_DAYS", "60"))

    Platform equivalents:
        - Databricks: dbutils.widgets.get("param") with try/except or getArgument()
        - Airflow: **kwargs from DAG context, with .get() defaults
        - Open-source Spark: argparse or spark-submit --conf
    """
    from awsglue.utils import getResolvedOptions

    # Parse required args (will raise if missing — that's intentional)
    args = getResolvedOptions(argv, required)

    # Parse optional args (silently skip if missing)
    for arg in (optional or []):
        for i, a in enumerate(argv):
            if a == f"--{arg}" and i + 1 < len(argv):
                args[arg] = argv[i + 1]
                break

    return args


# ============================================================================
# 6. STRUCTURED LOGGING — JSON format with correlation ID
# ============================================================================
class StructuredLogger:
    """
    Production-grade structured logging.
    Outputs JSON for CloudWatch Insights / Datadog / Splunk / Databricks Log Analytics.

    Usage:
        log = StructuredLogger("my_job", correlation_id="exec-123")
        log.info("Processing started", rows=1000, partition="202606")
        log.error("Failed", error=str(e), step="gold_write")

    Why structured?
        - CloudWatch Insights: query by fields (| filter job_name = "X" | stats count by step)
        - Datadog/Splunk: auto-parse JSON fields
        - Debugging: correlation_id traces one execution across all jobs in a pipeline

    Platform notes:
        - AWS Glue: enable continuous logging → CloudWatch
        - Databricks: Ganglia + driver logs (use log4j config for JSON)
        - Open-source: configure Python logging with JsonFormatter
    """

    def __init__(self, job_name: str, correlation_id: str = None, environment: str = "dev"):
        self.job_name = job_name
        self.correlation_id = correlation_id or str(uuid.uuid4())[:8]
        self.environment = environment

    def _emit(self, level: str, message: str, **kwargs):
        record = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "level": level,
            "job_name": self.job_name,
            "correlation_id": self.correlation_id,
            "environment": self.environment,
            "message": message,
            **kwargs,
        }
        print(json.dumps(record), flush=True)

    def info(self, message: str, **kwargs): self._emit("INFO", message, **kwargs)
    def warning(self, message: str, **kwargs): self._emit("WARNING", message, **kwargs)
    def error(self, message: str, **kwargs): self._emit("ERROR", message, **kwargs)


# ============================================================================
# 7. HISTORY / BACKFILL MODE — Same job, different behavior
# ============================================================================
class BackfillController:
    """
    Controls whether a job runs in daily (incremental) or backfill (full history) mode.
    Same Glue script handles both — controlled by DDB parameters.

    Pattern from production:
        - Daily: LOOKBACK_DAYS=60, processes last 2 months
        - Backfill: LOOKBACK_DAYS=0 + FORCE_RUN=true, processes everything
        - Initial Load: INITIAL_LOAD=true, special cap logic for DS data

    Usage:
        ctrl = BackfillController(args)
        if ctrl.is_backfill:
            months = ctrl.get_all_months(silver_df)
        else:
            months = ctrl.get_incremental_months(silver_df)

    Platform equivalents:
        - Databricks Workflows: parameter overrides per trigger
        - Airflow: DAG params + backfill command (airflow backfill -s START -e END)
        - Step Functions: input JSON controls behavior
    """

    def __init__(self, args: dict):
        self.lookback = int(args.get("LOOKBACK_DAYS", "60"))
        self.force_run = args.get("FORCE_RUN", "false").lower() == "true"
        self.initial_load = args.get("INITIAL_LOAD", "false").lower() == "true"

    @property
    def is_backfill(self) -> bool:
        return self.lookback == 0 or self.initial_load

    @property
    def is_daily(self) -> bool:
        return not self.is_backfill

    def get_months_to_process(self, df: DataFrame, partition_col: str = "mnth_id") -> List[int]:
        """Get months based on mode."""
        if self.is_backfill:
            return sorted([r[0] for r in df.select(partition_col).distinct().collect()])
        else:
            from datetime import datetime, timedelta
            cutoff = int((datetime.now() - timedelta(days=self.lookback)).strftime("%Y%m"))
            return sorted([r[0] for r in
                           df.filter(F.col(partition_col) >= cutoff)
                           .select(partition_col).distinct().collect()])

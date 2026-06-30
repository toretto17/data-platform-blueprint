"""
================================================================================
AWS GLUE — Advanced Patterns & Options  [AWS Glue 5.x / Spark 3.5]
================================================================================
Purpose: Comprehensive reference for all AWS Glue-specific DE patterns NOT
         covered in the main job templates. Includes: DynamicFrame ops, error
         handling (stageThreshold), bounded execution, Iceberg/Delta formats,
         performance options, cost optimizations (Flex, G.025X, warm pools).

This is a REFERENCE file — copy the patterns you need into your jobs.

Contents:
    1. DynamicFrame error handling (stageThreshold, totalThreshold, errorsAsDynamicFrame)
    2. Bounded execution (boundedSize, groupFilter, bounded reads)
    3. Glue-native write optimization (useGlueParquetWriter, useS3ListImplementation)
    4. Iceberg table support (read/write/MERGE via Spark SQL in Glue)
    5. Cost optimization (Flex execution, G.025X workers, Auto Scaling, warm pools)
    6. Job bookmarks (advanced: keys, transformation_ctx, reset)
    7. Data Catalog + Lake Formation integration tips

Verified against: docs.aws.amazon.com/glue/latest/dg/ (Jun 2026), Glue 5.0 blog.

Platform notes: AWS Glue 5.x (Spark 3.5). Some features require specific Glue versions.
Version : 2026-06-30
================================================================================
"""
import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("advanced_patterns_aws")


# ============================================================================
# 1. DYNAMICFRAME ERROR HANDLING
# ============================================================================
"""
DynamicFrame has built-in error handling that DataFrame doesn't:
- stageThreshold: max errors per stage (transformation step) before failing
- totalThreshold: max total errors across all stages before failing
- errorsAsDynamicFrame(): get the bad records as a separate DynamicFrame for logging

This lets you process millions of rows, tolerate some bad records, and capture
them separately — without crashing the whole job.
"""


def read_with_error_thresholds(glue_context: GlueContext, database: str, table: str):
    """Read with error thresholds — tolerate some bad records."""
    dyf = glue_context.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        transformation_ctx="read_source",             # REQUIRED for bookmarks + error tracking
        additional_options={
            "boundedFiles": "100",                     # process max 100 files per run (bounded execution)
        },
    )

    # Apply a mapping that might fail on some rows
    mapped = dyf.apply_mapping([
        ("id", "string", "id", "string"),
        ("amount", "string", "amount", "double"),     # string→double can fail on non-numeric
        ("event_date", "string", "event_date", "date"),
    ])

    # Check errors — if too many bad records, you can decide to fail
    error_count = mapped.errorsCount()
    logger.info(f"Errors in mapping: {error_count}")

    # Get bad records as a separate DynamicFrame (for logging/debugging)
    errors_dyf = mapped.errorsAsDynamicFrame()
    if errors_dyf.count() > 0:
        logger.warning(f"Bad records: {errors_dyf.count()}")
        # Write bad records to a separate S3 path for debugging
        glue_context.write_dynamic_frame.from_options(
            frame=errors_dyf,
            connection_type="s3",
            connection_options={"path": "s3://CHANGE_ME/_errors/"},
            format="json",
        )

    # Use stageThreshold / totalThreshold in resolveChoice for controlled failure
    resolved = mapped.resolveChoice(
        choice="cast:double",
        stageThreshold=100,                           # max 100 errors in this stage
        totalThreshold=1000,                          # max 1000 total errors across job
    )
    return resolved


# ============================================================================
# 2. BOUNDED EXECUTION (control how much data to process per run)
# ============================================================================
"""
For large datasets, you may want to process only a bounded amount per run
(e.g. max 100 files, or max 1GB) instead of everything at once. Useful for:
- Cost control (short jobs, predictable duration)
- Incremental processing without bookmarks
- Preventing OOM on first run of a large backlog
"""


def read_bounded(glue_context: GlueContext, database: str, table: str):
    """Bounded read: process at most N files or N bytes per run."""
    # Option A: bound by file count
    dyf = glue_context.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        transformation_ctx="bounded_read",
        additional_options={
            "boundedFiles": "50",                      # max 50 files per run
            # "boundedSize": "1073741824",            # OR max 1 GB per run (in bytes)
        },
    )
    return dyf


def read_with_group_filter(glue_context: GlueContext, database: str, table: str):
    """groupFilter: process only specific partition groups (like a targeted predicate).
    More flexible than push_down_predicate for complex partition selection."""
    dyf = glue_context.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        transformation_ctx="group_filter_read",
        push_down_predicate="year='2026' AND month='06'",
        # groupFilter: advanced filtering (lambda on partition values)
        # additional_options={"groupFilter": "partition_0 == '2026'"},
    )
    return dyf


# ============================================================================
# 3. GLUE-NATIVE WRITE OPTIMIZATION
# ============================================================================
"""
Glue has native optimized writers that are faster than vanilla Spark writers.
These are enabled via connection_options or format_options.
"""


def write_optimized(glue_context: GlueContext, dyf: DynamicFrame, path: str):
    """Use Glue's native Parquet writer (faster + better S3 handling)."""
    glue_context.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={
            "path": path,
            "partitionKeys": ["mnth_id"],             # CHANGE_ME
            # Performance options:
            "useGlueParquetWriter": "true",           # Glue's optimized Parquet writer
            "useS3ListImplementation": "true",        # faster S3 listing (parallel list)
        },
        format="parquet",
        format_options={
            "compression": "snappy",
            "blockSize": 134217728,                    # 128MB block size (optimal for most)
        },
    )
    logger.info(f"Written (optimized) → {path}")


# ============================================================================
# 4. ICEBERG TABLE SUPPORT (Glue 4.0+ / 5.0)
# ============================================================================
"""
Glue supports Apache Iceberg tables natively (--datalake-formats=iceberg).
Read/write/MERGE via Spark SQL. Iceberg gives: time-travel, schema evolution,
hidden partitioning, MERGE INTO, row-level deletes.

Required Glue job params:
  --datalake-formats  iceberg
  --conf  spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf  spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf  spark.sql.catalog.glue_catalog.warehouse=s3://CHANGE_ME/warehouse/
  --conf  spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
  --conf  spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
"""


def iceberg_read_write_merge(spark: SparkSession):
    """Iceberg operations via Spark SQL in Glue."""

    # Read from Iceberg table
    df = spark.sql("SELECT * FROM glue_catalog.my_db.my_iceberg_table WHERE dt = '20260630'")

    # Write (append)
    df.writeTo("glue_catalog.my_db.my_iceberg_table").append()

    # Write (overwrite partition)
    df.writeTo("glue_catalog.my_db.my_iceberg_table").overwritePartitions()

    # MERGE (upsert) — Iceberg native
    spark.sql("""
        MERGE INTO glue_catalog.my_db.target t
        USING glue_catalog.my_db.source s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Time-travel (read historical version)
    historical = spark.sql("""
        SELECT * FROM glue_catalog.my_db.my_iceberg_table
        FOR SYSTEM_TIME AS OF '2026-06-29 10:00:00'
    """)

    # Expire snapshots (cleanup old versions — save storage cost)
    spark.sql("""
        CALL glue_catalog.system.expire_snapshots(
            table => 'my_db.my_iceberg_table',
            older_than => TIMESTAMP '2026-06-01 00:00:00'
        )
    """)
    return df


# ============================================================================
# 5. COST OPTIMIZATION
# ============================================================================
"""
AWS Glue cost levers:
- Worker type: G.025X (cheapest, 2GB RAM) → G.1X → G.2X → G.4X → G.8X
- Flex execution: up to 35% cheaper (uses spare capacity, may wait to start)
- Auto Scaling: start with few workers, scale up automatically (Glue 3.0+)
- Warm pools (keep_alive_period_in_seconds): reuse workers across runs (< 2 min startup)
- Bounded execution: cap files/bytes per run (predictable cost)
"""


def cost_optimized_job_config():
    """Reference: cost optimization options for Glue jobs.
    Set these in the Glue job DefaultArguments or Terraform."""
    return {
        # --- Worker type (pick the smallest that fits your data) ---
        # G.025X: 2 vCPU, 4 GB (for light transforms, DQ checks, metadata)
        # G.1X:   4 vCPU, 16 GB (standard ETL)
        # G.2X:   8 vCPU, 32 GB (complex joins, large shuffles)
        # G.4X:   16 vCPU, 64 GB (very large datasets)
        # G.8X:   32 vCPU, 128 GB (extreme — rarely needed)
        "WorkerType": "G.1X",                         # CHANGE_ME

        # --- Flex execution (up to 35% cheaper — non-urgent jobs) ---
        # Runs on spare capacity; may wait up to 20 min to start.
        # Perfect for: nightly batch jobs, backfills, non-SLA-critical pipelines.
        "ExecutionClass": "FLEX",                     # "STANDARD" (default) or "FLEX"

        # --- Auto Scaling (start small, grow if needed — Glue 3.0+) ---
        # Replaces: NumberOfWorkers fixed at max. Now starts at min, scales to max.
        "NumberOfWorkers": 10,                        # this becomes the MAX
        "--enable-auto-scaling": "true",              # workers scale 2 → max dynamically

        # --- Warm pools (reuse workers — fast startup < 2 min) ---
        # Workers stay warm for the specified seconds after job ends.
        # Next run of the same job reuses them (no cold-start provisioning).
        "--keep-alive-period-in-seconds": "300",      # 5 min warm pool (saves ~3 min startup)

        # --- Bounded execution (cap per run — predictable cost) ---
        # Set in connection_options: boundedFiles / boundedSize
        # Each run processes at most N files → multiple runs drain the backlog.

        # --- Other cost savers ---
        "--job-bookmark-option": "job-bookmark-enable",  # skip already-processed files
        "--enable-metrics": "true",                    # track DPU usage in CloudWatch
    }


# ============================================================================
# 6. JOB BOOKMARKS (ADVANCED)
# ============================================================================
"""
Job Bookmarks track which data has been processed (file-level for S3, key-based for JDBC).
They prevent reprocessing on each run. Key points:
- MUST set transformation_ctx on every read (bookmarks are per-ctx, not global)
- For JDBC: bookmark tracks by the table's primary key or specified bookmark keys
- Reset: aws glue reset-job-bookmark --job-name <name>
- Pause: --job-bookmark-option job-bookmark-pause (run without updating the marker)
"""


def read_with_bookmark(glue_context: GlueContext, database: str, table: str):
    """Standard bookmark-enabled read. transformation_ctx MUST be unique per source."""
    return glue_context.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        transformation_ctx="my_unique_source_name",   # CHANGE_ME (unique per source in job)
        # For JDBC bookmark:
        # additional_options={"jobBookmarkKeys": ["id"], "jobBookmarkKeysSortOrder": "asc"},
    )


# ============================================================================
# 7. DATA CATALOG + LAKE FORMATION TIPS
# ============================================================================
"""
- push_down_predicate: Glue reads ONLY the matching partitions from the catalog
  (skips S3 listing for non-matching partitions). MASSIVE perf win for partitioned tables.
- Lake Formation: Glue role needs lakeformation:GetDataAccess for LF-managed tables.
- Catalog updates: set updateBehavior to auto-register new partitions on write.
"""


def read_with_catalog_optimizations(glue_context: GlueContext, database: str, table: str,
                                     partition_predicate: str):
    """Read with all catalog optimizations enabled."""
    return glue_context.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        push_down_predicate=partition_predicate,       # e.g. "mnth_id >= '202601'"
        transformation_ctx="optimized_read",
        additional_options={
            "useS3ListImplementation": "true",         # parallel S3 listing
            "boundedFiles": "200",                     # cap files per run
        },
    )


def write_with_catalog_update(glue_context: GlueContext, dyf: DynamicFrame,
                               database: str, table: str, path: str):
    """Write + auto-update the Glue Catalog (registers new partitions)."""
    sink = glue_context.getSink(
        connection_type="s3",
        path=path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",           # auto-register new partitions
        partitionKeys=["mnth_id"],
    )
    sink.setFormat("glueparquet", compression="snappy")
    sink.setCatalogInfo(catalogDatabase=database, catalogTableName=table)
    sink.writeFrame(dyf)
    logger.info(f"Written + catalog updated → {database}.{table}")


# ============================================================================
# SUMMARY: ALL GLUE JOB PARAMETERS (quick reference)
# ============================================================================
def all_glue_job_params():
    """Copy these into your Glue job DefaultArguments (Terraform or console)."""
    return {
        # --- Required ---
        "--job-language": "python",
        "--job-bookmark-option": "job-bookmark-enable",
        "--enable-glue-datacatalog": "true",

        # --- Performance ---
        "--enable-metrics": "true",
        "--enable-spark-ui": "true",
        "--spark-event-logs-path": "s3://CHANGE_ME/spark-logs/",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-continuous-log-filter": "true",
        "--conf": "spark.sql.adaptive.enabled=true",

        # --- Cost ---
        "--enable-auto-scaling": "true",
        "--keep-alive-period-in-seconds": "300",

        # --- Delta / Iceberg ---
        "--datalake-formats": "iceberg,delta",         # enable both
        "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

        # --- Custom ---
        "--TempDir": "s3://CHANGE_ME/temp/",
        "--additional-python-modules": "boto3>=1.34.0",
    }


if __name__ == "__main__":
    print("Reference file — copy patterns into your jobs.")
    print("Cost config:", cost_optimized_job_config())
    print("All params:", all_glue_job_params())

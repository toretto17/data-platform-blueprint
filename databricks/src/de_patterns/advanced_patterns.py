"""
================================================================================
DATABRICKS DATA ENGINEERING — Advanced Patterns & Options  [Databricks]
================================================================================
Purpose: Comprehensive reference for all Databricks-specific DE patterns NOT
         covered in the main job templates. Includes: schema handling modes,
         error/rescue patterns, Autoloader advanced options, CDF streaming
         options, checkpointing strategies, and performance tuning.

This is a REFERENCE file — copy the patterns you need into your jobs.

Contents:
    1. Schema handling modes (FAILFAST / PERMISSIVE / DROPMALFORMED / rescue)
    2. Autoloader advanced options (schemaEvolutionMode, rate limits, file filters)
    3. CDF streaming options (ignoreChanges, ignoreDeletes, skipChangeCommits)
    4. Checkpointing strategies (exactly-once, recovery, cleanup)
    5. Error handling patterns (badRecordsPath, rescued data column)
    6. Performance options (maxFilesPerTrigger, maxBytesPerTrigger, etc.)

Verified against: docs.databricks.com (Jun 2026), Azure Databricks docs.

Platform notes: DBR 13.3 LTS+. Some features require higher versions (noted).
Version : 2026-06-30
================================================================================
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# ============================================================================
# 1. SCHEMA HANDLING MODES (for read — CSV, JSON, etc.)
# ============================================================================
"""
When reading semi-structured data (CSV, JSON), Spark needs to know how to handle
rows that don't match the expected schema. Three modes + rescued data column:

| Mode | Behavior | Use when |
|---|---|---|
| PERMISSIVE (default) | Sets malformed fields to null, puts raw text in _corrupt_record | You want ALL rows, handle bad ones later |
| DROPMALFORMED | Silently drops rows that don't parse | You don't care about bad rows |
| FAILFAST | Throws exception on first bad row | You want to STOP immediately on bad data |

+ Rescued Data Column: captures columns that don't match schema (schema drift safety net)
"""


def read_with_failfast(path: str, fmt: str = "json") -> DataFrame:
    """FAILFAST: stop immediately if any row doesn't match schema.
    Use in: production where bad data should block the pipeline."""
    return (spark.read
            .option("mode", "FAILFAST")
            .format(fmt)
            .load(path))


def read_with_permissive(path: str, fmt: str = "json") -> DataFrame:
    """PERMISSIVE (default): malformed → null, raw text → _corrupt_record column.
    Use in: ingestion where you want to land everything then filter bad rows."""
    return (spark.read
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .format(fmt)
            .load(path))


def read_with_dropmalformed(path: str, fmt: str = "json") -> DataFrame:
    """DROPMALFORMED: silently drop bad rows (no error, no _corrupt_record).
    Use in: when you truly don't need bad rows and don't want to track them."""
    return (spark.read
            .option("mode", "DROPMALFORMED")
            .format(fmt)
            .load(path))


def read_with_rescued_data(path: str, fmt: str = "json") -> DataFrame:
    """RESCUED DATA COLUMN: captures data that doesn't fit the schema into a JSON column.
    Better than PERMISSIVE for schema drift — you keep the data without losing it.
    Requires: explicit schema OR inferSchema + rescuedDataColumn option."""
    return (spark.read
            .option("rescuedDataColumn", "_rescued_data")
            .format(fmt)
            .load(path))


def read_with_bad_records_path(path: str, bad_path: str, fmt: str = "json") -> DataFrame:
    """BAD RECORDS PATH: writes unparseable rows to a separate location for later review.
    NOTE: no transaction guarantees on bad records (Databricks docs caveat)."""
    return (spark.read
            .option("badRecordsPath", bad_path)   # CHANGE_ME: s3://bucket/_bad_records/
            .format(fmt)
            .load(path))


# ============================================================================
# 2. AUTOLOADER ADVANCED OPTIONS
# ============================================================================
"""
Autoloader (cloudFiles) is the recommended ingestion method for files.
Below are ALL the important options beyond the basics in stream_ingest.py.
"""


def autoloader_full_options(source_path: str, checkpoint_path: str,
                            target_table: str, fmt: str = "json"):
    """Autoloader with ALL production-recommended options.
    Copy + customize what you need."""
    stream = (spark.readStream
              .format("cloudFiles")
              # --- Core options ---
              .option("cloudFiles.format", fmt)                          # parquet|json|csv|avro|text
              .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")  # REQUIRED: persists inferred schema
              .option("cloudFiles.inferColumnTypes", "true")             # infer types (not all string)

              # --- Schema evolution (how to handle new columns appearing) ---
              # Modes: "addNewColumns" (default) | "rescue" | "failOnNewColumns" | "none"
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # auto-add new cols to table

              # --- Rescued data (safety net for schema drift) ---
              .option("rescuedDataColumn", "_rescued_data")             # JSON col for data that didn't fit schema

              # --- Rate limiting (control processing speed / cost) ---
              .option("maxFilesPerTrigger", "1000")                     # max files per micro-batch (backpressure)
              .option("maxBytesPerTrigger", "10g")                      # max bytes per micro-batch

              # --- File filtering ---
              .option("pathGlobFilter", "*.json")                       # only process matching files
              # .option("modifiedAfter", "2026-01-01T00:00:00Z")        # only files modified after this timestamp
              # .option("modifiedBefore", "2026-12-31T23:59:59Z")       # only files modified before

              # --- File discovery mode ---
              # "directory" (default, uses directory listing) or "notification" (uses cloud events — faster for huge dirs)
              # .option("cloudFiles.useNotifications", "true")           # requires setup (SQS/EventGrid/PubSub)

              .load(source_path))

    # Write with all production options
    query = (stream
             .writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path + "/_write")  # REQUIRED: exactly-once
             .option("mergeSchema", "true")                             # auto-evolve target schema
             .trigger(availableNow=True)                                 # process all available, then stop
             # OR: .trigger(processingTime="60 seconds")                # continuous micro-batch every 60s
             .toTable(target_table))
    return query


# ============================================================================
# 3. CDF (Change Data Feed) STREAMING OPTIONS
# ============================================================================
"""
When reading CDF as a stream, these options control behavior on source table changes.
"""


def cdf_streaming_with_options(source_table: str, checkpoint_path: str):
    """CDF streaming read with production options."""
    stream = (spark.readStream
              .option("readChangeFeed", "true")

              # --- How to handle schema changes in the source ---
              # .option("withEventTimeOrder", "true")       # order events by event time (DBR 13.3+)

              # --- Handle non-append operations on source ---
              # If source table is compacted/optimized, these prevent stream failure:
              .option("ignoreChanges", "true")              # ignore file-level changes (OPTIMIZE, Z-ORDER)
              # .option("ignoreDeletes", "true")            # ignore DELETE operations
              # .option("skipChangeCommits", "true")        # skip commits that only have changes (no new data)

              # --- Rate limiting ---
              .option("maxFilesPerTrigger", "100")
              .option("maxBytesPerTrigger", "1g")

              .table(source_table))
    return stream


# ============================================================================
# 4. CHECKPOINTING STRATEGIES
# ============================================================================
"""
Checkpoints enable exactly-once processing. Key decisions:
- WHERE to store: durable location (S3/ADLS) — NOT local disk / DBFS root
- WHEN to clean up: only after confirming downstream success
- HOW to recover: restart from checkpoint (automatic) or reset (new checkpoint)
"""


def checkpoint_best_practices():
    """Reference: checkpoint path conventions."""
    return {
        "location_pattern": "s3://CHANGE_ME/_checkpoints/{job_name}/",
        "rules": [
            "ALWAYS use a durable cloud path (S3/ADLS/GCS) — never ephemeral storage",
            "One checkpoint per stream (never share between different streams)",
            "If you change the query logic significantly, use a NEW checkpoint path",
            "To replay from scratch: delete the checkpoint folder + target table",
            "To resume after failure: just restart — checkpoint handles recovery automatically",
            "For schema evolution: keep the same checkpoint (schema updates are compatible)",
        ],
        "recovery_options": {
            "normal_restart": "Just restart the job — picks up from last committed offset",
            "schema_change_restart": "Same checkpoint works if schema evolution is additive",
            "full_reset": "Delete checkpoint folder → stream replays everything (reprocesses all)",
            "partial_reset": "Set startingVersion/startingTimestamp → checkpoint from a specific point",
        }
    }


# ============================================================================
# 5. ERROR HANDLING PATTERNS
# ============================================================================
"""
Production pipelines must handle bad data gracefully:
- Don't crash on 1 bad row out of millions
- Track bad rows for debugging
- Alert on high error rates
"""


def streaming_with_error_handling(source_path: str, target_table: str,
                                  checkpoint_path: str, bad_records_table: str):
    """Production streaming with rescued data → separate bad records table."""
    stream = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")
              .option("rescuedDataColumn", "_rescued_data")   # captures schema-drift data
              .option("cloudFiles.schemaEvolutionMode", "rescue")  # don't fail, rescue mismatches
              .load(source_path))

    def process_batch(df: DataFrame, batch_id: int):
        """Split good vs rescued rows per micro-batch."""
        # Good rows: _rescued_data is null (everything fit the schema)
        good = df.filter(F.col("_rescued_data").isNull()).drop("_rescued_data")
        good.write.format("delta").mode("append").saveAsTable(target_table)

        # Bad/rescued rows: _rescued_data has content (something didn't fit)
        bad = df.filter(F.col("_rescued_data").isNotNull())
        if bad.head(1):
            (bad.withColumn("_batch_id", F.lit(batch_id))
                .withColumn("_error_ts", F.current_timestamp())
                .write.format("delta").mode("append").saveAsTable(bad_records_table))

    query = (stream.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", checkpoint_path)
             .trigger(availableNow=True)
             .start())
    return query


# ============================================================================
# 6. PERFORMANCE OPTIONS
# ============================================================================
"""
Control cost + speed of streaming/batch ingestion.
"""


def performance_configs():
    """Recommended Spark configs for ingestion performance."""
    configs = {
        # --- Streaming rate control ---
        "maxFilesPerTrigger": "1000",         # files per micro-batch (lower = less memory per batch)
        "maxBytesPerTrigger": "10g",          # bytes per micro-batch (alternative to file count)

        # --- Delta write optimization ---
        "spark.databricks.delta.optimizeWrite.enabled": "true",   # coalesce small files on write
        "spark.databricks.delta.autoCompact.enabled": "true",     # auto-compact after writes
        "spark.sql.shuffle.partitions": "auto",                    # AQE decides (DBR 14+)

        # --- Autoloader specific ---
        "cloudFiles.maxFilesPerTrigger": "1000",      # same as maxFilesPerTrigger for cloudFiles
        "cloudFiles.maxBytesPerTrigger": "10g",
        "cloudFiles.backfillInterval": "1 day",        # how often to re-scan for missed files

        # --- Memory ---
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
    }
    return configs


# ============================================================================
# USAGE EXAMPLES
# ============================================================================
if __name__ == "__main__":
    # Example 1: Autoloader with all options
    # autoloader_full_options("s3://raw/events/", "s3://checkpoints/events", "main.bronze.events", "json")

    # Example 2: Read with FAILFAST (stop on bad data)
    # df = read_with_failfast("s3://raw/strict_data/", "csv")

    # Example 3: Read with rescued data (keep everything, flag mismatches)
    # df = read_with_rescued_data("s3://raw/evolving_schema/", "json")

    # Example 4: CDF streaming with production options
    # stream = cdf_streaming_with_options("main.bronze.customers_cdc", "s3://checkpoints/cdc")

    # Example 5: Streaming with error routing (good → target, bad → error table)
    # streaming_with_error_handling("s3://raw/events/", "main.bronze.events",
    #                               "s3://checkpoints/events", "main.ops.bad_records")

    print("See docstrings above for usage. Copy patterns into your jobs.")

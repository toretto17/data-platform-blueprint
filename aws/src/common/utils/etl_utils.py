"""
================================================================================
ETL UTILITIES — Production Patterns
================================================================================
Contains:
    1. Early Exit (freshness check without .count())
    2. Metadata-based freshness (Gold watermark vs Silver new data)
    3. Write strategies (Spark native, Glue Catalog, Delta, Iceberg, Databricks)
    4. Schema evolution handling
    5. Partition management

Best Practices:
    - NEVER use .count() to check emptiness — use .head(1) or .isEmpty (Spark 3.3+)
    - Metadata-driven freshness avoids reprocessing unchanged data
    - Write strategy is pluggable — AWS Glue, Databricks, open-source Spark
================================================================================
"""
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Tuple
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("etl_utils")


# ============================================================================
# 1. EARLY EXIT — Check data existence without .count()
# ============================================================================
class EarlyExitCheck:
    """
    Check if DataFrame has data WITHOUT triggering a full .count().
    .count() scans the entire dataset — O(N). .head(1) stops at first row — O(1).

    Platform behavior:
        - Spark (all): .head(1) or .isEmpty() — universally works
        - Delta: .head(1) reads 1 file max (data skipping applies)
        - Iceberg: same — manifest pruning means minimal I/O
        - Databricks: Photon optimizes .isEmpty() to metadata check when possible
        - Pandas/Polars: len(df) == 0 is O(1) since shape is stored in metadata

    NEVER DO:
        if df.count() == 0:   # Scans entire dataset!
        if df.rdd.isEmpty():  # Forces RDD evaluation (old Spark pattern, slow)

    ALWAYS DO:
        if EarlyExitCheck.is_empty(df):  # O(1)

    Usage:
        if EarlyExitCheck.is_empty(df):
            logger.info("No new data — exiting early")
            job.commit()
            return

        if not EarlyExitCheck.has_new_data(new_df, existing_df, key_col="data_dt"):
            logger.info("No new partitions — exiting early")
            job.commit()
            return
    """

    @staticmethod
    def is_empty(df: DataFrame) -> bool:
        """Check if DataFrame is empty without full scan. O(1) operation."""
        # Spark 3.3+: df.isEmpty() is the idiomatic way
        # Fallback for older versions: head(1)
        try:
            return df.isEmpty()
        except AttributeError:
            return len(df.head(1)) == 0

    @staticmethod
    def has_rows(df: DataFrame) -> bool:
        """Inverse of is_empty. Preferred for readability."""
        return not EarlyExitCheck.is_empty(df)

    @staticmethod
    def has_new_data(source_df: DataFrame, target_df: DataFrame,
                     key_col: str = "data_dt") -> bool:
        """Check if source has partitions not yet in target. O(1) — only checks max values."""
        source_max = source_df.agg(F.max(key_col)).head(1)
        if not source_max or source_max[0][0] is None:
            return False
        target_max = target_df.agg(F.max(key_col)).head(1)
        if not target_max or target_max[0][0] is None:
            return True  # Target empty = all data is new
        return source_max[0][0] > target_max[0][0]

    @staticmethod
    def get_new_partitions(source_df: DataFrame, target_df: DataFrame,
                           partition_col: str) -> List:
        """Get partition values in source but not in target. Cheap DISTINCT operation."""
        source_parts = source_df.select(partition_col).distinct()
        target_parts = target_df.select(partition_col).distinct()
        new_parts = source_parts.subtract(target_parts)
        if EarlyExitCheck.is_empty(new_parts):
            return []
        return [row[0] for row in new_parts.collect()]


# ============================================================================
# 2. METADATA-BASED FRESHNESS — Avoid reprocessing unchanged data
# ============================================================================
class MetadataFreshnessManager:
    """
    Track pipeline watermarks to avoid reprocessing data that hasn't changed.

    Pattern (from production):
        - Gold is NOT at daily granularity (e.g., monthly aggregate)
        - Silver gets new daily data
        - Compare: last processed watermark vs latest Silver data_dt
        - If Silver max(data_dt) <= watermark → skip (no new data)

    Storage options (choose based on platform):

    | Storage | Platform | Pros | Cons |
    |---------|----------|------|------|
    | S3 marker | AWS | Simple, serverless, cheap | No transactions, eventual consistency |
    | DynamoDB | AWS | Transactional, queryable, TTL | Extra AWS dependency |
    | Delta table properties | Delta/Databricks | Co-located with data, ACID | Requires Delta |
    | Iceberg table properties | Iceberg | Same as Delta | Requires Iceberg |
    | PostgreSQL/MySQL | Any | Standard RDBMS, dashboardable | External dependency |
    | Redis | Any | Fast, TTL support | Volatile (if not persisted) |
    | DBFS / Workspace | Databricks | Native, no extra infra | Databricks-only |
    | MLflow tracking | ML pipelines | Versioned, UI | Overkill for ETL |

    Usage:
        fm = MetadataFreshnessManager(spark, storage="s3", bucket="my-bucket", prefix="metadata/")
        if fm.is_fresh("gold_sales_mart", silver_df, date_col="data_dt"):
            logger.info("Gold already has latest Silver data — skipping")
            return
        # ... process ...
        fm.update_watermark("gold_sales_mart", new_max_date)

    Databricks-specific:
        fm = MetadataFreshnessManager(spark, storage="delta", metadata_table="main.metadata.watermarks")
    """

    def __init__(self, spark: SparkSession, storage: str = "s3", **kwargs):
        self.spark = spark
        self.storage = storage
        self.config = kwargs  # bucket, prefix, table_name, etc.

    def get_watermark(self, pipeline_name: str) -> Optional[str]:
        """Get last-processed watermark for a pipeline."""
        if self.storage == "s3":
            return self._get_s3_watermark(pipeline_name)
        elif self.storage == "dynamodb":
            return self._get_ddb_watermark(pipeline_name)
        elif self.storage == "delta":
            return self._get_delta_watermark(pipeline_name)
        return None

    def update_watermark(self, pipeline_name: str, watermark_value: str):
        """Update watermark after successful processing."""
        if self.storage == "s3":
            self._put_s3_watermark(pipeline_name, watermark_value)
        elif self.storage == "dynamodb":
            self._put_ddb_watermark(pipeline_name, watermark_value)
        elif self.storage == "delta":
            self._put_delta_watermark(pipeline_name, watermark_value)
        logger.info(f"Watermark updated: {pipeline_name} = {watermark_value}")

    def is_fresh(self, pipeline_name: str, source_df: DataFrame,
                 date_col: str = "data_dt") -> bool:
        """Check if target already has all available source data.
        Returns True if fresh (no new data to process).
        """
        watermark = self.get_watermark(pipeline_name)
        if watermark is None:
            return False  # No watermark = never processed

        # Get max date from source WITHOUT .count()
        source_max_row = source_df.agg(F.max(date_col).alias("max_dt")).head(1)
        if not source_max_row or source_max_row[0]["max_dt"] is None:
            return True  # Source empty = nothing to process

        source_max = str(source_max_row[0]["max_dt"])
        is_fresh = source_max <= watermark
        logger.info(f"Freshness check [{pipeline_name}]: watermark={watermark}, source_max={source_max}, fresh={is_fresh}")
        return is_fresh

    # --- S3 storage ---
    def _get_s3_watermark(self, pipeline_name: str) -> Optional[str]:
        import boto3
        s3 = boto3.client("s3")
        key = f"{self.config.get('prefix', 'metadata/watermarks/')}{pipeline_name}"
        try:
            resp = s3.get_object(Bucket=self.config["bucket"], Key=key)
            return resp["Body"].read().decode().strip()
        except s3.exceptions.NoSuchKey:
            return None
        except Exception:
            return None

    def _put_s3_watermark(self, pipeline_name: str, value: str):
        import boto3
        s3 = boto3.client("s3")
        key = f"{self.config.get('prefix', 'metadata/watermarks/')}{pipeline_name}"
        s3.put_object(Bucket=self.config["bucket"], Key=key, Body=value.encode())

    # --- DynamoDB storage ---
    def _get_ddb_watermark(self, pipeline_name: str) -> Optional[str]:
        import boto3
        ddb = boto3.resource("dynamodb")
        table = ddb.Table(self.config.get("table_name", "pipeline_metadata"))
        resp = table.get_item(Key={"pipeline_name": pipeline_name})
        return resp.get("Item", {}).get("watermark")

    def _put_ddb_watermark(self, pipeline_name: str, value: str):
        import boto3
        ddb = boto3.resource("dynamodb")
        table = ddb.Table(self.config.get("table_name", "pipeline_metadata"))
        table.put_item(Item={"pipeline_name": pipeline_name, "watermark": value,
                             "updated_at": datetime.utcnow().isoformat()})

    # --- Delta table properties ---
    def _get_delta_watermark(self, pipeline_name: str) -> Optional[str]:
        try:
            props = self.spark.sql(f"SHOW TBLPROPERTIES {self.config['metadata_table']}").collect()
            for row in props:
                if row["key"] == f"watermark.{pipeline_name}":
                    return row["value"]
        except Exception:
            pass
        return None

    def _put_delta_watermark(self, pipeline_name: str, value: str):
        self.spark.sql(
            f"ALTER TABLE {self.config['metadata_table']} SET TBLPROPERTIES ('watermark.{pipeline_name}' = '{value}')"
        )


# ============================================================================
# 3. WRITE STRATEGIES — Pluggable, platform-agnostic
# ============================================================================
class WriteStrategy(ABC):
    """
    Abstract write strategy. Implementations for:
        - Spark Native (open-source, any platform)
        - AWS Glue Catalog (saveAsTable with catalog sync)
        - Delta Lake (ACID, schema evolution, time travel)
        - Apache Iceberg (open table format, Athena/Trino compatible)
        - Databricks (Unity Catalog, optimized Delta)

    Performance notes:
        - Spark native write (Parquet) = FASTEST (no catalog overhead)
        - Glue Catalog write = slower (catalog API calls per partition)
        - Delta/Iceberg = moderate (transaction log overhead, but ACID guarantees)

    For maximum write speed:
        1. Write Parquet directly to S3 (spark native)
        2. Run MSCK REPAIR TABLE or Glue Crawler to update catalog
        3. Or use Delta/Iceberg which handle catalog atomically
    """

    @abstractmethod
    def write(self, df: DataFrame, target_path: str, partition_col: str,
              mode: str = "overwrite", **kwargs):
        pass


class SparkNativeWriter(WriteStrategy):
    """
    Direct Parquet write — FASTEST option.
    No catalog coupling. Use with external schema sync (MSCK REPAIR / Crawler).

    When to use:
        - Maximum write throughput needed
        - Catalog sync can be deferred or batched
        - Simple partitioned Parquet is sufficient
    """

    def write(self, df: DataFrame, target_path: str, partition_col: str,
              mode: str = "overwrite", **kwargs):
        logger.info(f"[SparkNative] Writing to {target_path} (mode={mode})")
        (df.write
         .mode(mode)
         .partitionBy(partition_col)
         .option("compression", kwargs.get("compression", "snappy"))
         .option("maxRecordsPerFile", kwargs.get("max_records_per_file", 1000000))
         .parquet(target_path))

    def sync_catalog(self, spark: SparkSession, database: str, table: str, location: str):
        """Update Hive/Glue catalog after write (cheaper than saveAsTable)."""
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {database}.{table}
            USING parquet
            LOCATION '{location}'
        """)
        spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
        logger.info(f"[SparkNative] Catalog synced: {database}.{table}")


class GlueCatalogWriter(WriteStrategy):
    """
    AWS Glue Catalog integrated write (saveAsTable).
    Automatically updates catalog metadata.

    When to use:
        - AWS-native stack (Glue + Athena)
        - Need immediate catalog visibility
        - Catalog consistency more important than raw speed
    """

    def write(self, df: DataFrame, target_path: str, partition_col: str,
              mode: str = "overwrite", **kwargs):
        database = kwargs["database"]
        table = kwargs["table"]
        logger.info(f"[GlueCatalog] Writing to {database}.{table} at {target_path}")

        spark = df.sparkSession
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        (df.write
         .mode(mode)
         .partitionBy(partition_col)
         .format("parquet")
         .option("path", target_path)
         .saveAsTable(f"{database}.{table}"))


class DeltaLakeWriter(WriteStrategy):
    """
    Delta Lake write — ACID transactions, schema evolution, time travel.

    When to use:
        - Need ACID guarantees (concurrent writers)
        - Schema evolution (new columns added over time)
        - Time travel / audit requirements
        - Databricks or open-source Spark with delta-spark

    Prerequisites:
        - delta-spark package: spark.jars.packages = io.delta:delta-spark_2.12:3.1.0
        - Databricks: built-in
    """

    def write(self, df: DataFrame, target_path: str, partition_col: str,
              mode: str = "overwrite", **kwargs):
        logger.info(f"[Delta] Writing to {target_path} (mode={mode})")

        writer = (df.write
                  .format("delta")
                  .mode(mode)
                  .partitionBy(partition_col)
                  .option("overwriteSchema", kwargs.get("overwrite_schema", "false"))
                  .option("mergeSchema", kwargs.get("merge_schema", "true")))

        # Optimize file size
        if kwargs.get("optimize_write", True):
            writer = writer.option("optimizeWrite", "true")

        writer.save(target_path)

    def merge_upsert(self, spark: SparkSession, source_df: DataFrame,
                     target_path: str, merge_keys: List[str], **kwargs):
        """Delta MERGE (upsert) — update existing + insert new rows."""
        from delta.tables import DeltaTable
        target = DeltaTable.forPath(spark, target_path)
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

        (target.alias("target")
         .merge(source_df.alias("source"), merge_condition)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        logger.info(f"[Delta] Merge complete on keys: {merge_keys}")

    def optimize(self, spark: SparkSession, target_path: str, z_order_cols: List[str] = None):
        """Run OPTIMIZE (compaction) + optional Z-ORDER."""
        from delta.tables import DeltaTable
        dt = DeltaTable.forPath(spark, target_path)
        if z_order_cols:
            dt.optimize().executeZOrderBy(*z_order_cols)
        else:
            dt.optimize().executeCompaction()
        logger.info(f"[Delta] Optimized {target_path}")


class IcebergWriter(WriteStrategy):
    """
    Apache Iceberg write — open table format, multi-engine compatible.

    When to use:
        - Multi-engine queries (Spark + Trino + Athena + Flink)
        - AWS (Athena v3 native Iceberg support)
        - Schema evolution + partition evolution
        - Hidden partitioning (no need to know partition layout in queries)

    Prerequisites:
        - Iceberg runtime: spark.jars.packages = org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0
        - Glue Catalog as Iceberg catalog: spark.sql.catalog.glue_catalog = org.apache.iceberg.spark.SparkCatalog
    """

    def write(self, df: DataFrame, target_path: str, partition_col: str,
              mode: str = "overwrite", **kwargs):
        database = kwargs.get("database", "")
        table = kwargs.get("table", "")
        catalog = kwargs.get("catalog", "glue_catalog")
        full_table = f"{catalog}.{database}.{table}" if database else target_path

        logger.info(f"[Iceberg] Writing to {full_table} (mode={mode})")

        if mode == "overwrite":
            df.writeTo(full_table).overwritePartitions()
        elif mode == "append":
            df.writeTo(full_table).append()
        else:
            # Create or replace
            (df.writeTo(full_table)
             .tableProperty("format-version", "2")
             .partitionedBy(partition_col)
             .createOrReplace())

    def merge_upsert(self, spark: SparkSession, source_df: DataFrame,
                     full_table: str, merge_keys: List[str]):
        """Iceberg MERGE INTO (requires Spark 3.4+ with Iceberg)."""
        source_df.createOrReplaceTempView("__iceberg_source")
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        spark.sql(f"""
            MERGE INTO {full_table} t
            USING __iceberg_source s ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)


class DatabricksWriter(WriteStrategy):
    """
    Databricks-optimized write — Unity Catalog, Photon, Auto Optimize.

    When to use:
        - Running on Databricks
        - Unity Catalog for governance
        - Photon engine for speed
        - Auto Optimize + Auto Compact

    Notes:
        - Databricks uses Delta by default
        - Unity Catalog: three-level namespace (catalog.schema.table)
        - Liquid clustering (Databricks 13.3+) replaces Z-ORDER
    """

    def write(self, df: DataFrame, target_path: str, partition_col: str,
              mode: str = "overwrite", **kwargs):
        catalog = kwargs.get("catalog", "main")
        schema = kwargs.get("schema", "default")
        table = kwargs.get("table", "")
        full_table = f"{catalog}.{schema}.{table}"

        logger.info(f"[Databricks] Writing to {full_table} (mode={mode})")

        writer = (df.write
                  .format("delta")
                  .mode(mode)
                  .option("mergeSchema", "true")
                  .option("optimizeWrite", "true")     # Auto-optimize file sizes
                  .option("autoCompact", "true"))      # Auto-compact small files

        if partition_col:
            writer = writer.partitionBy(partition_col)

        writer.saveAsTable(full_table)

    def apply_liquid_clustering(self, spark: SparkSession, full_table: str, cluster_cols: List[str]):
        """Databricks 13.3+ liquid clustering (replaces Z-ORDER)."""
        cols = ", ".join(cluster_cols)
        spark.sql(f"ALTER TABLE {full_table} CLUSTER BY ({cols})")
        logger.info(f"[Databricks] Liquid clustering applied: {cluster_cols}")


# ============================================================================
# 4. SCHEMA EVOLUTION — Handle new/changed columns gracefully
# ============================================================================
class SchemaEvolution:
    """
    Handle schema changes between pipeline runs.

    Patterns:
        - New columns added to source → auto-add to target (mergeSchema)
        - Columns removed → keep in target (backward compatible)
        - Type changes → cast with fallback

    Platform-specific:
        - Delta/Iceberg: mergeSchema option
        - Parquet + Glue: ALTER TABLE ADD COLUMNS
        - Databricks: schema enforcement + evolution policies
    """

    @staticmethod
    def detect_drift(source_df: DataFrame, target_df: DataFrame) -> Dict[str, List[str]]:
        """Detect schema differences between source and target."""
        source_cols = set(source_df.columns)
        target_cols = set(target_df.columns)
        return {
            "new_columns": list(source_cols - target_cols),
            "removed_columns": list(target_cols - source_cols),
            "common_columns": list(source_cols & target_cols),
        }

    @staticmethod
    def align_schemas(source_df: DataFrame, target_df: DataFrame) -> DataFrame:
        """Align source schema to match target (add missing cols as NULL, drop extra)."""
        target_cols = set(target_df.columns)
        source_cols = set(source_df.columns)

        # Add missing columns as NULL
        for col in target_cols - source_cols:
            target_type = dict(target_df.dtypes).get(col, "string")
            source_df = source_df.withColumn(col, F.lit(None).cast(target_type))

        # Select only target columns (in target order)
        return source_df.select(target_df.columns)

    @staticmethod
    def evolve_glue_catalog(spark: SparkSession, database: str, table: str,
                            new_columns: List[Tuple[str, str]]):
        """Add new columns to Glue/Hive catalog table."""
        for col_name, col_type in new_columns:
            spark.sql(f"ALTER TABLE {database}.{table} ADD COLUMNS ({col_name} {col_type})")
            logger.info(f"Schema evolved: added {col_name} ({col_type}) to {database}.{table}")


# ============================================================================
# 5. WRITE STRATEGY FACTORY — Pick the right writer for your platform
# ============================================================================
def get_writer(platform: str = "spark_native") -> WriteStrategy:
    """
    Factory to get the appropriate write strategy.

    Args:
        platform: One of:
            - "spark_native"  → Direct Parquet (fastest, no catalog coupling)
            - "glue_catalog"  → AWS Glue saveAsTable (immediate catalog visibility)
            - "delta"         → Delta Lake (ACID, schema evolution, time travel)
            - "iceberg"       → Apache Iceberg (multi-engine, AWS/GCP/Azure native)
            - "databricks"    → Databricks Unity Catalog (managed Delta, Photon)

    Performance ranking (write speed, fastest first):
        1. spark_native → No overhead. Parquet directly to object store.
        2. databricks → Photon engine + optimizeWrite. ~1s txn log overhead.
        3. delta → Transaction log commit (~1-2s overhead per write).
        4. iceberg → Manifest file management (~2-3s overhead per write).
        5. glue_catalog → API calls per partition (~5-10s per partition registered).

    Decision matrix:
    ┌────────────────────────────────────────────────────────────────────────┐
    │ Need ACID transactions?                                                │
    │   NO  → spark_native (fastest) + deferred catalog sync                │
    │   YES → ↓                                                              │
    │                                                                        │
    │ Need multi-engine queries (Spark + Trino + Athena + Flink)?           │
    │   YES → iceberg                                                        │
    │   NO  → ↓                                                              │
    │                                                                        │
    │ Running on Databricks?                                                 │
    │   YES → databricks (Unity Catalog, Photon, Liquid Clustering)         │
    │   NO  → ↓                                                              │
    │                                                                        │
    │ AWS Glue + Athena only?                                                │
    │   YES → glue_catalog (simple) or iceberg (if Athena v3)               │
    │   NO  → delta (EMR, Dataproc, self-managed Spark)                     │
    └────────────────────────────────────────────────────────────────────────┘

    Post-write optimization (platform-specific):
        - spark_native: MSCK REPAIR TABLE or Glue Crawler or Hive metastore sync
        - delta: OPTIMIZE + VACUUM (or Databricks Auto Optimize = zero effort)
        - iceberg: rewrite_data_files() procedure
        - databricks: Auto Optimize + Auto Compact (enabled once, handles itself)
        - glue_catalog: no post-write needed (catalog updated during saveAsTable)

    Catalog sync (if write doesn't auto-register):
        - AWS: aws glue create-partition / MSCK REPAIR TABLE / Crawler
        - Databricks: Unity Catalog auto-syncs for managed tables
        - Open-source Hive: MSCK REPAIR TABLE or ALTER TABLE ADD PARTITION
        - Iceberg: No sync needed — manifest IS the catalog
    """
    writers = {
        "spark_native": SparkNativeWriter,
        "glue_catalog": GlueCatalogWriter,
        "delta": DeltaLakeWriter,
        "iceberg": IcebergWriter,
        "databricks": DatabricksWriter,
    }
    if platform not in writers:
        raise ValueError(f"Unknown platform: {platform}. Available: {list(writers.keys())}")
    return writers[platform]()


# ============================================================================
# 6. PARTITION MANAGEMENT — Platform-specific efficient partition operations
# ============================================================================
class PartitionManager:
    """
    Utilities for partition management.

    Key insight: HOW you "check partitions without scanning" depends on platform:

    | Platform | Mechanism |
    |----------|-----------|
    | Hive / Glue Catalog | SHOW PARTITIONS (catalog metadata API, O(1)) |
    | Delta Lake | Transaction log (_delta_log/) has per-file stats → filter() auto-skips |
    | Iceberg | Manifest files have partition stats → query planning prunes |
    | Databricks Unity | Same as Delta — just filter(), Photon + data skipping handles rest |
    | Raw Parquet (no catalog) | No metadata layer — must list files (aws s3 ls / dbutils.fs.ls) |

    In Delta/Iceberg, you DON'T need to explicitly "get partitions then filter."
    Just read with .filter() — the engine's metadata layer does partition pruning.
    The explicit partition listing below is for Hive/Glue-style catalogs.
    """

    # --- Hive / Glue Catalog (traditional partition registry) ---
    @staticmethod
    def get_partitions_hive(spark: SparkSession, database: str, table: str,
                            lookback_days: int, partition_col: str = "data_dt") -> List:
        """Get partition values from last N days via Hive Metastore/Glue Catalog.
        O(1) operation — reads catalog metadata only, no data scan.
        Works with: AWS Glue, Hive Metastore, Databricks (legacy Hive tables).
        """
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")
        partitions = spark.sql(f"SHOW PARTITIONS {database}.{table}").collect()
        values = [r[0].split("=")[1] for r in partitions]
        return sorted([v for v in values if v >= cutoff])

    # --- Delta Lake (transaction log based) ---
    @staticmethod
    def get_partitions_delta(spark: SparkSession, table_path_or_name: str,
                             partition_col: str, lookback_days: int) -> List:
        """Get distinct partition values from Delta table.
        Delta doesn't have SHOW PARTITIONS — but reading distinct partition col
        is fast because Delta's data skipping uses file-level stats.
        For large tables, use DESCRIBE HISTORY or _delta_log parsing.
        """
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")
        # This is fast on Delta — file-level stats avoid full scan
        parts = (spark.read.format("delta").load(table_path_or_name)
                 .filter(f"{partition_col} >= '{cutoff}'")
                 .select(partition_col).distinct().collect())
        return sorted([r[0] for r in parts])

    # --- Iceberg (manifest-based) ---
    @staticmethod
    def get_partitions_iceberg(spark: SparkSession, full_table: str,
                               partition_col: str, lookback_days: int) -> List:
        """Get partition values from Iceberg using metadata tables.
        Iceberg exposes .partitions metadata table — reads manifest only, no data scan.
        """
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")
        parts = spark.sql(f"""
            SELECT DISTINCT partition.{partition_col}
            FROM {full_table}.partitions
        """).collect()
        return sorted([str(r[0]) for r in parts if str(r[0]) >= cutoff])

    # --- Databricks Unity Catalog ---
    @staticmethod
    def get_partitions_databricks(spark: SparkSession, catalog: str, schema: str,
                                   table: str, partition_col: str, lookback_days: int) -> List:
        """Databricks Unity Catalog — Delta tables under the hood.
        Uses DESCRIBE TABLE + data skipping. Liquid Clustering tables don't need
        explicit partition listing — just filter and let Photon optimize.
        """
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")
        full_table = f"{catalog}.{schema}.{table}"
        parts = (spark.table(full_table)
                 .filter(f"{partition_col} >= '{cutoff}'")
                 .select(partition_col).distinct().collect())
        return sorted([r[0] for r in parts])

    # --- File listing fallback (no catalog — raw S3/ADLS/GCS) ---
    @staticmethod
    def get_partitions_from_path(path: str, partition_col: str = "data_dt",
                                 platform: str = "aws") -> List:
        """List partitions from file system when no catalog is available.
        Use as LAST RESORT — slower than catalog-based approaches.
        """
        if platform == "aws":
            import boto3
            s3 = boto3.client("s3")
            bucket, prefix = path.replace("s3://", "").split("/", 1)
            result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
            prefixes = [p["Prefix"] for p in result.get("CommonPrefixes", [])]
            return sorted([p.split("=")[-1].rstrip("/") for p in prefixes if "=" in p])
        elif platform == "databricks":
            # dbutils.fs.ls — only works inside Databricks runtime
            # from pyspark.dbutils import DBUtils
            # dbutils = DBUtils(spark)
            # return [f.name.split("=")[-1].rstrip("/") for f in dbutils.fs.ls(path) if "=" in f.name]
            raise NotImplementedError("Use inside Databricks runtime with dbutils")
        elif platform == "gcp":
            # from google.cloud import storage
            raise NotImplementedError("Use google-cloud-storage client")
        return []

    # --- Drop partitions (platform-aware) ---
    @staticmethod
    def drop_partitions(spark: SparkSession, database: str, table: str,
                        partition_col: str, values: List[str], platform: str = "hive"):
        """Drop specific partitions. Platform-specific behavior."""
        if platform == "hive":
            for val in values:
                spark.sql(f"ALTER TABLE {database}.{table} DROP IF EXISTS PARTITION ({partition_col}='{val}')")
        elif platform == "delta":
            # Delta: DELETE rows matching partition (ACID)
            spark.sql(f"DELETE FROM {database}.{table} WHERE {partition_col} IN ({','.join(repr(v) for v in values)})")
        elif platform == "iceberg":
            # Iceberg: same as Delta
            spark.sql(f"DELETE FROM {database}.{table} WHERE {partition_col} IN ({','.join(repr(v) for v in values)})")
        elif platform == "databricks":
            # Same as Delta
            spark.sql(f"DELETE FROM {database}.{table} WHERE {partition_col} IN ({','.join(repr(v) for v in values)})")

    # --- Coalesce output (universal) ---
    @staticmethod
    def coalesce_output(df: DataFrame, target_file_size_mb: int = 128,
                        platform: str = "spark") -> DataFrame:
        """
        Coalesce output to prevent small files problem.

        Platform-specific alternatives:
            - Spark: .coalesce(N) or .repartition(N) before write
            - Delta: OPTIMIZE command AFTER write (recommended — decouple write from compaction)
            - Iceberg: rewrite_data_files procedure
            - Databricks: Auto Optimize + Auto Compact (set once, never think about it)

        If on Databricks with Auto Optimize enabled, skip this entirely.
        """
        if platform == "databricks":
            # Databricks handles this automatically with optimizeWrite=true
            # Just return as-is
            return df
        elif platform in ("delta", "iceberg"):
            # For Delta/Iceberg: write many files, then OPTIMIZE separately
            # This is preferred over coalesce (avoids shuffle before write)
            return df
        else:
            # Spark native / Glue: coalesce to avoid small files
            num_partitions = max(1, df.rdd.getNumPartitions())
            # Heuristic: aim for ~128MB per file (assuming ~100 bytes/row)
            target_partitions = max(1, num_partitions // 4)
            return df.coalesce(target_partitions)


class DataOptimizer:
    """
    In-code decision helpers for FILE SIZING and SKEW (salting).

    Wire these into every layer job (silver / gold / consumption) right before the
    write, so the sizing/skew decision lives with the pipeline and is auditable.

    See docs/architecture/PARTITIONING_FILE_SIZING_AND_TABLE_FORMATS.md for the
    full decision guide (§3 file sizing, §4 skew & salting).

    Golden numbers:
        target file size    : 256 MB (accept 128 MB – 1 GB)
        skew_ratio          : > 3   → significant skew
        null_pct on key     : > 80% → treat as skew (salt or filter-and-union)
    """

    TARGET_FILE_BYTES = 256 * 1024 * 1024      # 256 MB
    SKEW_RATIO_THRESHOLD = 3.0
    NULL_PCT_THRESHOLD = 80.0

    # ------------------------------------------------------------------ #
    # FILE SIZING (size-aware — replaces the crude num_partitions // 4)  #
    # ------------------------------------------------------------------ #
    @staticmethod
    def right_size_output(df: DataFrame,
                          target_file_bytes: int = TARGET_FILE_BYTES,
                          avg_row_bytes: Optional[int] = None,
                          row_count: Optional[int] = None,
                          platform: str = "spark") -> DataFrame:
        """
        Resize output partitions to hit ~target_file_bytes per file.

        DECISION (in-code):
            target_files = ceil(total_bytes / target_file_bytes)
            if current > target_files * 2 : coalesce(target_files)   # shrink, no shuffle
            elif current < target_files    : repartition(target_files) # grow, shuffle
            else                           : leave as-is

        For Delta/Iceberg/Databricks: DON'T shuffle before write — return df unchanged
        and rely on target-file-size property + OPTIMIZE/rewrite_data_files/Auto Optimize.

        Args:
            avg_row_bytes : bytes/row estimate. If None, defaults to 200 (wide rows).
                            Feed job_optimizer's row-size estimate here when available.
            row_count     : pass a known/estimated count to AVOID a full-scan .count().
                            If None, falls back to current Spark partition count only.
        """
        if platform in ("delta", "iceberg", "databricks"):
            # Table formats size files via table properties + compaction (see §3/§8).
            return df

        current = max(1, df.rdd.getNumPartitions())

        # Only compute a size-based target when we have a row_count WITHOUT scanning.
        if row_count is not None:
            rb = avg_row_bytes if avg_row_bytes else 200
            total_bytes = max(1, row_count * rb)
            target_files = max(1, -(-total_bytes // target_file_bytes))  # ceil div
        else:
            # No count available → conservative shrink only (never explode partitions).
            target_files = max(1, current // 4)

        if current > target_files * 2:
            return df.coalesce(int(target_files))          # shrink: cheap, no shuffle
        if current < target_files:
            return df.repartition(int(target_files))       # grow: needs shuffle
        return df                                          # already about right

    # ------------------------------------------------------------------ #
    # SKEW DETECTION                                                      #
    # ------------------------------------------------------------------ #
    @classmethod
    def detect_skew(cls, df: DataFrame, key_cols: List[str]) -> dict:
        """
        Profile skew on key_cols. Returns a decision dict:
            {skew_ratio, null_pct, is_skewed, recommend_salt, reason}

        NOTE: this triggers a shuffle+aggregation. Run during a tuning pass, not on
        every production run. Cache the verdict in config once known.
        """
        key = key_cols[0] if len(key_cols) == 1 else F.concat_ws("|", *key_cols)
        grp = df.groupBy(key.alias("_k") if hasattr(key, "alias") else key)
        counts = grp.count()
        stats = counts.agg(
            F.max("count").alias("mx"),
            F.avg("count").alias("av"),
        ).head(1)[0]
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
            # Recommend salt only when AQE is likely insufficient (heavy skew / null-heavy).
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

        Adds a random _salt bucket to the large side and explodes the small side across
        all buckets, so the hot key is spread over salt_n reducers.
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
        Stage 1: aggregate by (group + salt). Stage 2: aggregate away the salt.
        Only SUM/COUNT/MIN/MAX are safe to two-stage (associative). NOT for AVG/median.
        """
        fn = getattr(F, agg_fn)
        salted = df.withColumn("_salt", (F.rand() * salt_n).cast("int"))
        stage1 = salted.groupBy(*group_cols, "_salt").agg(fn(agg_col).alias("_partial"))
        return stage1.groupBy(*group_cols).agg(fn("_partial").alias(agg_col))


# ============================================================================
# 7. BEST PRACTICES REFERENCE — Data Engineering Rules (Platform-Agnostic)
# ============================================================================
"""
═══════════════════════════════════════════════════════════════════════════════
DATA ENGINEERING BEST PRACTICES — Universal Rules Across All Platforms
═══════════════════════════════════════════════════════════════════════════════

1. NEVER use .count() to check emptiness
   ✅ df.isEmpty() or len(df.head(1)) == 0
   ❌ df.count() == 0  (full scan!)

2. ALWAYS use early exit before expensive operations
   ✅ Check if source has data BEFORE reading dimensions, doing joins, etc.

3. NEVER hardcode environment-specific values
   ✅ ${environment}, ${account_id} rendered at deploy time
   ✅ Config classes / env vars / DDB parameters

4. ALWAYS round floats before writing
   ✅ round(col, 2) for all double/float columns as last step

5. NEVER write small files (< 10MB)
   ✅ Coalesce before write (Spark native)
   ✅ Or use OPTIMIZE after write (Delta/Iceberg)
   ✅ Or enable Auto Optimize (Databricks)

6. ALWAYS use dynamic partition overwrite (not full overwrite)
   ✅ spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
   ✅ Delta: .mode("overwrite").option("replaceWhere", "mnth_id = 202606")

7. NEVER let DQ checks crash the pipeline
   ✅ Try/except → warn + skip
   ❌ raise Exception("DQ failed")

8. ALWAYS track metadata/watermarks
   ✅ Know what's been processed → skip reprocessing
   ✅ Delta: _delta_log gives this for free (time travel)
   ✅ Others: explicit watermark storage

9. NEVER read more data than needed
   ✅ Partition pruning (push-down predicates)
   ✅ Column pruning (select only needed columns)
   ✅ Delta/Iceberg: data skipping (automatic from file stats)

10. ALWAYS test with 2+ months/partitions
    ✅ Window functions break with single partition
    ✅ MoM/YoY calculations need history

═══════════════════════════════════════════════════════════════════════════════
PLATFORM-SPECIFIC EQUIVALENTS
═══════════════════════════════════════════════════════════════════════════════

┌──────────────────────┬─────────────────────┬─────────────────────┬─────────────────────┐
│ Concept              │ AWS Glue            │ Databricks          │ Open-Source Spark   │
├──────────────────────┼─────────────────────┼─────────────────────┼─────────────────────┤
│ Job configuration    │ DynamoDB + Lambda   │ Workflow Parameters │ Airflow Variables   │
│ Catalog              │ Glue Data Catalog   │ Unity Catalog       │ Hive Metastore      │
│ Read source          │ create_dynamic_frame│ spark.table()       │ spark.read          │
│ Write target         │ saveAsTable         │ .saveAsTable()      │ .write.parquet()    │
│ Schema sync          │ MSCK REPAIR TABLE   │ Automatic (managed) │ MSCK REPAIR TABLE   │
│ Partition pruning    │ push_down_predicate │ .filter() (auto)    │ .filter()           │
│ File compaction      │ Manual coalesce     │ Auto Optimize       │ Manual / Cron       │
│ Scheduling           │ EventBridge         │ Workflows / Triggers│ Airflow / Cron      │
│ Orchestration        │ Step Functions      │ Workflows (DAG)     │ Airflow DAG         │
│ Secret management    │ Secrets Manager     │ Scope/Secrets       │ Vault / env vars    │
│ DQ framework         │ Glue DQ / custom    │ Delta Expectations  │ Great Expectations  │
│ Monitoring           │ CloudWatch          │ Ganglia / Custom    │ Prometheus/Grafana  │
│ CI/CD                │ CodePipeline/GH     │ Repos + Bundles     │ GitHub Actions      │
│ Feature Store        │ SageMaker FS        │ Feature Engineering │ Feast               │
│ ML Training          │ SageMaker           │ MLflow + AutoML     │ MLflow + KubeFlow   │
│ Model Registry       │ SM Model Registry   │ MLflow Registry     │ MLflow Registry     │
│ Streaming            │ Kinesis → Glue      │ Structured Streaming│ Kafka + Spark SS    │
│ Table format         │ Parquet/Iceberg     │ Delta (default)     │ Delta/Iceberg/Hudi  │
│ Cost optimization    │ Spot / Auto Scale   │ Photon / Serverless │ Spot + right-sizing │
│ Governance           │ Lake Formation      │ Unity Catalog       │ Apache Ranger       │
│ Lineage              │ Manual / OpenLineage│ Unity Catalog       │ OpenLineage/Marquez │
└──────────────────────┴─────────────────────┴─────────────────────┴─────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
DATABRICKS-SPECIFIC BEST PRACTICES (if using Databricks)
═══════════════════════════════════════════════════════════════════════════════

1. Use Unity Catalog (3-level namespace: catalog.schema.table)
2. Use Delta Lake (default format — don't override to Parquet)
3. Enable Photon (10x faster for aggregations, joins, writes)
4. Enable Auto Optimize (optimizeWrite=true, autoCompact=true)
5. Use Liquid Clustering (replaces Z-ORDER in DBR 13.3+)
6. Use Structured Streaming for incremental (trigger.availableNow)
7. Use CLONE for zero-copy test environments
8. Use Delta Live Tables for declarative pipelines
9. Use Workflows (not notebooks) for production
10. Use dbutils.widgets for parameterization (not sys.argv)

═══════════════════════════════════════════════════════════════════════════════
OPEN-SOURCE / SELF-MANAGED SPARK BEST PRACTICES
═══════════════════════════════════════════════════════════════════════════════

1. Pin Spark version in requirements (don't rely on cluster default)
2. Use Iceberg or Delta for table format (not raw Parquet for prod)
3. Use Airflow or Dagster for orchestration (not cron)
4. Use Great Expectations or Soda for DQ
5. Use Feast for Feature Store (or build simple Parquet-based one)
6. Use MLflow for experiment tracking + model registry
7. Use S3/GCS/ADLS for storage (not HDFS — unless on-prem)
8. Monitor with Spark UI + Prometheus exporter + Grafana
9. Use Hive Metastore or Nessie (for Iceberg) as catalog
10. Auto-scale with YARN/K8s dynamic allocation

═══════════════════════════════════════════════════════════════════════════════
"""

"""
================================================================================
DATABRICKS ETL JOB TEMPLATE — Bronze Layer  [Databricks / Delta / Unity Catalog]
================================================================================
Purpose: Databricks twin of bronze_job_template.py. Lands raw source → Bronze
         Delta table. Keeps data AS-IS + ingestion lineage. Append-only.

Two ingestion styles supported (pick in _read_raw):
    A) Autoloader (cloudFiles)  — incremental file ingestion (recommended)
    B) Batch read               — one-shot read of a path/JDBC

Pattern:
    1. Read raw (Autoloader stream OR batch)
    2. Add ingestion lineage (_ingest_ts, _ingest_date, _source_file)
    3. (Light) raw DQ — warn+skip
    4. Write to Bronze Delta table (append), partitioned by _ingest_date

Customize (search "CHANGE_ME" / "TODO"):
    - WIDGETS / config values (catalog, schema, table, source path)
    - _define_source()      : source location + format
    - _read_raw()           : Autoloader vs batch
    - _add_audit_columns()  : lineage columns

Config (Databricks job parameters / widgets — NOT CLI args):
    catalog, schema, table, source_path, source_format,
    checkpoint_path (Autoloader), mode (append), use_autoloader (true/false)

Platform notes:
    - Databricks Runtime 15.x LTS, Delta 3.x, Unity Catalog.
    - Target written as a 3-level UC name: `catalog.schema.table`.
    - AWS twin: bronze_job_template.py (Glue → Parquet + Glue Catalog).

────────────────────────────────────────────────────────────────────────────
SQL EQUIVALENT (if you prefer pure SQL in a Databricks SQL/notebook cell):
────────────────────────────────────────────────────────────────────────────
-- Autoloader via SQL (Databricks):
CREATE OR REFRESH STREAMING TABLE catalog.schema.bronze_sales       -- CHANGE_ME
AS
SELECT
    *,
    current_timestamp()                       AS _ingest_ts,
    date_format(current_date(), 'yyyyMMdd')   AS _ingest_date,
    _metadata.file_path                       AS _source_file,
    'sales_raw'                               AS _source_system     -- CHANGE_ME
FROM STREAM read_files(
    's3://CHANGE_ME/raw/sales/',                                    -- CHANGE_ME
    format => 'parquet'                                            -- CHANGE_ME
);

-- One-time batch insert via SQL:
INSERT INTO catalog.schema.bronze_sales
SELECT *, current_timestamp(), date_format(current_date(),'yyyyMMdd'),
       _metadata.file_path, 'sales_raw'
FROM parquet.`s3://CHANGE_ME/raw/sales/`;
────────────────────────────────────────────────────────────────────────────
Version : 2026-06-28 — Databricks twin of Bronze template
================================================================================
"""
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("bronze_etl_databricks")

# In a Databricks notebook/job, `spark` and `dbutils` are pre-injected.
spark = SparkSession.builder.getOrCreate()


# ============================================================================
# DATA QUALITY — warn+skip (Databricks flavor)
# ============================================================================
class DataQualityManager:
    """Lightweight DQ: warn instead of crash. For full DQ use Delta Live Tables
    expectations (EXPECT ... ON VIOLATION) — see src/common/validations."""

    def validate(self, df: DataFrame, checks: dict | None) -> bool:
        """Run lightweight, real assertions. `checks` example:
            {"not_null": ["id", "event_ts"], "min_rows": 1, "unique": ["id"]}
        Returns True if all pass, False otherwise (warn-only; caller decides)."""
        if not checks:
            return True
        ok = True

        # min_rows
        min_rows = checks.get("min_rows")
        if min_rows is not None:
            n = df.count()
            if n < min_rows:
                logger.warning(f"DQ min_rows FAILED: {n} < {min_rows}")
                ok = False

        # not_null columns
        for col in checks.get("not_null", []):
            nulls = df.filter(F.col(col).isNull()).count()
            if nulls > 0:
                logger.warning(f"DQ not_null FAILED: column '{col}' has {nulls} nulls")
                ok = False

        # unique columns (no duplicate keys)
        for col in checks.get("unique", []):
            total = df.count()
            distinct = df.select(col).distinct().count()
            if total != distinct:
                logger.warning(f"DQ unique FAILED: '{col}' has {total - distinct} duplicates")
                ok = False

        logger.info(f"DQ checks {'passed' if ok else 'FAILED'}: {list(checks.keys())}")
        return ok


# ============================================================================
# BASE BRONZE JOB (Databricks)
# ============================================================================
class BaseBronzeJobDatabricks:
    """
    Databricks Bronze base class. Extend and override:
        - _define_source()    -> source location + format
        - _add_audit_columns()-> lineage (usually keep default)

    Bronze principle: land raw + lineage only. No business logic.
    """

    def __init__(self, cfg: dict):
        """
        cfg keys (read from job params / widgets):
          catalog, schema, table        -> UC target  (CHANGE_ME)
          source_path, source_format    -> raw source (CHANGE_ME)
          checkpoint_path               -> Autoloader checkpoint (CHANGE_ME)
          mode                          -> "append" (Bronze default)
          use_autoloader                -> "true" | "false"
          source_system                 -> lineage label (CHANGE_ME)
        """
        self.cfg = cfg
        self.catalog = cfg["catalog"]
        self.schema = cfg["schema"]
        self.table = cfg["table"]
        self.target = f"{self.catalog}.{self.schema}.{self.table}"
        self.mode = cfg.get("mode", "append")
        self.use_autoloader = str(cfg.get("use_autoloader", "true")).lower() == "true"
        self.dq = DataQualityManager()

        self._configure_spark()
        logger.info(f"Bronze (Databricks) init → target={self.target}, autoloader={self.use_autoloader}")

    def _configure_spark(self):
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # ------------------------------------------------------------------
    # OVERRIDE THESE
    # ------------------------------------------------------------------
    def _define_source(self) -> dict:
        """Return source spec. Override.
        e.g. {"path": "s3://raw/sales/", "format": "parquet"}
        """
        raise NotImplementedError("Override _define_source()")

    def _get_dq_checks(self) -> dict | None:
        return None

    # ------------------------------------------------------------------
    # CORE LOGIC
    # ------------------------------------------------------------------
    def _read_raw(self) -> DataFrame:
        src = self._define_source()
        fmt = src.get("format", "parquet")
        if self.use_autoloader:
            # A) Autoloader — incremental, schema-evolving file ingestion
            logger.info(f"Autoloader reading {src['path']} ({fmt})")
            reader = (spark.readStream.format("cloudFiles")
                      .option("cloudFiles.format", fmt)
                      .option("cloudFiles.schemaLocation", self.cfg["checkpoint_path"])
                      .option("cloudFiles.inferColumnTypes", "true"))
            if fmt == "csv":
                reader = reader.option("header", "true")
            return reader.load(src["path"])
        # B) Batch — one-shot read
        logger.info(f"Batch reading {src['path']} ({fmt})")
        reader = spark.read.option("recursiveFileLookup", "true")
        if fmt == "csv":
            reader = reader.option("header", "true")
        return reader.format(fmt).load(src["path"])

    def _add_audit_columns(self, df: DataFrame) -> DataFrame:
        """Stamp ingestion lineage. _metadata is Databricks' file metadata column."""
        return (df
                .withColumn("_ingest_ts", F.current_timestamp())
                .withColumn("_ingest_date", F.date_format(F.current_date(), "yyyyMMdd"))
                .withColumn("_source_file", F.col("_metadata.file_path"))
                .withColumn("_source_system", F.lit(self.cfg.get("source_system", "CHANGE_ME"))))

    def _write_output(self, df: DataFrame):
        """Write to Bronze Delta table (append), partitioned by _ingest_date."""
        if self.use_autoloader:
            # Streaming write with checkpoint (exactly-once)
            logger.info(f"Streaming write → {self.target}")
            (df.writeStream
               .format("delta")
               .option("checkpointLocation", self.cfg["checkpoint_path"] + "/_write")
               .option("mergeSchema", "true")
               .partitionBy("_ingest_date")
               .trigger(availableNow=True)         # process all available, then stop
               .toTable(self.target))
        else:
            logger.info(f"Batch write → {self.target} (mode={self.mode})")
            (df.write
               .format("delta")
               .mode(self.mode)
               .option("mergeSchema", "true")
               .partitionBy("_ingest_date")
               .saveAsTable(self.target))
        logger.info("Bronze write complete.")

    def run(self):
        try:
            raw_df = self._read_raw()
            bronze_df = self._add_audit_columns(raw_df)
            # Note: for streaming, DQ row checks run inside foreachBatch — see DLT for full DQ.
            if not self.use_autoloader:
                self.dq.validate(bronze_df, self._get_dq_checks())
            self._write_output(bronze_df)
            logger.info("Bronze (Databricks) job completed successfully")
        except Exception as e:
            logger.error(f"Bronze (Databricks) job failed: {e}")
            raise


# ============================================================================
# EXAMPLE IMPLEMENTATION (delete and replace)
# ============================================================================
class BronzeSalesJobDatabricks(BaseBronzeJobDatabricks):
    def _define_source(self):
        return {"path": self.cfg.get("source_path", "s3://CHANGE_ME/raw/sales/"),
                "format": self.cfg.get("source_format", "parquet")}


# ============================================================================
# ENTRY POINT (Databricks job: read params via dbutils widgets)
# ============================================================================
if __name__ == "__main__":
    # In Databricks, replace os.environ defaults with dbutils.widgets.get(...)
    # Example:
    #   dbutils.widgets.text("catalog", "")
    #   cfg = {k: dbutils.widgets.get(k) for k in [...]}
    import os
    cfg = {
        "catalog":        os.environ.get("catalog", "CHANGE_ME"),
        "schema":         os.environ.get("schema", "bronze"),
        "table":          os.environ.get("table", "bronze_sales"),
        "source_path":    os.environ.get("source_path", "s3://CHANGE_ME/raw/sales/"),
        "source_format":  os.environ.get("source_format", "parquet"),
        "checkpoint_path": os.environ.get("checkpoint_path", "s3://CHANGE_ME/_checkpoints/bronze_sales"),
        "mode":           os.environ.get("mode", "append"),
        "use_autoloader": os.environ.get("use_autoloader", "true"),
        "source_system":  os.environ.get("source_system", "sales_raw"),
    }
    BronzeSalesJobDatabricks(cfg).run()

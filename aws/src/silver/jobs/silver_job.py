"""
================================================================================
GLUE ETL JOB TEMPLATE — Silver Layer
================================================================================
Purpose: Template for building Silver-layer ETL jobs (Raw → Silver).
         Handles: source reading, cleansing, dedup, type casting, DQ validation,
         partitioned write.

Pattern:
    1. Parse args from DDB config (via framework Step Function)
    2. Read source data (cross-account Glue Catalog or S3)
    3. Apply transformations (cleanse, cast, derive)
    4. Run Data Quality checks (warn+skip if ruleset missing)
    5. Write to Silver (partitioned Parquet, dynamic overwrite)

Customize:
    - _define_source_schema(): Define expected source columns
    - _apply_transformations(): Your business logic
    - _derive_columns(): Add computed columns (site_code, data_dt, etc.)

Args (from DDB config via framework):
    --TARGET_BUCKET, --TARGET_DATABASE, --TARGET_TABLE
    --PARTITION_COLUMN, --LOOKBACK_DAYS, --MODE (append|overwrite)
    --DQ_BUCKET, --source_system
    Framework: --JOB_NAME, --data_date (dl_date from SF)
================================================================================
"""
import sys
import logging
from typing import Optional, List
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("silver_etl")


# ============================================================================
# DATA QUALITY — warn+skip pattern (never crash on missing ruleset)
# ============================================================================
class DataQualityManager:
    """DQ validation that warns instead of crashing if rulesets are missing."""

    def __init__(self, glue_client, glue_context=None):
        self.glue_client = glue_client
        self.glue_context = glue_context   # needed for in-job EvaluateDataQuality

    def get_ruleset(self, ruleset_name: str) -> Optional[str]:
        try:
            response = self.glue_client.get_data_quality_ruleset(Name=ruleset_name)
            return str(response["Ruleset"])
        except self.glue_client.exceptions.EntityNotFoundException:
            logger.warning(f"DQ ruleset '{ruleset_name}' not found — skipping")
            return None
        except Exception as e:
            logger.warning(f"Failed to fetch ruleset '{ruleset_name}': {e} — skipping")
            return None

    def validate(self, df: DataFrame, ruleset_name: str) -> bool:
        """Run a Glue Data Quality ruleset against df. Returns True if passed or
        skipped (missing ruleset), False if a rule failed.
        Real implementation using the in-job EvaluateDataQuality transform."""
        ruleset_str = self.get_ruleset(ruleset_name)
        if ruleset_str is None:
            return True  # Skip = pass
        try:
            from awsgluedq.transforms import EvaluateDataQuality
            from awsglue.dynamicframe import DynamicFrame

            dyf = DynamicFrame.fromDF(df, self.glue_context, "dq_input")
            outcomes = EvaluateDataQuality().process_rows(
                frame=dyf,
                ruleset=ruleset_str,
                publishing_options={
                    "dataQualityEvaluationContext": ruleset_name,
                    "enableDataQualityCloudWatchMetrics": True,
                    "enableDataQualityResultsPublishing": True,
                },
            )
            results = outcomes.errorsAsDynamicFrame().toDF()
            failed = [r for r in results.collect() if str(r.asDict().get("Outcome", "")).lower() == "failed"]
            if failed:
                for r in failed:
                    logger.error(f"DQ rule FAILED: {r.asDict().get('Rule')}")
                return False
            logger.info(f"DQ ruleset '{ruleset_name}' passed.")
            return True
        except ImportError:
            logger.warning("awsgluedq not available — running basic fallback DQ (row_count > 0).")
            return len(df.head(1)) > 0


# ============================================================================
# BASE SILVER JOB
# ============================================================================
class BaseSilverJob:
    """
    Base class for Silver ETL jobs. Extend and override:
        - _define_sources() -> list of source configs
        - _apply_transformations(df) -> transformed df
        - _derive_columns(df) -> df with derived columns
    """

    def __init__(self):
        # Parse arguments from DDB config (passed by framework SF)
        self.args = getResolvedOptions(sys.argv, [
            "JOB_NAME",
            "TARGET_BUCKET",
            "TARGET_DATABASE",
            "TARGET_TABLE",
            "PARTITION_COLUMN",
            "LOOKBACK_DAYS",
            "MODE",
            "DQ_BUCKET",
            "source_system",
            "data_date",
        ])

        # Initialize Spark/Glue
        sc = SparkContext()
        self.glueContext = GlueContext(sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(self.args["JOB_NAME"], self.args)

        # Configure Spark
        self._configure_spark()

        # DQ Manager
        import boto3
        self.region = self.args.get("REGION", "ap-southeast-1")
        self.dq_manager = DataQualityManager(boto3.client("glue", region_name=self.region), self.glueContext)

        logger.info(f"Job initialized: {self.args['JOB_NAME']}")
        logger.info(f"Target: {self.args['TARGET_DATABASE']}.{self.args['TARGET_TABLE']}")
        logger.info(f"Mode: {self.args['MODE']}, Lookback: {self.args['LOOKBACK_DAYS']} days")

    def _configure_spark(self):
        """Set Spark configs for performance. Override for custom tuning."""
        configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.sql.autoBroadcastJoinThreshold": "52428800",  # 50MB
        }
        for k, v in configs.items():
            self.spark.conf.set(k, v)

    # ------------------------------------------------------------------
    # OVERRIDE THESE IN YOUR JOB
    # ------------------------------------------------------------------

    def _define_sources(self) -> List[dict]:
        """Define source tables to read. Override this.
        Returns list of dicts: [{"db": "...", "table": "...", "predicate": "..."}]
        """
        raise NotImplementedError("Override _define_sources()")

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """Apply cleansing, casting, filtering. Override this."""
        raise NotImplementedError("Override _apply_transformations()")

    def _derive_columns(self, df: DataFrame) -> DataFrame:
        """Add derived columns (site_code, data_dt, mnth_id). Override this."""
        return df

    def _get_dq_ruleset_name(self) -> Optional[str]:
        """Return DQ ruleset name for this job, or None to skip DQ."""
        return None

    # ------------------------------------------------------------------
    # CORE LOGIC (usually no need to override)
    # ------------------------------------------------------------------

    def _compute_date_filter(self) -> str:
        """Compute date predicate based on LOOKBACK_DAYS and data_date."""
        data_date = self.args.get("data_date", datetime.now().strftime("%Y-%m-%d"))
        lookback = int(self.args["LOOKBACK_DAYS"])
        if lookback == 0:
            return ""  # No filter = full load
        cutoff = (datetime.strptime(data_date[:10], "%Y-%m-%d") - timedelta(days=lookback)).strftime("%Y%m%d")
        return f"{self.args['PARTITION_COLUMN']} >= '{cutoff}'"

    def _read_sources(self) -> DataFrame:
        """Read from Glue Catalog with push-down predicate."""
        sources = self._define_sources()
        date_filter = self._compute_date_filter()

        frames = []
        for src in sources:
            predicate = src.get("predicate", date_filter)
            logger.info(f"Reading {src['db']}.{src['table']} (predicate: {predicate or 'none'})")
            dyf = self.glueContext.create_dynamic_frame.from_catalog(
                database=src["db"],
                table_name=src["table"],
                push_down_predicate=predicate or None,
            )
            frames.append(dyf.toDF())

        if len(frames) == 1:
            return frames[0]
        return frames  # Multi-source: override _apply_transformations to join

    def _write_output(self, df: DataFrame):
        """Write to S3 as partitioned Parquet with Glue Catalog update."""
        target_path = f"s3://{self.args['TARGET_BUCKET']}/{self.args['TARGET_TABLE']}"
        partition_col = self.args["PARTITION_COLUMN"]
        mode = self.args["MODE"]  # "append" or "overwrite"

        logger.info(f"Writing to {target_path} (mode={mode}, partition={partition_col})")

        (df.write
         .mode(mode)
         .partitionBy(partition_col)
         .format("parquet")
         .option("path", target_path)
         .saveAsTable(f"{self.args['TARGET_DATABASE']}.{self.args['TARGET_TABLE']}"))

        logger.info(f"Write complete. Rows: {df.count()}")

    # ------------------------------------------------------------------
    # WRITE STRATEGY — override to change platform (delta, iceberg, etc.)
    # ------------------------------------------------------------------

    def _get_write_strategy(self) -> str:
        """Return write platform. Override to change.
        Options: spark_native, glue_catalog, delta, iceberg, databricks
        """
        return "glue_catalog"  # Default for AWS Glue. Change per platform.

    def run(self):
        """Execute the full ETL pipeline."""
        from aws.src.common.utils.etl_utils import EarlyExitCheck, MetadataFreshnessManager, get_writer, DataOptimizer

        try:
            # 1. Read
            raw_df = self._read_sources()

            # 2. EARLY EXIT — don't process empty data (O(1) check, no .count())
            if EarlyExitCheck.is_empty(raw_df):
                logger.info("No new data from source — exiting early")
                self.job.commit()
                return

            # 3. Transform
            transformed_df = self._apply_transformations(raw_df)

            # 4. Derive columns
            final_df = self._derive_columns(transformed_df)

            # 5. EARLY EXIT — check if we actually produced any rows
            if EarlyExitCheck.is_empty(final_df):
                logger.info("Transformations produced 0 rows — exiting early")
                self.job.commit()
                return

            # 6. Data Quality
            ruleset = self._get_dq_ruleset_name()
            if ruleset:
                self.dq_manager.validate(final_df, ruleset)

            # 7. Write (pluggable strategy)
            writer = get_writer(self._get_write_strategy())
            target_path = f"s3://{self.args['TARGET_BUCKET']}/{self.args['TARGET_TABLE']}"

            # --- FILE SIZING decision (§3 of PARTITIONING_FILE_SIZING_AND_TABLE_FORMATS) ---
            # Right-size output to ~256 MB/file. No-op for delta/iceberg/databricks;
            # coalesces/repartitions for spark_native/glue_catalog. Pass row_count to
            # enable size-based sizing without a full-scan .count().
            final_df = DataOptimizer.right_size_output(
                final_df,
                platform=self._get_write_strategy(),
                row_count=None,
            )
            writer.write(
                final_df, target_path,
                partition_col=self.args["PARTITION_COLUMN"],
                mode=self.args["MODE"],
                database=self.args["TARGET_DATABASE"],
                table=self.args["TARGET_TABLE"],
            )

            self.job.commit()
            logger.info("Job completed successfully")

        except Exception as e:
            logger.error(f"Job failed: {e}", exc_info=True)
            raise


# ============================================================================
# EXAMPLE IMPLEMENTATION (delete and replace with your logic)
# ============================================================================
class SilverSalesJob(BaseSilverJob):
    """Example: Raw sales → Silver sales."""

    def _define_sources(self):
        return [
            {"db": "source_central_data", "table": "raw_sales_day"},
        ]

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        return (
            df
            .filter(F.col("record_date").isNotNull())
            .dropDuplicates(["transaction_id"])
            .withColumn("amount", F.col("amount").cast(T.DecimalType(18, 2)))
        )

    def _derive_columns(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("data_dt", F.date_format("record_date", "yyyyMMdd").cast("int"))
            .withColumn("mnth_id", F.date_format("record_date", "yyyyMM").cast("int"))
        )

    def _get_dq_ruleset_name(self):
        return f"dq-{self.args['TARGET_TABLE']}-completeness"


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    SilverSalesJob().run()

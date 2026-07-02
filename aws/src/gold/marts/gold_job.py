"""
================================================================================
GLUE ETL JOB TEMPLATE — Gold Layer
================================================================================
Purpose: Template for Gold-layer ETL jobs (Silver → Gold).
         Handles: multi-source joins, aggregations, window functions,
         period aggregates (MTD/YTD/WTD), zero-fill, comparison periods.

Pattern:
    1. Read Silver tables + dimension tables
    2. Apply business aggregations (GROUP BY grain)
    3. Compute period aggregates via window functions
    4. Compute comparison periods (MoM, YoY, WoW)
    5. Zero-fill with dimension skeleton (active entities only)
    6. Round all float columns to 2dp
    7. Write to Gold (partitioned Parquet)

Key Patterns (from production experience):
    - Window PARTITION BY must include ALL dimensions (Bug Pattern 2)
    - Group-level columns must be deduplicated before SUM (Bug Pattern 5)
    - Rates/ratios: period agg = ratio-of-sums, NOT sum-of-ratios (Bug Pattern 10)
    - Dimension filter at Gold ensures consumption matches (Bug Pattern 11)
================================================================================
"""
import sys
import logging
from typing import List, Dict

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("gold_etl")


class BaseGoldJob:
    """
    Base class for Gold ETL jobs. Override:
        - _define_sources(): source tables
        - _build_daily_aggregates(silver_df, dim_df): core aggregation
        - _build_period_aggregates(daily_df): MTD/YTD/WTD windows
        - _build_comparison_periods(df): MoM/YoY/WoW
    """

    # Define your grain (primary key) columns
    GRAIN_COLUMNS: List[str] = ["site_code", "data_dt"]  # CHANGE_ME
    PARTITION_COLUMN: str = "mnth_id"

    def __init__(self):
        self.args = getResolvedOptions(sys.argv, [
            "JOB_NAME", "TARGET_BUCKET", "TARGET_DATABASE", "TARGET_TABLE",
            "PARTITION_COLUMN", "LOOKBACK_DAYS", "MODE", "FORCE_RUN",
            "data_date",
        ])
        sc = SparkContext()
        self.glueContext = GlueContext(sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(self.args["JOB_NAME"], self.args)
        self._configure_spark()

    def _configure_spark(self):
        """Tuned for Gold aggregation workloads."""
        configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.shuffle.partitions": "200",  # Tune: (data_gb * 1000) / 128
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.sql.autoBroadcastJoinThreshold": "52428800",
        }
        for k, v in configs.items():
            self.spark.conf.set(k, v)

    # ------------------------------------------------------------------
    # OVERRIDE THESE
    # ------------------------------------------------------------------

    def _define_sources(self) -> Dict[str, dict]:
        """Define source tables. Return dict keyed by role.
        Example: {"silver": {"db": "...", "table": "..."}, "dimension": {"db": "...", "table": "..."}}
        """
        raise NotImplementedError

    def _build_daily_aggregates(self, silver_df: DataFrame, dim_df: DataFrame) -> DataFrame:
        """Core aggregation: GROUP BY grain columns. Override this."""
        raise NotImplementedError

    def _build_period_aggregates(self, daily_df: DataFrame) -> DataFrame:
        """Add MTD/YTD/WTD running totals. Override or use helper below."""
        return daily_df

    def _build_comparison_periods(self, df: DataFrame) -> DataFrame:
        """Add MoM/YoY/WoW comparisons. Override or use helper below."""
        return df

    # ------------------------------------------------------------------
    # HELPERS (use in your overrides)
    # ------------------------------------------------------------------

    def add_running_totals(self, df: DataFrame, metrics: List[str],
                           partition_cols: List[str], order_col: str = "data_dt") -> DataFrame:
        """Add MTD/YTD/WTD running sums for metric columns.

        IMPORTANT: partition_cols must include ALL dimensions in the grain.
        Missing a dimension = Bug Pattern 2 (wrong values across groups).
        """
        # MTD window
        w_mtd = (Window.partitionBy(*partition_cols, "mnth_id")
                 .orderBy(order_col)
                 .rowsBetween(Window.unboundedPreceding, 0))
        # YTD window
        w_ytd = (Window.partitionBy(*partition_cols, "tm_key_yr")
                 .orderBy(order_col)
                 .rowsBetween(Window.unboundedPreceding, 0))

        for col_name in metrics:
            df = df.withColumn(f"mtd_{col_name}", F.sum(col_name).over(w_mtd))
            df = df.withColumn(f"ytd_{col_name}", F.sum(col_name).over(w_ytd))
        return df

    def add_rate_period_aggregates(self, df: DataFrame, numerator: str, denominator: str,
                                   rate_col: str, partition_cols: List[str]) -> DataFrame:
        """Compute period rate as ratio-of-sums (NOT sum-of-rates — Bug Pattern 10).

        Example: MTD breach rate = SUM(daily_breaches) / SUM(daily_closed)
        """
        w_mtd = (Window.partitionBy(*partition_cols, "mnth_id")
                 .orderBy("data_dt").rowsBetween(Window.unboundedPreceding, 0))

        df = df.withColumn(f"mtd_{rate_col}",
                           F.sum(numerator).over(w_mtd) / F.when(
                               F.sum(denominator).over(w_mtd) != 0,
                               F.sum(denominator).over(w_mtd)
                           ))
        return df

    def zero_fill_with_dimension(self, fact_df: DataFrame, dim_df: DataFrame,
                                 dim_key: str, date_col: str = "data_dt",
                                 metric_columns: List[str] = None) -> DataFrame:
        """Zero-fill: ensures all dimension entities appear for every date.

        Pattern: CROSS JOIN (dim × dates) LEFT JOIN fact → fill nulls with 0.
        Only active entities (from dimension latest partition) are included.
        """
        # Get all dates from fact
        dates_df = fact_df.select(date_col, "mnth_id").distinct()

        # Cross join: every dim entity × every date
        skeleton = dim_df.crossJoin(F.broadcast(dates_df))

        # Left join fact onto skeleton
        join_cols = [dim_key, date_col]
        result = skeleton.join(fact_df, on=join_cols, how="left")

        # Fill null metrics with 0
        if metric_columns:
            result = result.fillna(0, subset=metric_columns)

        return result

    def round_all_floats(self, df: DataFrame, decimal_places: int = 2) -> DataFrame:
        """Round all double/float columns to N decimal places (Bug Pattern 4)."""
        for field in df.schema.fields:
            if field.dataType in (F.DoubleType(), F.FloatType()) or "double" in str(field.dataType).lower():
                df = df.withColumn(field.name, F.round(F.col(field.name), decimal_places))
        return df

    def read_dimension_latest_partition(self, db: str, table: str, key_col: str) -> DataFrame:
        """Read latest partition of a dimension/mapping table."""
        partitions = self.spark.sql(f"SHOW PARTITIONS {db}.{table}").collect()
        latest = sorted(r[0].split("=")[1] for r in partitions)[-1]
        partition_col = partitions[0][0].split("=")[0]
        logger.info(f"Dimension {db}.{table} latest partition: {latest}")

        dyf = self.glueContext.create_dynamic_frame.from_catalog(
            database=db, table_name=table,
            push_down_predicate=f"{partition_col} = '{latest}'"
        )
        return (dyf.toDF()
                .filter(F.col(key_col).isNotNull())
                .select(F.col(key_col)).dropDuplicates([key_col]))

    # ------------------------------------------------------------------
    # EXECUTION
    # ------------------------------------------------------------------

    def _write_output(self, df: DataFrame):
        target_path = f"s3://{self.args['TARGET_BUCKET']}/{self.args['TARGET_TABLE']}"
        partition_col = self.args["PARTITION_COLUMN"]

        # Round all floats before write
        df = self.round_all_floats(df)

        logger.info(f"Writing Gold to {target_path}")
        (df.write.mode("overwrite")
         .partitionBy(partition_col)
         .format("parquet")
         .option("path", target_path)
         .saveAsTable(f"{self.args['TARGET_DATABASE']}.{self.args['TARGET_TABLE']}"))

    def run(self):
        """Execute Gold pipeline with metadata freshness check and pluggable write."""
        from aws.src.common.utils.etl_utils import EarlyExitCheck, MetadataFreshnessManager, get_writer, DataOptimizer

        try:
            sources = self._define_sources()

            # Read silver
            silver_dyf = self.glueContext.create_dynamic_frame.from_catalog(
                database=sources["silver"]["db"],
                table_name=sources["silver"]["table"],
            )
            silver_df = silver_dyf.toDF()

            # EARLY EXIT — no data in silver (O(1), no .count())
            if EarlyExitCheck.is_empty(silver_df):
                logger.info("Silver table is empty — exiting early")
                self.job.commit()
                return

            # METADATA FRESHNESS — skip if Gold already has latest Silver data
            # Gold is often NOT at daily granularity (monthly aggregates).
            # Compare: Silver max(data_dt) vs Gold watermark.
            fm = MetadataFreshnessManager(
                self.spark, storage="s3",
                bucket=self.args["TARGET_BUCKET"].split("/")[0],
                prefix="metadata/watermarks/",
            )
            if self.args.get("FORCE_RUN", "false").lower() != "true":
                if fm.is_fresh(self.args["TARGET_TABLE"], silver_df, date_col="data_dt"):
                    logger.info("Gold already has latest Silver data — exiting early")
                    self.job.commit()
                    return

            # Read dimension (if defined)
            dim_df = None
            if "dimension" in sources:
                dim_df = self.read_dimension_latest_partition(
                    sources["dimension"]["db"],
                    sources["dimension"]["table"],
                    sources["dimension"]["key_col"],
                )

            # Build Gold
            daily_df = self._build_daily_aggregates(silver_df, dim_df)

            # EARLY EXIT — aggregation produced nothing
            if EarlyExitCheck.is_empty(daily_df):
                logger.info("Aggregation produced 0 rows — exiting early")
                self.job.commit()
                return

            period_df = self._build_period_aggregates(daily_df)
            final_df = self._build_comparison_periods(period_df)

            # Write (pluggable strategy)
            writer = get_writer(self._get_write_strategy())
            target_path = f"s3://{self.args['TARGET_BUCKET']}/{self.args['TARGET_TABLE']}"

            # Round all floats before write (Bug Pattern 4)
            final_df = self.round_all_floats(final_df)

            # --- FILE SIZING decision (§3 of PARTITIONING_FILE_SIZING_AND_TABLE_FORMATS) ---
            # Right-size output to ~256 MB/file. No-op for delta/iceberg/databricks
            # (those size via target-file-size property + OPTIMIZE/rewrite/Auto Optimize).
            # For spark_native/glue_catalog it coalesces/repartitions to hit the target.
            # Pass row_count (e.g. from job_optimizer) to enable size-based sizing without
            # a full-scan .count(); left None = conservative shrink only.
            final_df = DataOptimizer.right_size_output(
                final_df,
                platform=self._get_write_strategy(),
                row_count=None,
            )
            # SKEW/SALTING (§4): if this job has a known-skewed join/group key, profile once
            # with DataOptimizer.detect_skew(...) during tuning, then apply
            # DataOptimizer.salt_join(...) / salt_aggregate(...) only if recommend_salt.
            # Rely on AQE skewJoin (enabled in _configure_spark) for moderate skew.

            writer.write(
                final_df, target_path,
                partition_col=self.args["PARTITION_COLUMN"],
                mode="overwrite",
                database=self.args["TARGET_DATABASE"],
                table=self.args["TARGET_TABLE"],
            )

            # Update watermark after successful write
            max_dt = silver_df.agg(F.max("data_dt")).head(1)[0][0]
            if max_dt:
                fm.update_watermark(self.args["TARGET_TABLE"], str(max_dt))

            self.job.commit()
            logger.info("Gold job completed successfully")

        except Exception as e:
            logger.error(f"Gold job failed: {e}", exc_info=True)
            raise

    def _get_write_strategy(self) -> str:
        """Override to change write platform (delta, iceberg, databricks, etc.)."""
        return "glue_catalog"


# ============================================================================
# EXAMPLE IMPLEMENTATION
# ============================================================================
class GoldTicketingSummaryJob(BaseGoldJob):
    """Example: Silver ticketing → Gold ticketing summary (site × day grain)."""

    GRAIN_COLUMNS = ["site_code", "data_dt"]

    def _define_sources(self):
        return {
            "silver": {"db": "my_analytics_silver", "table": "silver_ticketing"},
            "dimension": {"db": "my_analytics_silver", "table": "cellsite_mapping", "key_col": "std_site_nm"},
        }

    def _build_daily_aggregates(self, silver_df, dim_df):
        # Filter to active sites only (Bug Pattern 11: filter early)
        silver_df = silver_df.join(
            F.broadcast(dim_df.withColumnRenamed("std_site_nm", "site_code")),
            on="site_code", how="inner"
        )
        # Aggregate
        return (silver_df
                .groupBy("site_code", "data_dt", "mnth_id")
                .agg(
                    F.countDistinct("ticketid").alias("daily_closed_tickets"),
                    F.sum("down_time").alias("daily_downtime_minutes"),
                ))

    def _build_period_aggregates(self, daily_df):
        return self.add_running_totals(
            daily_df,
            metrics=["daily_closed_tickets", "daily_downtime_minutes"],
            partition_cols=["site_code"],
        )


if __name__ == "__main__":
    GoldTicketingSummaryJob().run()

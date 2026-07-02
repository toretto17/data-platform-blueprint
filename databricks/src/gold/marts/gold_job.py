"""
================================================================================
ETL JOB TEMPLATE — Gold Layer  [Databricks / Delta / Unity Catalog]
================================================================================
Purpose: Databricks twin of aws/src/gold/marts/gold_job.py (Silver → Gold).
         Business aggregations, window/period aggregates (MTD/YTD/WTD),
         comparison periods (MoM/YoY), zero-fill, round, write.

Key DE patterns carried over (from production bug-fixes):
    - Window PARTITION BY must include ALL dimensions (else cross-contamination)
    - Group-level columns must be deduped before SUM
    - Rates/ratios: period aggregate = ratio-of-sums, NOT sum-of-ratios
    - Round float cols before write

Customize:
    - _read_silver()             : source silver table(s)
    - _build_daily_aggregates()  : core GROUP BY
    - _build_period_aggregates() : MTD/YTD/WTD windows
    - _build_comparison_periods(): MoM/YoY
    - _dq_config()

Platform notes: DBR 15.x LTS+, Delta 3.x, UC. AWS twin: aws/src/gold/marts/gold_job.py
Version : 2026-06-28
================================================================================
"""
import logging
from typing import List

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from databricks.src.common.utils.etl_utils import EarlyExitCheck, get_writer, DataOptimizer
from databricks.src.common.validations.dq_framework import DataQualityFramework, DQConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("gold_etl_databricks")
spark = SparkSession.builder.getOrCreate()


class BaseGoldJobDatabricks:
    GRAIN_COLUMNS: List[str] = ["site_code", "data_dt"]   # CHANGE_ME
    PARTITION_COLUMN: str = "mnth_id"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.target_table = cfg["target_table"]            # CHANGE_ME e.g. main.gold.sales_mart
        self.mode = cfg.get("mode", "overwrite")
        self._configure_spark()
        self.dq = DataQualityFramework(spark)
        logger.info(f"Gold(Databricks) init → {self.target_table}")

    def _configure_spark(self):
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    # ---- OVERRIDE ----
    def _read_silver(self) -> DataFrame:
        raise NotImplementedError("Override _read_silver()")

    def _build_daily_aggregates(self, silver: DataFrame) -> DataFrame:
        raise NotImplementedError("Override _build_daily_aggregates()")

    def _build_period_aggregates(self, daily: DataFrame) -> DataFrame:
        """Default: pass-through. Override to add MTD/YTD windows.
        IMPORTANT: partition windows by ALL grain dims (not just date)."""
        return daily

    def _build_comparison_periods(self, df: DataFrame) -> DataFrame:
        return df

    def _dq_config(self) -> DQConfig | None:
        return None

    # ---- CORE ----
    def _round_floats(self, df: DataFrame, dp: int = 2) -> DataFrame:
        for f in df.schema.fields:
            if f.dataType.typeName() in ("double", "float"):
                df = df.withColumn(f.name, F.round(F.col(f.name), dp))
        return df

    def run(self):
        try:
            silver = self._read_silver()
            if EarlyExitCheck.is_empty(silver):
                logger.info("No silver data — early exit.")
                return
            daily = self._build_daily_aggregates(silver)
            period = self._build_period_aggregates(daily)
            final = self._build_comparison_periods(period)
            final = self._round_floats(final)
            if EarlyExitCheck.is_empty(final):
                logger.info("Aggregation produced 0 rows — early exit.")
                return
            cfg = self._dq_config()
            if cfg:
                report = self.dq.validate(final, cfg)
                if report.has_failures:
                    self.dq.publish_metrics(report)
                    from databricks.src.common.exceptions.exceptions import DQError
                    raise DQError(report.summary)
            # --- FILE SIZING decision (§3 of PARTITIONING_FILE_SIZING_AND_TABLE_FORMATS) ---
            # No-op on Delta (Auto Optimize sizes files on write + OPTIMIZE/ZORDER compacts).
            # Kept for symmetry with the AWS tree; flip platform to size manually if needed.
            # SKEW/SALTING (§4): profile once with DataOptimizer.detect_skew(...); apply
            # DataOptimizer.salt_join(...) / salt_aggregate(...) only if recommend_salt.
            final = DataOptimizer.right_size_output(final, platform="databricks")
            get_writer().write(final, self.target_table, partition_col=self.PARTITION_COLUMN, mode=self.mode)
            logger.info("Gold(Databricks) job complete.")
        except Exception as e:
            logger.error(f"Gold(Databricks) job failed: {e}")
            raise


# ============================================================================
# EXAMPLE IMPLEMENTATION (delete + replace)
# ============================================================================
class GoldSalesMartDatabricks(BaseGoldJobDatabricks):
    GRAIN_COLUMNS = ["product", "ch_area", "data_dt"]

    def _read_silver(self):
        return spark.table("main.silver.sales")            # CHANGE_ME

    def _build_daily_aggregates(self, silver: DataFrame) -> DataFrame:
        return (silver.groupBy("product", "ch_area", "data_dt", "mnth_id")
                      .agg(F.sum("amount").alias("daily_amount"),
                           F.countDistinct("transaction_id").alias("daily_txns")))

    def _build_period_aggregates(self, daily: DataFrame) -> DataFrame:
        # MTD example — window partitioned by ALL dims (product, ch_area, mnth_id)
        w = (Window.partitionBy("product", "ch_area", "mnth_id")
             .orderBy("data_dt").rowsBetween(Window.unboundedPreceding, Window.currentRow))
        return daily.withColumn("mtd_amount", F.sum("daily_amount").over(w))


if __name__ == "__main__":
    cfg = {"target_table": "main.gold.sales_mart", "mode": "overwrite"}
    GoldSalesMartDatabricks(cfg).run()

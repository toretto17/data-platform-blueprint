"""
================================================================================
CONSUMPTION JOB TEMPLATE — Gold → Consumption  [Databricks / Delta SQL / UC]
================================================================================
Purpose: Databricks twin of aws/src/consumption/jobs/consumption_job.py.
         Builds the final reporting/serving table consumed by BI (Databricks SQL,
         dashboards) or synced downstream. Supports INITIAL_LOAD (full history)
         vs daily incremental.

Pattern:
    1. Read gold + dimension/geo tables
    2. INITIAL_LOAD: load full history (optionally capped). Daily: load new days.
    3. Join enrichment (dim_time, geo) + apply row-level security columns if any
    4. Write to consumption Delta table (UC) — optionally expose as a UC view

Customize:
    - _read_gold(), _enrich(), _dq_config()
    - INITIAL_LOAD logic / cap

Platform notes: DBR 15.x LTS+, Delta, Unity Catalog. AWS twin uses Athena/Glue.
Version : 2026-06-28
================================================================================
"""
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from databricks.src.common.utils.etl_utils import EarlyExitCheck, get_writer, DataOptimizer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("consumption_databricks")
spark = SparkSession.builder.getOrCreate()


class BaseConsumptionJobDatabricks:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.gold_table = cfg["gold_table"]                # CHANGE_ME main.gold.sales_mart
        self.target_table = cfg["target_table"]            # CHANGE_ME main.consumption.sales_mart
        self.partition_col = cfg.get("partition_col", "mnth_id")
        self.is_initial_load = str(cfg.get("initial_load", "false")).lower() == "true"
        self.expose_view = cfg.get("expose_view")          # optional UC view name
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        logger.info(f"Consumption(Databricks) init → {self.target_table} initial_load={self.is_initial_load}")

    # ---- OVERRIDE ----
    def _read_gold(self) -> DataFrame:
        """Return gold rows. Override to add INITIAL_LOAD cap / daily filter."""
        df = spark.table(self.gold_table)
        if not self.is_initial_load:
            # Daily: only the latest day(s). CHANGE_ME to your incremental predicate.
            latest = df.agg(F.max("data_dt")).collect()[0][0]
            df = df.filter(F.col("data_dt") == latest)
            logger.info(f"daily mode → data_dt = {latest}")
        else:
            logger.info("INITIAL_LOAD → full gold history")
        return df

    def _enrich(self, df: DataFrame) -> DataFrame:
        """Join dims / add reporting columns. Override as needed."""
        return df

    # ---- CORE ----
    def run(self):
        try:
            gold = self._read_gold()
            if EarlyExitCheck.is_empty(gold):
                logger.info("No gold rows — early exit.")
                return
            out = self._enrich(gold)
            mode = "overwrite" if self.is_initial_load else "append"
            # --- FILE SIZING decision (§3) — no-op on Delta (Auto Optimize). Symmetric
            # with AWS tree. SKEW/SALTING (§4): detect_skew() then salt_* if recommend_salt.
            out = DataOptimizer.right_size_output(out, platform="databricks")
            get_writer().write(out, self.target_table, partition_col=self.partition_col, mode=mode)
            if self.expose_view:
                spark.sql(f"CREATE OR REPLACE VIEW {self.expose_view} AS SELECT * FROM {self.target_table}")
                logger.info(f"Exposed UC view {self.expose_view}")
            logger.info("Consumption(Databricks) job complete.")
        except Exception as e:
            logger.error(f"Consumption(Databricks) job failed: {e}")
            raise


# ============================================================================
# EXAMPLE (delete + replace)
# ============================================================================
class ConsumptionSalesMartDatabricks(BaseConsumptionJobDatabricks):
    def _enrich(self, df: DataFrame) -> DataFrame:
        dim_time = spark.table("main.silver.dim_time").select("dt_key", "true_tm_key_wk")  # CHANGE_ME
        return df.join(F.broadcast(dim_time), df["data_dt"] == dim_time["dt_key"], "left").drop("dt_key")


if __name__ == "__main__":
    cfg = {"gold_table": "main.gold.sales_mart", "target_table": "main.consumption.sales_mart",
           "initial_load": "false", "expose_view": "main.consumption.v_sales_mart"}
    ConsumptionSalesMartDatabricks(cfg).run()

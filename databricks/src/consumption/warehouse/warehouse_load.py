"""
================================================================================
WAREHOUSE LOAD — Consumption → Databricks SQL serving  [Databricks]
================================================================================
Purpose: Twin of aws/src/consumption/warehouse/warehouse_load.py. Prepares the
         consumption layer for BI on Databricks SQL. Two serving options:
            • serving_table      : a curated Delta table (BI queries it directly)
            • materialized_view  : a Databricks SQL Materialized View (auto-refresh)
         Plus a UC view for a stable, governed access point.

In the Lakehouse there's no separate warehouse to copy into — Databricks SQL
queries Delta/UC directly. So "warehouse load" here = build the governed serving
object(s) BI tools connect to.

Customize (CHANGE_ME):
    - SOURCE_TABLE (consumption Delta), SERVING_TABLE, VIEW_NAME, MODE
    - LOAD_WINDOW_COL/VAL for idempotent partition refresh

Platform notes: DBR 15.x LTS+, Delta, Unity Catalog, Databricks SQL.
AWS twin loads Amazon Redshift via the Spectrum→native pattern.
Version : 2026-06-28
================================================================================
"""
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("warehouse_load_databricks")
spark = SparkSession.builder.getOrCreate()


class DatabricksSQLServing:
    def __init__(self, cfg: dict):
        self.source_table = cfg["source_table"]          # CHANGE_ME main.consumption.sales_mart
        self.serving_table = cfg["serving_table"]         # CHANGE_ME main.bi.sales_mart
        self.view_name = cfg.get("view_name")             # optional governed view
        self.mode = cfg.get("mode", "serving_table")      # serving_table | materialized_view
        self.window_col = cfg.get("load_window_col")      # e.g. mnth_id
        self.window_val = cfg.get("load_window_val")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    def _build_serving_table(self):
        """Idempotent refresh: dynamic-overwrite only the load window partition."""
        src = spark.table(self.source_table)
        if self.window_col and self.window_val is not None:
            src = src.filter(F.col(self.window_col) == self.window_val)
            (src.write.format("delta").mode("overwrite")
                .option("replaceWhere", f"{self.window_col} = {self.window_val}")
                .saveAsTable(self.serving_table))
            logger.info(f"refreshed {self.serving_table} where {self.window_col}={self.window_val}")
        else:
            (src.write.format("delta").mode("overwrite")
                .option("overwriteSchema", "true").saveAsTable(self.serving_table))
            logger.info(f"full refresh {self.serving_table}")

    def _build_materialized_view(self):
        """Databricks SQL Materialized View — auto-incremental refresh by the platform."""
        spark.sql(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {self.serving_table} AS
            SELECT * FROM {self.source_table}
        """)
        # Trigger a refresh (no-op if already fresh)
        spark.sql(f"REFRESH MATERIALIZED VIEW {self.serving_table}")
        logger.info(f"materialized view refreshed: {self.serving_table}")

    def _expose_view(self):
        if self.view_name:
            spark.sql(f"CREATE OR REPLACE VIEW {self.view_name} AS SELECT * FROM {self.serving_table}")
            logger.info(f"governed view: {self.view_name}")

    def run(self):
        if self.mode == "materialized_view":
            self._build_materialized_view()
        else:
            self._build_serving_table()
        self._expose_view()
        logger.info("Databricks SQL serving prepared.")


if __name__ == "__main__":
    cfg = {"source_table": "main.consumption.sales_mart",   # CHANGE_ME
           "serving_table": "main.bi.sales_mart",           # CHANGE_ME
           "view_name": "main.bi.v_sales_mart",
           "mode": "serving_table"}
    DatabricksSQLServing(cfg).run()

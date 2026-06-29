"""
================================================================================
FULL LOAD — truncate-and-reload / full snapshot overwrite  [Databricks / Delta]
================================================================================
Purpose : Replace the entire target with the current source snapshot. Use when:
            • the source is small, OR
            • there's no reliable watermark/CDC, OR
            • you need a guaranteed clean rebuild.

Two safe variants:
    • overwrite_all      : replace the whole table (atomic with Delta).
    • dynamic_partition  : replace only the partitions present in this load
                           (safer for large partitioned tables — keeps untouched
                           partitions intact).

BOTH styles: PySpark write + SQL (INSERT OVERWRITE).

Customize: SOURCE_TABLE, TARGET_TABLE, PARTITION_COL (for dynamic), MODE.
AWS twin: aws/src/de_patterns/full_load.py
Version : 2026-06-28
================================================================================
"""
import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger("full_load_databricks")
spark = SparkSession.builder.getOrCreate()


class FullLoadDatabricks:
    SOURCE_TABLE: str = "main.bronze.dim_product"   # CHANGE_ME
    TARGET_TABLE: str = "main.silver.dim_product"   # CHANGE_ME
    MODE: str = "overwrite_all"                     # "overwrite_all" | "dynamic_partition"
    PARTITION_COL: Optional[str] = None             # required for dynamic_partition

    def read_source(self) -> DataFrame:
        return spark.table(self.SOURCE_TABLE)

    # ---- PySpark ----
    def write_pyspark(self, df: DataFrame):
        if self.MODE == "overwrite_all":
            (df.write.format("delta").mode("overwrite")
               .option("overwriteSchema", "true").saveAsTable(self.TARGET_TABLE))
            logger.info(f"[pyspark] full overwrite → {self.TARGET_TABLE}")
        elif self.MODE == "dynamic_partition":
            if not self.PARTITION_COL:
                raise ValueError("dynamic_partition requires PARTITION_COL")
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            (df.write.format("delta").mode("overwrite")
               .partitionBy(self.PARTITION_COL).saveAsTable(self.TARGET_TABLE))
            logger.info(f"[pyspark] dynamic partition overwrite ({self.PARTITION_COL}) → {self.TARGET_TABLE}")
        else:
            raise ValueError("MODE must be overwrite_all|dynamic_partition")

    # ---- SQL ----
    def write_sql(self, df: DataFrame):
        df.createOrReplaceTempView("v_full")
        if self.MODE == "overwrite_all":
            # INSERT OVERWRITE replaces all rows; table must exist (create on first run).
            if not spark.catalog.tableExists(self.TARGET_TABLE):
                df.write.format("delta").saveAsTable(self.TARGET_TABLE)
            else:
                spark.sql(f"INSERT OVERWRITE {self.TARGET_TABLE} SELECT * FROM v_full")
            logger.info(f"[sql] INSERT OVERWRITE → {self.TARGET_TABLE}")
        elif self.MODE == "dynamic_partition":
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            # With dynamic mode, INSERT OVERWRITE only replaces partitions present in v_full.
            spark.sql(f"INSERT OVERWRITE {self.TARGET_TABLE} SELECT * FROM v_full")
            logger.info(f"[sql] dynamic INSERT OVERWRITE → {self.TARGET_TABLE}")

    def run(self, use_sql: bool = False):
        df = self.read_source()
        if len(df.head(1)) == 0:
            logger.warning("Source is empty — skipping full load to avoid wiping target.")
            return
        (self.write_sql if use_sql else self.write_pyspark)(df)
        logger.info("Full load complete.")


if __name__ == "__main__":
    job = FullLoadDatabricks()
    job.SOURCE_TABLE = "main.bronze.dim_product"   # CHANGE_ME
    job.TARGET_TABLE = "main.silver.dim_product"   # CHANGE_ME
    job.MODE = "overwrite_all"
    job.run()

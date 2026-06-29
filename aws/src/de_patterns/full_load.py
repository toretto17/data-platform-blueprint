"""
================================================================================
FULL LOAD — full snapshot overwrite  [AWS Glue + Delta on S3]
================================================================================
Purpose : Replace the entire target (or only present partitions) with the source
          snapshot. AWS twin of databricks/src/de_patterns/full_load.py.

Variants: overwrite_all | dynamic_partition (keeps untouched partitions).
BOTH styles: PySpark write + SQL (INSERT OVERWRITE).

Glue params: --datalake-formats delta + Delta spark confs.
Customize: SOURCE_TABLE, TARGET_TABLE, TARGET_PATH, MODE, PARTITION_COL.
Version : 2026-06-28
================================================================================
"""
import sys
import logging
from typing import Optional

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("full_load_aws")


class FullLoadAWS:
    SOURCE_TABLE: str = "bronze_db.dim_product"    # CHANGE_ME
    TARGET_TABLE: str = "silver_db.dim_product"    # CHANGE_ME
    TARGET_PATH: str = "s3://CHANGE_ME/silver/dim_product/"
    MODE: str = "overwrite_all"                    # overwrite_all | dynamic_partition
    PARTITION_COL: Optional[str] = None

    def __init__(self):
        self.args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        sc = SparkContext.getOrCreate()
        self.gc = GlueContext(sc)
        self.spark = self.gc.spark_session
        self.spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        self.spark.conf.set("spark.sql.catalog.spark_catalog",
                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        self.job = Job(self.gc)
        self.job.init(self.args["JOB_NAME"], self.args)

    def read_source(self) -> DataFrame:
        return self.spark.table(self.SOURCE_TABLE)

    def _ensure_table_registered(self):
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.TARGET_TABLE} "
                       f"USING DELTA LOCATION '{self.TARGET_PATH}'")

    def write_pyspark(self, df: DataFrame):
        if self.MODE == "overwrite_all":
            (df.write.format("delta").mode("overwrite")
               .option("overwriteSchema", "true").save(self.TARGET_PATH))
            logger.info(f"[pyspark] full overwrite → {self.TARGET_PATH}")
        elif self.MODE == "dynamic_partition":
            if not self.PARTITION_COL:
                raise ValueError("dynamic_partition requires PARTITION_COL")
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            (df.write.format("delta").mode("overwrite")
               .partitionBy(self.PARTITION_COL).save(self.TARGET_PATH))
            logger.info(f"[pyspark] dynamic partition overwrite → {self.TARGET_PATH}")
        else:
            raise ValueError("MODE must be overwrite_all|dynamic_partition")
        self._ensure_table_registered()

    def write_sql(self, df: DataFrame):
        df.createOrReplaceTempView("v_full")
        if self.MODE == "dynamic_partition":
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        self._ensure_table_registered()
        self.spark.sql(f"INSERT OVERWRITE TABLE {self.TARGET_TABLE} SELECT * FROM v_full")
        logger.info(f"[sql] INSERT OVERWRITE → {self.TARGET_TABLE} (mode={self.MODE})")

    def run(self, use_sql: bool = False):
        df = self.read_source()
        if len(df.head(1)) == 0:
            logger.warning("Source empty — skipping full load to avoid wiping target.")
            self.job.commit()
            return
        (self.write_sql if use_sql else self.write_pyspark)(df)
        self.job.commit()
        logger.info("Full load complete.")


if __name__ == "__main__":
    job = FullLoadAWS()
    job.SOURCE_TABLE = "bronze_db.dim_product"     # CHANGE_ME
    job.TARGET_TABLE = "silver_db.dim_product"
    job.TARGET_PATH = "s3://CHANGE_ME/silver/dim_product/"
    job.MODE = "overwrite_all"
    job.run()

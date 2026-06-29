"""
================================================================================
BATCH INGESTION — external source → Bronze  [Databricks]
================================================================================
Purpose: Twin of aws/src/ingestion/batch/batch_ingest.py. Reads a batch source
         (cloud files OR JDBC) and lands it as Bronze Delta. For incremental file
         ingestion prefer Autoloader (see streaming/stream_ingest.py).

Sources:
    • files — spark.read on a path (parquet/csv/json)
    • jdbc  — relational DB (credentials from Databricks secret scope)

Customize: source_type, source_path/jdbc_*, target_table.
Version : 2026-06-28
================================================================================
"""
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("batch_ingest_databricks")
spark = SparkSession.builder.getOrCreate()


class BatchIngestDatabricks:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.source_type = cfg["source_type"].lower()      # files | jdbc
        self.target_table = cfg["target_table"]            # CHANGE_ME main.bronze.x

    def _secret(self, scope: str, key: str) -> str:
        # In a notebook/job: dbutils.secrets.get(scope, key)
        try:
            return dbutils.secrets.get(scope, key)  # noqa: F821 (dbutils injected)
        except Exception:
            raise RuntimeError(f"could not read secret {scope}/{key}")

    def read(self) -> DataFrame:
        if self.source_type == "files":
            fmt = self.cfg.get("source_format", "parquet")
            path = self.cfg["source_path"]                 # CHANGE_ME
            logger.info(f"reading files {path} ({fmt})")
            r = spark.read.option("recursiveFileLookup", "true")
            if fmt == "csv":
                r = r.option("header", "true")
            return r.format(fmt).load(path)
        if self.source_type == "jdbc":
            user = self._secret(self.cfg["secret_scope"], self.cfg["secret_user_key"])
            pwd = self._secret(self.cfg["secret_scope"], self.cfg["secret_pwd_key"])
            logger.info(f"reading jdbc {self.cfg['jdbc_url']} table={self.cfg['jdbc_table']}")
            return (spark.read.format("jdbc")
                    .option("url", self.cfg["jdbc_url"])           # CHANGE_ME
                    .option("dbtable", self.cfg["jdbc_table"])      # CHANGE_ME
                    .option("user", user).option("password", pwd)
                    .load())
        raise SystemExit(f"Unsupported source_type {self.source_type}")

    def write_bronze(self, df: DataFrame):
        df = (df.withColumn("_ingest_ts", F.current_timestamp())
                .withColumn("_ingest_date", F.date_format(F.current_date(), "yyyyMMdd")))
        (df.write.format("delta").mode("append").option("mergeSchema", "true")
           .partitionBy("_ingest_date").saveAsTable(self.target_table))
        logger.info(f"appended raw → {self.target_table}")

    def run(self):
        df = self.read()
        if len(df.head(1)) == 0:
            logger.info("no source rows — exit"); return
        self.write_bronze(df)
        logger.info("batch ingest complete")


if __name__ == "__main__":
    cfg = {"source_type": "files", "source_path": "s3://CHANGE_ME/raw/sales/",
           "source_format": "parquet", "target_table": "main.bronze.sales"}
    BatchIngestDatabricks(cfg).run()

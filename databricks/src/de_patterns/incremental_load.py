"""
================================================================================
INCREMENTAL LOAD — watermark-based append/upsert  [Databricks / Delta]
================================================================================
Purpose : Load only NEW/CHANGED source rows since the last run, using a high-
          watermark column (e.g. updated_at / id / load_date). Two write modes:
            • append : pure inserts (event/fact data)
            • upsert : MERGE on keys (dimension-like data)

BOTH styles: PySpark filter + write/MERGE, and the SQL equivalents (temp view).

Watermark is persisted in a small Delta control table (see MetadataFreshnessManager
in common/utils/etl_utils.py). Here we show the explicit version for clarity.

Customize: SOURCE_TABLE, TARGET_TABLE, WATERMARK_COL, KEYS, WRITE_MODE.
AWS twin: aws/src/de_patterns/incremental_load.py
Version : 2026-06-28
================================================================================
"""
import logging
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("incremental_load_databricks")
spark = SparkSession.builder.getOrCreate()


class IncrementalLoadDatabricks:
    SOURCE_TABLE: str = "main.bronze.events"      # CHANGE_ME
    TARGET_TABLE: str = "main.silver.events"      # CHANGE_ME
    WATERMARK_COL: str = "updated_at"             # CHANGE_ME monotonically increasing col
    KEYS: List[str] = ["id"]                      # CHANGE_ME (for upsert mode)
    WRITE_MODE: str = "append"                    # "append" | "upsert"
    WATERMARK_TABLE: str = "main.ops.watermarks"  # control table

    def _ensure_wm_table(self):
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.WATERMARK_TABLE}
                      (table_name STRING, watermark STRING, updated_ts TIMESTAMP) USING DELTA""")

    def get_watermark(self) -> Optional[str]:
        self._ensure_wm_table()
        r = (spark.table(self.WATERMARK_TABLE).filter(F.col("table_name") == self.TARGET_TABLE)
             .orderBy(F.col("updated_ts").desc()).limit(1).collect())
        return r[0]["watermark"] if r else None

    def set_watermark(self, value: str):
        spark.sql(f"DELETE FROM {self.WATERMARK_TABLE} WHERE table_name = '{self.TARGET_TABLE}'")
        (spark.createDataFrame([(self.TARGET_TABLE, str(value))], ["table_name", "watermark"])
         .withColumn("updated_ts", F.current_timestamp())
         .write.mode("append").saveAsTable(self.WATERMARK_TABLE))
        logger.info(f"watermark set {self.TARGET_TABLE}={value}")

    # ---- read only new rows (PySpark + SQL) ----
    def read_increment_pyspark(self, wm: Optional[str]) -> DataFrame:
        df = spark.table(self.SOURCE_TABLE)
        if wm is not None:
            df = df.filter(F.col(self.WATERMARK_COL) > F.lit(wm))
        logger.info(f"[pyspark] reading rows where {self.WATERMARK_COL} > {wm}")
        return df

    def read_increment_sql(self, wm: Optional[str]) -> DataFrame:
        pred = f"WHERE {self.WATERMARK_COL} > '{wm}'" if wm is not None else ""
        spark.sql(f"CREATE OR REPLACE TEMP VIEW v_inc AS SELECT * FROM {self.SOURCE_TABLE} {pred}")
        logger.info(f"[sql] CREATE TEMP VIEW v_inc {pred}")
        return spark.table("v_inc")

    # ---- write ----
    def write(self, inc: DataFrame):
        if self.WRITE_MODE == "append":
            inc.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(self.TARGET_TABLE)
            logger.info(f"[append] → {self.TARGET_TABLE}")
        elif self.WRITE_MODE == "upsert":
            inc.createOrReplaceTempView("v_inc_up")
            on = " AND ".join([f"t.{k} = s.{k}" for k in self.KEYS])
            if not spark.catalog.tableExists(self.TARGET_TABLE):
                inc.write.format("delta").saveAsTable(self.TARGET_TABLE)
            else:
                spark.sql(f"""MERGE INTO {self.TARGET_TABLE} t USING v_inc_up s ON {on}
                              WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
            logger.info(f"[upsert] MERGE → {self.TARGET_TABLE}")
        else:
            raise ValueError(f"WRITE_MODE must be append|upsert, got {self.WRITE_MODE}")

    def run(self, use_sql: bool = False):
        wm = self.get_watermark()
        inc = self.read_increment_sql(wm) if use_sql else self.read_increment_pyspark(wm)
        if len(inc.head(1)) == 0:
            logger.info("No new rows — nothing to load.")
            return
        new_wm = inc.agg(F.max(self.WATERMARK_COL)).collect()[0][0]
        self.write(inc)
        self.set_watermark(str(new_wm))
        logger.info(f"Incremental load complete. New watermark={new_wm}")


if __name__ == "__main__":
    job = IncrementalLoadDatabricks()
    job.SOURCE_TABLE = "main.bronze.events"   # CHANGE_ME
    job.TARGET_TABLE = "main.silver.events"   # CHANGE_ME
    job.WATERMARK_COL = "updated_at"
    job.WRITE_MODE = "append"
    job.run()

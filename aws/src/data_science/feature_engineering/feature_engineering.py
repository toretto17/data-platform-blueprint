"""
================================================================================
FEATURE ENGINEERING TEMPLATE — [AWS Glue / EMR]
================================================================================
Purpose: Same reusable feature computation patterns as Databricks twin, but
         writes to a SageMaker Feature Group (via FeatureStoreManager) or
         a Glue Catalog table.

Helpers (identical logic, portable Spark):
    lag_features(), rolling_stats(), calendar_features(), encode_categorical()

End-to-end pipeline: read Silver → compute → write to FS / Gold.
Databricks twin: databricks/src/data_science/feature_engineering/feature_engineering.py
Version : 2026-06-29
================================================================================
"""
import logging
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger("feature_engineering_aws")


def lag_features(df: DataFrame, partition_cols: List[str], order_col: str,
                 value_cols: List[str], lags: List[int] = [1, 7, 30]) -> DataFrame:
    w = Window.partitionBy(*partition_cols).orderBy(order_col)
    for col in value_cols:
        for lag in lags:
            df = df.withColumn(f"{col}_lag_{lag}", F.lag(col, lag).over(w))
    return df


def rolling_stats(df: DataFrame, partition_cols: List[str], order_col: str,
                  value_cols: List[str], windows: List[int] = [7, 30, 90]) -> DataFrame:
    for col in value_cols:
        for win in windows:
            w = (Window.partitionBy(*partition_cols).orderBy(order_col)
                 .rowsBetween(-(win - 1), Window.currentRow))
            df = df.withColumn(f"{col}_avg_{win}d", F.avg(col).over(w))
            df = df.withColumn(f"{col}_std_{win}d", F.stddev(col).over(w))
            df = df.withColumn(f"{col}_cv_{win}d",
                               F.col(f"{col}_std_{win}d") / F.coalesce(F.col(f"{col}_avg_{win}d"), F.lit(1)))
    return df


def calendar_features(df: DataFrame, date_col: str) -> DataFrame:
    return (df
            .withColumn("day_of_week", F.dayofweek(date_col))
            .withColumn("weekend_flag", F.when(F.dayofweek(date_col).isin(1, 7), 1).otherwise(0))
            .withColumn("month_sin", F.sin(2 * 3.14159 * F.month(date_col) / 12))
            .withColumn("month_cos", F.cos(2 * 3.14159 * F.month(date_col) / 12))
            .withColumn("dow_sin", F.sin(2 * 3.14159 * F.dayofweek(date_col) / 7))
            .withColumn("dow_cos", F.cos(2 * 3.14159 * F.dayofweek(date_col) / 7)))


class FeatureEngineeringPipeline:
    SOURCE_TABLE: str = "silver_db.sales"                       # CHANGE_ME
    TARGET_PATH: str = "s3://CHANGE_ME/gold/features/"
    TARGET_TABLE: str = "gold_db.sales_features"
    PARTITION_COLS: List[str] = ["item_id"]
    ORDER_COL: str = "tm_key_day"
    VALUE_COLS: List[str] = ["daily_ga", "daily_inflow_m1"]
    DATE_COL: str = "tm_key_day"

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or SparkSession.builder.getOrCreate()

    def compute(self) -> DataFrame:
        df = self.spark.table(self.SOURCE_TABLE)
        df = lag_features(df, self.PARTITION_COLS, self.ORDER_COL, self.VALUE_COLS)
        df = rolling_stats(df, self.PARTITION_COLS, self.ORDER_COL, self.VALUE_COLS)
        if self.DATE_COL in df.columns:
            df = calendar_features(df, self.DATE_COL)
        logger.info(f"features computed: {len(df.columns)} columns")
        return df

    def write(self, df: DataFrame):
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        (df.write.mode("overwrite").partitionBy("mnth_id")
           .format("parquet").option("path", self.TARGET_PATH)
           .saveAsTable(self.TARGET_TABLE))
        logger.info(f"written → {self.TARGET_TABLE}")

    def run(self):
        self.write(self.compute())


if __name__ == "__main__":
    FeatureEngineeringPipeline().run()

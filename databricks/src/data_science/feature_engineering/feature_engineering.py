"""
================================================================================
FEATURE ENGINEERING TEMPLATE — [Databricks]
================================================================================
Purpose: Reusable feature computation patterns for ML. Reads raw/silver tables,
         computes features (lags, rolling stats, encodings, calendar), writes to
         a UC feature table.

Contents:
    - lag_features(): window-based lag columns
    - rolling_stats(): rolling mean/std/cv over configurable windows
    - calendar_features(): day-of-week, holiday flag, weekend, month cyclical encoding
    - encode_categorical(): one-hot or target encoding
    - compute_and_write(): end-to-end pipeline (read → compute → write to FS)

Best practices:
    - Always compute features in Spark (not pandas) for scalability
    - Use window functions (not groupBy + join) for lags/rolling stats
    - Never leak future data (window ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    - Write to a Feature Table with PK + optional timeseries_col for PIT joins
    - Idempotent: mode='merge' (upsert by PK)

Customize: SOURCE_TABLE, FEATURE_TABLE, PRIMARY_KEYS, feature computations.
AWS twin: aws/src/data_science/feature_engineering/feature_engineering.py
Version : 2026-06-29
================================================================================
"""
import logging
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger("feature_engineering_databricks")
spark = SparkSession.builder.getOrCreate()


# ============================================================================
# FEATURE COMPUTATION HELPERS (reusable building blocks)
# ============================================================================

def lag_features(df: DataFrame, partition_cols: List[str], order_col: str,
                 value_cols: List[str], lags: Optional[List[int]] = None) -> DataFrame:
    """Add lag columns (e.g. value_lag_1, value_lag_7, value_lag_30).
    Window: no future leakage (ordered by order_col, partitioned by partition_cols)."""
    lags = lags or [1, 7, 30]
    w = Window.partitionBy(*partition_cols).orderBy(order_col)
    for col in value_cols:
        for lag in lags:
            df = df.withColumn(f"{col}_lag_{lag}", F.lag(col, lag).over(w))
    return df


def rolling_stats(df: DataFrame, partition_cols: List[str], order_col: str,
                  value_cols: List[str], windows: Optional[List[int]] = None) -> DataFrame:
    """Add rolling mean/std/cv over each window size.
    Window: ROWS BETWEEN (window-1) PRECEDING AND CURRENT ROW (no future leakage)."""
    windows = windows or [7, 30, 90]
    for col in value_cols:
        for win in windows:
            w = (Window.partitionBy(*partition_cols).orderBy(order_col)
                 .rowsBetween(-(win - 1), Window.currentRow))
            df = df.withColumn(f"{col}_avg_{win}d", F.avg(col).over(w))
            df = df.withColumn(f"{col}_std_{win}d", F.stddev(col).over(w))
            # CV = std / mean (coefficient of variation)
            df = df.withColumn(f"{col}_cv_{win}d",
                               F.col(f"{col}_std_{win}d") / F.coalesce(F.col(f"{col}_avg_{win}d"), F.lit(1)))
    return df


def calendar_features(df: DataFrame, date_col: str) -> DataFrame:
    """Add calendar features from a date column."""
    df = (df
          .withColumn("day_of_week", F.dayofweek(date_col))
          .withColumn("weekend_flag", F.when(F.dayofweek(date_col).isin(1, 7), 1).otherwise(0))
          .withColumn("month", F.month(date_col))
          .withColumn("day_of_month", F.dayofmonth(date_col))
          # Cyclical encoding (sin/cos for month — helps ML models understand periodicity)
          .withColumn("month_sin", F.sin(2 * 3.14159 * F.month(date_col) / 12))
          .withColumn("month_cos", F.cos(2 * 3.14159 * F.month(date_col) / 12))
          .withColumn("dow_sin", F.sin(2 * 3.14159 * F.dayofweek(date_col) / 7))
          .withColumn("dow_cos", F.cos(2 * 3.14159 * F.dayofweek(date_col) / 7)))
    return df


def encode_categorical(df: DataFrame, cols: List[str], method: str = "index") -> DataFrame:
    """Encode categorical columns. method='index' (StringIndexer-style integer encoding).
    For one-hot, use Spark ML's OneHotEncoder pipeline (not shown here for brevity)."""
    from pyspark.ml.feature import StringIndexer
    for col in cols:
        indexer = StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        df = indexer.fit(df).transform(df)
    return df


# ============================================================================
# END-TO-END PIPELINE (read → compute → write to Feature Store)
# ============================================================================

class FeatureEngineeringPipeline:
    # ---- CHANGE_ME ----
    SOURCE_TABLE: str = "main.silver.sales"
    FEATURE_TABLE: str = "main.features.sales_features"
    PRIMARY_KEYS: List[str] = ["item_id", "tm_key_day"]      # composite PK (item + day)
    PARTITION_COLS: List[str] = ["item_id"]                    # window partition
    ORDER_COL: str = "tm_key_day"                             # window ordering
    VALUE_COLS: List[str] = ["daily_ga", "daily_inflow_m1"]   # target features
    DATE_COL: str = "tm_key_day"

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)

    def compute(self) -> DataFrame:
        """Read source, compute features, return final DataFrame."""
        df = spark.table(self.SOURCE_TABLE)

        # 1. Lag features
        df = lag_features(df, self.PARTITION_COLS, self.ORDER_COL, self.VALUE_COLS, lags=[1, 7, 30])

        # 2. Rolling stats
        df = rolling_stats(df, self.PARTITION_COLS, self.ORDER_COL, self.VALUE_COLS, windows=[7, 30, 90])

        # 3. Calendar features
        if self.DATE_COL in df.columns:
            df = calendar_features(df, self.DATE_COL)

        # 4. Drop nulls from first rows of each partition (lags produce NULLs at the start)
        # CHANGE_ME: decide whether to keep or drop (keeping is safer for FS — downstream handles NULLs)

        logger.info(f"features computed: {len(df.columns)} columns")
        return df

    def write(self, df: DataFrame):
        """Write to the UC feature table via FeatureEngineeringClient."""
        from databricks.feature_engineering import FeatureEngineeringClient
        fe = FeatureEngineeringClient()
        if not spark.catalog.tableExists(self.FEATURE_TABLE):
            fe.create_table(name=self.FEATURE_TABLE, primary_keys=self.PRIMARY_KEYS, df=df)
            logger.info(f"created + wrote: {self.FEATURE_TABLE}")
        else:
            fe.write_table(name=self.FEATURE_TABLE, df=df, mode="merge")
            logger.info(f"merged into: {self.FEATURE_TABLE}")

    def run(self):
        df = self.compute()
        self.write(df)
        logger.info("feature engineering pipeline complete")


if __name__ == "__main__":
    FeatureEngineeringPipeline({
        "source_table": "main.silver.sales",               # CHANGE_ME
        "feature_table": "main.features.sales_features",   # CHANGE_ME
        "primary_keys": ["item_id", "tm_key_day"],
        "value_cols": ["daily_ga", "daily_inflow_m1"],
    }).run()

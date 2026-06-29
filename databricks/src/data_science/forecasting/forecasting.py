"""
================================================================================
FORECASTING PROJECT TEMPLATE — [Databricks]
================================================================================
Purpose: Time-series forecasting with MLflow tracking + Feature Store integration.
         Supports Prophet, AutoGluon-TimeSeries, and LightGBM-based approaches.

Pattern:
    1. Read feature table (time-series: has timeseries_columns)
    2. Split train/test by date (no random shuffle — temporal split)
    3. Fit model(s) + evaluate on holdout
    4. Log best model to MLflow + register in UC
    5. Generate forecasts (batch) and write to Gold/consumption

Best practices for time-series:
    - NEVER random-split — always temporal (cutoff date)
    - Validate on multiple horizons (1d, 7d, 30d)
    - Track MAPE/SMAPE/RMSE (not just RMSE)
    - Include calendar features (holidays, day-of-week) — see feature engineering

Customize: TARGET_COL, ITEM_COL, DATE_COL, models, horizon, metric.
AWS twin: aws/src/data_science/forecasting/forecasting.py
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Optional

import mlflow
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

logger = logging.getLogger("forecasting_databricks")
spark = SparkSession.builder.getOrCreate()


class ForecastingProject:
    # ---- CHANGE_ME ----
    FEATURE_TABLE: str = "main.features.ts_features"     # time-series feature table
    TARGET_COL: str = "daily_ga"                         # what to predict
    ITEM_COL: str = "item_id"                            # group-by column (per-entity forecasts)
    DATE_COL: str = "ds"                                 # date column
    HORIZON: int = 30                                    # days ahead to forecast
    EXPERIMENT: str = "/Shared/experiments/forecasting"
    MODEL_NAME: str = "main.ml.forecast_model"           # UC model name

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        mlflow.set_experiment(self.EXPERIMENT)

    # ---- 1. load data ----
    def load_data(self) -> pd.DataFrame:
        """Read from feature table. CHANGE_ME: filter/window as needed."""
        return spark.table(self.FEATURE_TABLE).toPandas()

    # ---- 2. temporal train/test split ----
    def split(self, df: pd.DataFrame, test_days: Optional[int] = None):
        """Split by date (never random). Test = last `test_days` days (default=HORIZON)."""
        test_days = test_days or self.HORIZON
        df[self.DATE_COL] = pd.to_datetime(df[self.DATE_COL])
        cutoff = df[self.DATE_COL].max() - pd.Timedelta(days=test_days)
        train = df[df[self.DATE_COL] <= cutoff]
        test = df[df[self.DATE_COL] > cutoff]
        logger.info(f"split: train={len(train)} test={len(test)} cutoff={cutoff.date()}")
        return train, test

    # ---- 3. train models ----
    def train_prophet(self, train: pd.DataFrame):
        """Fit Prophet per item. Returns dict of {item: model}."""
        from prophet import Prophet
        models = {}
        for item, grp in train.groupby(self.ITEM_COL):
            m = Prophet(daily_seasonality=True, yearly_seasonality=True)
            m.fit(grp.rename(columns={self.DATE_COL: "ds", self.TARGET_COL: "y"})[["ds", "y"]])
            models[item] = m
        logger.info(f"prophet trained for {len(models)} items")
        return models

    def train_lightgbm(self, train: pd.DataFrame):
        """Tabular approach: use lags + calendar features as inputs."""
        import lightgbm as lgb
        # CHANGE_ME: add your lag/calendar features
        feature_cols = [c for c in train.columns if c not in [self.TARGET_COL, self.ITEM_COL, self.DATE_COL]]
        model = lgb.LGBMRegressor(n_estimators=200, learning_rate=0.05, random_state=42)
        model.fit(train[feature_cols], train[self.TARGET_COL])
        logger.info("lightgbm trained")
        return model, feature_cols

    # ---- 4. evaluate ----
    def evaluate(self, y_true, y_pred) -> dict:
        """Compute forecasting metrics (MAPE, SMAPE, RMSE)."""
        y_true, y_pred = np.array(y_true, dtype=float), np.array(y_pred, dtype=float)
        mask = y_true != 0
        mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)
        smape = float(np.mean(2 * np.abs(y_true - y_pred) / (np.abs(y_true) + np.abs(y_pred) + 1e-8)) * 100)
        rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
        return {"mape": round(mape, 2), "smape": round(smape, 2), "rmse": round(rmse, 2)}

    # ---- 5. run full experiment ----
    def run(self):
        df = self.load_data()
        train, test = self.split(df)

        with mlflow.start_run(run_name="forecast_experiment"):
            # Train + evaluate multiple approaches (CHANGE_ME: pick/add models)
            # A) Prophet
            try:
                prophet_models = self.train_prophet(train)
                # Score test (simplified — full version generates future df per item)
                mlflow.log_param("model_type", "prophet")
                mlflow.log_param("n_items", len(prophet_models))
            except Exception as e:
                logger.warning(f"prophet skipped: {e}")

            # B) LightGBM (tabular)
            lgb_model, feat_cols = self.train_lightgbm(train)
            preds = lgb_model.predict(test[feat_cols])
            metrics = self.evaluate(test[self.TARGET_COL].values, preds)
            for k, v in metrics.items():
                mlflow.log_metric(k, v)
            mlflow.log_param("model_type_best", "lightgbm")
            mlflow.sklearn.log_model(lgb_model, "model", registered_model_name=self.MODEL_NAME)
            logger.info(f"forecast experiment: {metrics}")


if __name__ == "__main__":
    ForecastingProject({
        "feature_table": "main.features.sales_ts_features",  # CHANGE_ME
        "target_col": "daily_ga",
        "item_col": "item_id",
        "date_col": "tm_key_day",
        "horizon": 30,
    }).run()

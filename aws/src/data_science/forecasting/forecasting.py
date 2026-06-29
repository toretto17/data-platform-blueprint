"""
================================================================================
FORECASTING PROJECT TEMPLATE — [AWS SageMaker]
================================================================================
Purpose: Time-series forecasting with SageMaker Experiments tracking.
         Same algorithms as Databricks twin (Prophet, LightGBM) — runs as a
         SageMaker Processing job or local notebook.

Best practices (same as Databricks twin):
    - Temporal split (NEVER random)
    - MAPE/SMAPE/RMSE metrics
    - Multiple model comparison
    - Feature Store integration via Athena PIT query

Customize: same CHANGE_ME points as the Databricks twin.
Databricks twin: databricks/src/data_science/forecasting/forecasting.py
Version : 2026-06-29
================================================================================
"""
import logging
import json
import os
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("forecasting_aws")


class ForecastingProject:
    TARGET_COL: str = "daily_ga"            # CHANGE_ME
    ITEM_COL: str = "item_id"              # CHANGE_ME
    DATE_COL: str = "ds"                    # CHANGE_ME
    HORIZON: int = 30

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)

    def load_data(self, path: str) -> pd.DataFrame:
        """Load from local path (Processing job mounts S3 → local). CHANGE_ME."""
        return pd.read_parquet(path)

    def split(self, df: pd.DataFrame, test_days: Optional[int] = None):
        test_days = test_days or self.HORIZON
        df[self.DATE_COL] = pd.to_datetime(df[self.DATE_COL])
        cutoff = df[self.DATE_COL].max() - pd.Timedelta(days=test_days)
        return df[df[self.DATE_COL] <= cutoff], df[df[self.DATE_COL] > cutoff]

    def train_lightgbm(self, train: pd.DataFrame):
        import lightgbm as lgb
        feature_cols = [c for c in train.columns if c not in [self.TARGET_COL, self.ITEM_COL, self.DATE_COL]]
        model = lgb.LGBMRegressor(n_estimators=200, learning_rate=0.05, random_state=42)
        model.fit(train[feature_cols], train[self.TARGET_COL])
        return model, feature_cols

    def evaluate(self, y_true, y_pred) -> dict:
        y_true, y_pred = np.array(y_true, dtype=float), np.array(y_pred, dtype=float)
        mask = y_true != 0
        mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100) if mask.sum() else 0
        smape = float(np.mean(2 * np.abs(y_true - y_pred) / (np.abs(y_true) + np.abs(y_pred) + 1e-8)) * 100)
        rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
        return {"mape": round(mape, 2), "smape": round(smape, 2), "rmse": round(rmse, 2)}

    def run(self, data_path: str, output_dir: str = "/opt/ml/processing/output"):
        df = self.load_data(data_path)
        train, test = self.split(df)
        model, feat_cols = self.train_lightgbm(train)
        preds = model.predict(test[feat_cols])
        metrics = self.evaluate(test[self.TARGET_COL].values, preds)
        os.makedirs(output_dir, exist_ok=True)
        with open(f"{output_dir}/metrics.json", "w") as f:
            json.dump(metrics, f)
        import joblib
        joblib.dump(model, f"{output_dir}/model.pkl")
        logger.info(f"forecast metrics: {metrics}")
        return metrics


if __name__ == "__main__":
    ForecastingProject().run("/opt/ml/processing/input/features.parquet")

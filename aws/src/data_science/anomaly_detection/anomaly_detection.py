"""
================================================================================
DATA SCIENCE — ANOMALY DETECTION TEMPLATE
================================================================================
Purpose: Template for time-series anomaly detection projects.
         Pattern: Statistical baselines → ML model → threshold-based scoring.

Patterns from production:
    - Train per-segment (product × metric combinations)
    - Isolation Forest / Prophet / Statistical Z-score
    - Batch inference via SageMaker Batch Transform
    - Output: scored data with anomaly flags + severity

Workflow:
    1. Feature engineering (rolling stats, seasonality decomposition)
    2. Train baseline models per segment
    3. Score new data against baselines
    4. Flag anomalies with severity (HIGH/MEDIUM/LOW)
    5. Write to Gold/Consumption layer
================================================================================
"""
from dataclasses import dataclass
from typing import List, Dict, Tuple
import logging

import pandas as pd
import numpy as np

logger = logging.getLogger("anomaly_detection")


@dataclass
class AnomalyConfig:
    """Configuration for anomaly detection."""
    model_type: str = "isolation_forest"     # isolation_forest | prophet | zscore | ensemble
    target_columns: List[str] = None         # Metrics to monitor
    segment_columns: List[str] = None        # Train per segment (e.g., product, region)
    lookback_days: int = 90                  # Training window
    sensitivity: float = 0.05                # Anomaly threshold (lower = more sensitive)
    min_training_samples: int = 30           # Minimum samples to train

    def __post_init__(self):
        self.target_columns = self.target_columns or ["CHANGE_ME_metric"]
        self.segment_columns = self.segment_columns or ["CHANGE_ME_segment"]


class BaseAnomalyDetector:
    """Base class for anomaly detection. Override per algorithm."""

    def __init__(self, config: AnomalyConfig):
        self.config = config

    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add rolling statistics, lag features, seasonality indicators."""
        for col in self.config.target_columns:
            # Rolling stats
            df[f"{col}_rolling_mean_7d"] = df.groupby(self.config.segment_columns)[col].transform(
                lambda x: x.rolling(7, min_periods=1).mean())
            df[f"{col}_rolling_std_7d"] = df.groupby(self.config.segment_columns)[col].transform(
                lambda x: x.rolling(7, min_periods=1).std())
            # Z-score
            df[f"{col}_zscore"] = (df[col] - df[f"{col}_rolling_mean_7d"]) / df[f"{col}_rolling_std_7d"].clip(lower=1e-6)
            # Lag features
            df[f"{col}_lag_1"] = df.groupby(self.config.segment_columns)[col].shift(1)
            df[f"{col}_lag_7"] = df.groupby(self.config.segment_columns)[col].shift(7)
        return df

    def train(self, df: pd.DataFrame) -> Dict:
        """Train model(s). Returns model artifacts dict. Override this."""
        raise NotImplementedError

    def predict(self, df: pd.DataFrame, models: Dict) -> pd.DataFrame:
        """Score data. Returns df with anomaly_score and anomaly_flag. Override this."""
        raise NotImplementedError

    def assign_severity(self, df: pd.DataFrame, score_col: str = "anomaly_score") -> pd.DataFrame:
        """Assign severity based on score thresholds."""
        df["anomaly_severity"] = "NORMAL"
        df.loc[df[score_col] > 0.7, "anomaly_severity"] = "LOW"
        df.loc[df[score_col] > 0.85, "anomaly_severity"] = "MEDIUM"
        df.loc[df[score_col] > 0.95, "anomaly_severity"] = "HIGH"
        df["anomaly_flag"] = (df[score_col] > (1 - self.config.sensitivity)).astype(int)
        return df


class IsolationForestDetector(BaseAnomalyDetector):
    """Isolation Forest implementation."""

    def train(self, df: pd.DataFrame) -> Dict:
        from sklearn.ensemble import IsolationForest

        models = {}
        for segment, group in df.groupby(self.config.segment_columns):
            if len(group) < self.config.min_training_samples:
                logger.warning(f"Segment {segment}: insufficient data ({len(group)} rows), skipping")
                continue
            feature_cols = [c for c in group.columns if "rolling" in c or "lag" in c or "zscore" in c]
            X = group[feature_cols].fillna(0)
            model = IsolationForest(contamination=self.config.sensitivity, random_state=42, n_jobs=-1)
            model.fit(X)
            models[segment] = {"model": model, "feature_cols": feature_cols}
        return models

    def predict(self, df: pd.DataFrame, models: Dict) -> pd.DataFrame:
        df["anomaly_score"] = 0.0
        for segment, group in df.groupby(self.config.segment_columns):
            if segment not in models:
                continue
            model_info = models[segment]
            X = group[model_info["feature_cols"]].fillna(0)
            scores = model_info["model"].decision_function(X)
            # Normalize to 0-1 (lower decision_function = more anomalous)
            normalized = 1 - (scores - scores.min()) / (scores.max() - scores.min() + 1e-10)
            df.loc[group.index, "anomaly_score"] = normalized
        return self.assign_severity(df)


if __name__ == "__main__":
    # Minimal runnable example (replace CHANGE_ME with your columns).
    cfg = AnomalyConfig(
        model_type="isolation_forest",
        target_columns=["CHANGE_ME_metric"],     # CHANGE_ME
        segment_columns=["CHANGE_ME_segment"],    # CHANGE_ME
    )
    detector = IsolationForestDetector(cfg)
    # df = pd.read_parquet("/opt/ml/processing/input/features.parquet")
    # df = detector.engineer_features(df)
    # models = detector.train(df)
    # scored = detector.predict(df, models)
    logger.info("anomaly detection template ready")

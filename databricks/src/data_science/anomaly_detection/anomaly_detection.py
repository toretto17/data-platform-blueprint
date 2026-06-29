"""
================================================================================
ANOMALY DETECTION PROJECT TEMPLATE — [Databricks]
================================================================================
Purpose: Unsupervised anomaly detection (IsolationForest / Z-score) with MLflow
         tracking and Feature Store integration.

Pattern:
    1. Load features from Feature Store
    2. Fit IsolationForest per segment (e.g. per volume-tier / product)
    3. Score: normalize anomaly scores to [0,1], classify (normal/low/high)
    4. Log model + normalization params to MLflow
    5. Register for batch scoring via fe.score_batch

From production: same IsolationForest per-tier approach as our anomaly detection,
with normalize_scores() and classify() helpers.

Customize: FEATURE_TABLE, SEGMENT_COL, FEATURES, contamination, n_estimators.
AWS twin: aws/src/data_science/anomaly_detection/anomaly_detection.py
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Optional, List, Dict

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

logger = logging.getLogger("anomaly_databricks")
spark = SparkSession.builder.getOrCreate()


class AnomalyDetectionProject:
    # ---- CHANGE_ME ----
    FEATURE_TABLE: str = "main.features.sales_features"
    SEGMENT_COL: str = "volume_tier"         # fit one model per segment (e.g. HIGH/MID/LOW)
    FEATURES: List[str] = ["daily_ga", "avg_ga_3m", "volatility_ga_3m"]  # input features
    CONTAMINATION: float = 0.05              # expected anomaly fraction
    N_ESTIMATORS: int = 200
    EXPERIMENT: str = "/Shared/experiments/anomaly_detection"
    MODEL_NAME: str = "main.ml.anomaly_model"

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        mlflow.set_experiment(self.EXPERIMENT)

    def load_data(self) -> pd.DataFrame:
        return spark.table(self.FEATURE_TABLE).toPandas()

    @staticmethod
    def normalize_scores(raw_scores: np.ndarray, params: Optional[Dict] = None):
        """Invert IF decision_function (higher=more anomalous) + min-max to [0,1]."""
        inv = -raw_scores
        if params is None:
            params = {"min": float(inv.min()), "max": float(inv.max())}
        mn, mx = params["min"], params["max"]
        if mx == mn:
            return np.zeros_like(inv), params
        return np.clip((inv - mn) / (mx - mn), 0, 1), params

    @staticmethod
    def classify(labels: np.ndarray, norm_scores: np.ndarray) -> np.ndarray:
        """IF label (-1=anomaly) + normalized score → NORMAL/LOW_ANOMALY/HIGH_ANOMALY."""
        flags = np.full(len(labels), "NORMAL", dtype=object)
        a = labels == -1
        if a.sum() > 0:
            threshold = norm_scores[a].mean()
            flags[a & (norm_scores >= threshold)] = "HIGH_ANOMALY"
            flags[a & (norm_scores < threshold)] = "LOW_ANOMALY"
        return flags

    def run(self):
        df = self.load_data()
        segments = df[self.SEGMENT_COL].unique() if self.SEGMENT_COL in df.columns else ["ALL"]
        all_results = []

        with mlflow.start_run(run_name="anomaly_detection"):
            mlflow.log_params({"contamination": self.CONTAMINATION, "n_estimators": self.N_ESTIMATORS,
                               "n_segments": len(segments), "features": str(self.FEATURES)})
            from sklearn.ensemble import IsolationForest
            import joblib, tempfile, os

            models = {}
            for seg in segments:
                seg_data = df[df[self.SEGMENT_COL] == seg] if self.SEGMENT_COL in df.columns else df
                X = seg_data[self.FEATURES].fillna(0)
                model = IsolationForest(n_estimators=self.N_ESTIMATORS,
                                         contamination=self.CONTAMINATION, random_state=42, n_jobs=-1)
                model.fit(X)
                raw = model.decision_function(X)
                norm, params = self.normalize_scores(raw)
                labels = model.predict(X)
                flags = self.classify(labels, norm)
                models[seg] = {"model": model, "norm_params": params}
                n_anomalies = (flags != "NORMAL").sum()
                mlflow.log_metric(f"anomalies_{seg}", int(n_anomalies))
                all_results.append(seg_data.assign(anomaly_score=norm, anomaly_flag=flags))
                logger.info(f"  {seg}: {len(X)} rows, {n_anomalies} anomalies")

            # Save all segment models as one artifact
            tmp = tempfile.mkdtemp()
            for seg, m in models.items():
                joblib.dump(m["model"], os.path.join(tmp, f"model_{seg}.pkl"))
            import json
            with open(os.path.join(tmp, "norm_params.json"), "w") as f:
                json.dump({seg: m["norm_params"] for seg, m in models.items()}, f)
            mlflow.log_artifacts(tmp, "anomaly_models")

            # Optionally register the model (for batch scoring via fe.score_batch,
            # wrap in a custom pyfunc that loads all segment models)
            # mlflow.pyfunc.log_model(..., registered_model_name=self.MODEL_NAME)

            total_anomalies = sum(1 for r in all_results for f in r["anomaly_flag"] if f != "NORMAL")
            mlflow.log_metric("total_anomalies", total_anomalies)
            logger.info(f"anomaly detection complete: {total_anomalies} anomalies across {len(segments)} segments")


if __name__ == "__main__":
    AnomalyDetectionProject({
        "feature_table": "main.features.sales_anomaly_features",  # CHANGE_ME
        "segment_col": "volume_tier",
        "features": ["daily_ga", "avg_ga_3m", "volatility_ga_3m"],
    }).run()

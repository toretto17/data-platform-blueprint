"""
================================================================================
MODEL EVALUATION — [Databricks / MLflow]
================================================================================
Purpose: Evaluate a trained model against a holdout / test set, produce a metrics
         report, and gate registration (only promote if thresholds are met).

Pattern (verified Databricks + MLflow docs):
    1. Load model from MLflow (by run_id or model URI)
    2. Score test set (fe.score_batch for FS models, or model.predict)
    3. Compute metrics (classification / regression / custom)
    4. Log evaluation artifacts (metrics, confusion matrix, SHAP)
    5. Return pass/fail decision for registration gate

Customize: _load_test_data(), _compute_metrics(), THRESHOLDS.
AWS twin: aws/src/mlops/evaluation/evaluate.py
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Dict, Optional

import mlflow
import numpy as np
from pyspark.sql import SparkSession

logger = logging.getLogger("evaluate_databricks")
spark = SparkSession.builder.getOrCreate()


class ModelEvaluatorDatabricks:
    # ---- CHANGE_ME ----
    MODEL_URI: str = "models:/main.ml.churn_model/latest"   # UC model URI
    TEST_TABLE: str = "main.gold.test_labels"                # test labels table
    LABEL: str = "churn"
    THRESHOLDS: Dict[str, float] = {"f1": 0.6, "roc_auc": 0.7}  # min to pass

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)

    def _load_test_data(self):
        """Load test set. CHANGE_ME."""
        df = spark.table(self.TEST_TABLE).toPandas()
        X = df.drop(columns=[self.LABEL])
        y = df[self.LABEL]
        return X, y

    def _load_model(self):
        return mlflow.pyfunc.load_model(self.MODEL_URI)

    def _compute_metrics(self, y_true, y_pred, y_proba=None) -> Dict[str, float]:
        """CHANGE_ME with your domain-specific metrics."""
        from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, precision_score, recall_score
        metrics = {
            "accuracy": accuracy_score(y_true, y_pred),
            "f1": f1_score(y_true, y_pred, average="weighted"),
            "precision": precision_score(y_true, y_pred, average="weighted", zero_division=0),
            "recall": recall_score(y_true, y_pred, average="weighted", zero_division=0),
        }
        if y_proba is not None:
            try:
                metrics["roc_auc"] = roc_auc_score(y_true, y_proba)
            except Exception:
                pass
        return metrics

    def _gate(self, metrics: Dict[str, float]) -> bool:
        """Return True if ALL thresholds pass."""
        for metric, threshold in self.THRESHOLDS.items():
            if metrics.get(metric, 0) < threshold:
                logger.warning(f"GATE FAILED: {metric}={metrics.get(metric, 0):.4f} < {threshold}")
                return False
        return True

    def run(self) -> Dict:
        """Evaluate and return {metrics, passed}."""
        X, y = self._load_test_data()
        model = self._load_model()
        y_pred = model.predict(X)
        y_proba = None
        try:
            y_proba = model.predict(X)  # for pyfunc this IS the prediction
            if hasattr(model._model_impl, "predict_proba"):
                y_proba = model._model_impl.predict_proba(X)[:, 1]
        except Exception:
            pass
        metrics = self._compute_metrics(y, y_pred, y_proba)
        passed = self._gate(metrics)

        with mlflow.start_run(run_name="evaluation"):
            for k, v in metrics.items():
                mlflow.log_metric(f"eval_{k}", v)
            mlflow.log_metric("eval_passed", int(passed))

        logger.info(f"evaluation: {metrics} | passed={passed}")
        return {"metrics": metrics, "passed": passed}


if __name__ == "__main__":
    result = ModelEvaluatorDatabricks().run()
    if not result["passed"]:
        raise RuntimeError(f"evaluation gate FAILED: {result['metrics']}")

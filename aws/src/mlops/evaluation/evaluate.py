"""
================================================================================
MODEL EVALUATION — [AWS SageMaker]
================================================================================
Purpose: Evaluate a trained model, produce a metrics report (evaluation.json),
         and gate registration (only register if metrics pass thresholds).

Pattern (from production):
    1. Load model artifact (model.tar.gz from training output)
    2. Score test/holdout set
    3. Compute metrics → evaluation.json
    4. Upload to S3 (consumed by register step + SageMaker Model Card)
    5. Return pass/fail for pipeline condition gate

This runs as a SageMaker Processing job OR a Glue pythonshell job.

Customize: _load_test_data(), _compute_metrics(), THRESHOLDS.
Databricks twin: databricks/src/mlops/evaluation/evaluate.py
Version : 2026-06-29
================================================================================
"""
import json
import logging
import os
from typing import Dict, Optional

import numpy as np

logger = logging.getLogger("evaluate_aws")


class ModelEvaluatorAWS:
    THRESHOLDS: Dict[str, float] = {"f1": 0.6, "roc_auc": 0.7}

    def __init__(self, model_dir: str, test_data_path: str, output_dir: str):
        """
        model_dir:     local path to extracted model (e.g. /opt/ml/processing/model/)
        test_data_path: local path to test CSV/parquet
        output_dir:    where to write evaluation.json (e.g. /opt/ml/processing/evaluation/)
        """
        self.model_dir = model_dir
        self.test_data_path = test_data_path
        self.output_dir = output_dir

    def _load_model(self):
        """Load trained model. CHANGE_ME per your model framework."""
        import joblib
        return joblib.load(os.path.join(self.model_dir, "model.pkl"))

    def _load_test_data(self):
        """Load test set. CHANGE_ME."""
        import pandas as pd
        df = pd.read_csv(self.test_data_path)  # or .parquet
        label_col = "label"                     # CHANGE_ME
        X = df.drop(columns=[label_col])
        y = df[label_col]
        return X, y

    def _compute_metrics(self, y_true, y_pred, y_proba=None) -> Dict[str, float]:
        from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
        metrics = {
            "accuracy": float(accuracy_score(y_true, y_pred)),
            "f1": float(f1_score(y_true, y_pred, average="weighted")),
        }
        if y_proba is not None:
            try:
                metrics["roc_auc"] = float(roc_auc_score(y_true, y_proba))
            except Exception:
                pass
        return metrics

    def _gate(self, metrics: Dict[str, float]) -> bool:
        for metric, threshold in self.THRESHOLDS.items():
            if metrics.get(metric, 0) < threshold:
                logger.warning(f"GATE FAILED: {metric}={metrics.get(metric, 0):.4f} < {threshold}")
                return False
        return True

    def run(self) -> Dict:
        model = self._load_model()
        X, y = self._load_test_data()
        y_pred = model.predict(X)
        y_proba = model.predict_proba(X)[:, 1] if hasattr(model, "predict_proba") else None
        metrics = self._compute_metrics(y, y_pred, y_proba)
        passed = self._gate(metrics)
        # Write evaluation.json (consumed by SageMaker Model Card / pipeline ConditionStep)
        result = {"metrics": metrics, "passed": passed}
        os.makedirs(self.output_dir, exist_ok=True)
        with open(os.path.join(self.output_dir, "evaluation.json"), "w") as f:
            json.dump(result, f, indent=2)
        logger.info(f"evaluation: {metrics} | passed={passed}")
        return result


if __name__ == "__main__":
    # SageMaker Processing paths
    e = ModelEvaluatorAWS(
        model_dir="/opt/ml/processing/model",       # from ProcessingInput
        test_data_path="/opt/ml/processing/test/test.csv",
        output_dir="/opt/ml/processing/evaluation",
    )
    result = e.run()
    if not result["passed"]:
        # In a SageMaker Pipeline, the ConditionStep reads evaluation.json
        logger.error("evaluation gate FAILED")

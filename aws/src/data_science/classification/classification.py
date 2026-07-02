"""
================================================================================
CLASSIFICATION PROJECT TEMPLATE — [AWS SageMaker]
================================================================================
Purpose: Binary/multi-class classification with Optuna HPO + SageMaker Experiments.
         Runs as a Processing job or Training job.

Same pattern as Databricks twin:
    load → stratified split → Optuna HPO → evaluate → save model artifact

Customize: same CHANGE_ME points.
Databricks twin: databricks/src/data_science/classification/classification.py
Version : 2026-06-29
================================================================================
"""
import json
import logging
import os
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("classification_aws")


class ClassificationProject:
    LABEL: str = "churn"                    # CHANGE_ME
    N_TRIALS: int = 50
    CV_FOLDS: int = 5
    THRESHOLDS: Dict[str, float] = {"f1": 0.6}

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)

    def load_data(self, path: str):
        df = pd.read_parquet(path)
        X = df.drop(columns=[self.LABEL])
        y = df[self.LABEL]
        return X, y

    def split(self, X, y, test_size=0.2):
        from sklearn.model_selection import train_test_split
        return train_test_split(X, y, test_size=test_size, stratify=y, random_state=42)

    def tune(self, X_train, y_train) -> Dict:
        import optuna
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.model_selection import cross_val_score

        def objective(trial):
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 50, 300),
                "max_depth": trial.suggest_int("max_depth", 3, 10),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
            }
            clf = GradientBoostingClassifier(**params, random_state=42)
            return cross_val_score(clf, X_train, y_train, cv=self.CV_FOLDS, scoring="f1_weighted").mean()

        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=self.N_TRIALS)
        return study.best_params

    def evaluate(self, y_true, y_pred, y_proba=None) -> Dict[str, float]:
        from sklearn.metrics import f1_score, roc_auc_score, accuracy_score
        m = {"f1": float(f1_score(y_true, y_pred, average="weighted")),
             "accuracy": float(accuracy_score(y_true, y_pred))}
        if y_proba is not None:
            try: m["roc_auc"] = float(roc_auc_score(y_true, y_proba))
            except Exception: pass
        return m

    def run(self, data_path: str, output_dir: str = "/opt/ml/processing/output"):
        X, y = self.load_data(data_path)
        X_train, X_test, y_train, y_test = self.split(X, y)
        best_params = self.tune(X_train, y_train)
        from sklearn.ensemble import GradientBoostingClassifier
        model = GradientBoostingClassifier(**best_params, random_state=42)
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        proba = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else None
        metrics = self.evaluate(y_test, preds, proba)
        os.makedirs(output_dir, exist_ok=True)
        import joblib
        joblib.dump(model, f"{output_dir}/model.pkl")
        with open(f"{output_dir}/metrics.json", "w") as f:
            json.dump({**metrics, "params": best_params}, f)
        logger.info(f"classification: {metrics}")
        return metrics


if __name__ == "__main__":
    ClassificationProject().run("/opt/ml/processing/input/features.parquet")

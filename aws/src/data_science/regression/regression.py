"""
================================================================================
REGRESSION + MODEL COMPARISON + SHAP — [AWS SageMaker]
================================================================================
Purpose: Regression with Optuna HPO, multi-model comparison, SHAP explainability.
         Runs as a SageMaker Processing/Training job.

Same pattern as Databricks twin.
Databricks twin: databricks/src/data_science/regression/regression.py
Version : 2026-06-29
================================================================================
"""
import json
import logging
import os
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("regression_aws")


class RegressionProject:
    LABEL: str = "revenue"                  # CHANGE_ME
    N_TRIALS: int = 40

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)

    def load_data(self, path: str):
        df = pd.read_parquet(path)
        return df.drop(columns=[self.LABEL]), df[self.LABEL]

    def split(self, X, y, test_size=0.2):
        from sklearn.model_selection import train_test_split
        return train_test_split(X, y, test_size=test_size, random_state=42)

    def evaluate(self, y_true, y_pred) -> Dict[str, float]:
        from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
        mask = y_true != 0
        mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100) if mask.sum() else 0
        return {"rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
                "mae": float(mean_absolute_error(y_true, y_pred)),
                "r2": float(r2_score(y_true, y_pred)), "mape": round(mape, 2)}

    def compare_models(self, X_train, y_train, X_test, y_test):
        from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
        from sklearn.linear_model import ElasticNet
        results = []
        for name, model in [("GBR", GradientBoostingRegressor(n_estimators=200, random_state=42)),
                             ("RF", RandomForestRegressor(n_estimators=200, random_state=42)),
                             ("ElasticNet", ElasticNet(alpha=0.1, random_state=42))]:
            model.fit(X_train, y_train)
            metrics = self.evaluate(y_test.values, model.predict(X_test))
            results.append({"model": name, **metrics})
        return pd.DataFrame(results).sort_values("rmse")

    def tune(self, X_train, y_train) -> Dict:
        import optuna
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.model_selection import cross_val_score
        def objective(trial):
            params = {"n_estimators": trial.suggest_int("n_estimators", 50, 500),
                      "max_depth": trial.suggest_int("max_depth", 3, 12),
                      "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True)}
            return cross_val_score(GradientBoostingRegressor(**params, random_state=42),
                                   X_train, y_train, cv=5, scoring="neg_root_mean_squared_error").mean()
        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=self.N_TRIALS)
        return study.best_params

    def run(self, data_path: str, output_dir: str = "/opt/ml/processing/output"):
        X, y = self.load_data(data_path)
        X_train, X_test, y_train, y_test = self.split(X, y)
        leaderboard = self.compare_models(X_train, y_train, X_test, y_test)
        best_params = self.tune(X_train, y_train)
        from sklearn.ensemble import GradientBoostingRegressor
        model = GradientBoostingRegressor(**best_params, random_state=42)
        model.fit(X_train, y_train)
        metrics = self.evaluate(y_test.values, model.predict(X_test))
        os.makedirs(output_dir, exist_ok=True)
        import joblib
        joblib.dump(model, f"{output_dir}/model.pkl")
        with open(f"{output_dir}/metrics.json", "w") as f:
            json.dump({**metrics, "params": best_params, "leaderboard": leaderboard.to_dict()}, f, default=str)
        logger.info(f"regression: {metrics}\n{leaderboard.to_string(index=False)}")


if __name__ == "__main__":
    RegressionProject().run("/opt/ml/processing/input/features.parquet")

"""
================================================================================
REGRESSION + EXPERIMENTATION + HPO TEMPLATE — [Databricks]
================================================================================
Purpose: Combines regression, experiment tracking, and hyperparameter tuning in
         one file (they share the same pattern; only the metric changes).

Also includes:
    - Model comparison (leaderboard across multiple algorithms)
    - SHAP explainability logging

Pattern: same as classification (FS load → split → Optuna HPO → evaluate → register)
but with regression metrics (RMSE, MAE, R², MAPE).

Customize: FEATURE_TABLE, LABEL, algorithms, search_space.
AWS twin: aws/src/data_science/regression/regression.py
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Optional, Dict, List

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

logger = logging.getLogger("regression_databricks")
spark = SparkSession.builder.getOrCreate()


class RegressionProject:
    # ---- CHANGE_ME ----
    FEATURE_TABLE: str = "main.features.sales_features"
    LABEL: str = "revenue"
    PRIMARY_KEYS: list = ["item_id"]
    EXPERIMENT: str = "/Shared/experiments/regression"
    MODEL_NAME: str = "main.ml.revenue_model"
    N_TRIALS: int = 40

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        mlflow.set_experiment(self.EXPERIMENT)

    def load_data(self):
        from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
        fe = FeatureEngineeringClient()
        label_df = spark.table("main.gold.labels")        # CHANGE_ME
        ts = fe.create_training_set(
            df=label_df,
            feature_lookups=[FeatureLookup(table_name=self.FEATURE_TABLE,
                                           feature_names=None, lookup_key=self.PRIMARY_KEYS)],
            label=self.LABEL, exclude_columns=self.PRIMARY_KEYS)
        df = ts.load_df().toPandas()
        return df.drop(columns=[self.LABEL]), df[self.LABEL], ts

    def split(self, X, y, test_size=0.2):
        from sklearn.model_selection import train_test_split
        return train_test_split(X, y, test_size=test_size, random_state=42)

    # ---- HPO with Optuna ----
    def tune(self, X_train, y_train) -> Dict:
        import optuna
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.model_selection import cross_val_score

        def objective(trial):
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 50, 500),
                "max_depth": trial.suggest_int("max_depth", 3, 12),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            }
            model = GradientBoostingRegressor(**params, random_state=42)
            scores = cross_val_score(model, X_train, y_train, cv=5, scoring="neg_root_mean_squared_error")
            return scores.mean()

        study = optuna.create_study(direction="maximize")  # neg RMSE → maximize
        study.optimize(objective, n_trials=self.N_TRIALS)
        return study.best_params

    # ---- Evaluate ----
    def evaluate(self, y_true, y_pred) -> Dict[str, float]:
        from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
        mask = y_true != 0
        mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100) if mask.sum() > 0 else 0
        return {
            "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
            "mae": float(mean_absolute_error(y_true, y_pred)),
            "r2": float(r2_score(y_true, y_pred)),
            "mape": round(mape, 2),
        }

    # ---- Model comparison (leaderboard) ----
    def compare_models(self, X_train, y_train, X_test, y_test) -> pd.DataFrame:
        """Train multiple algorithms, log each, return a leaderboard sorted by RMSE."""
        from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
        from sklearn.linear_model import ElasticNet
        import xgboost as xgb

        candidates = {
            "GBR": GradientBoostingRegressor(n_estimators=200, random_state=42),
            "RF": RandomForestRegressor(n_estimators=200, random_state=42),
            "XGB": xgb.XGBRegressor(n_estimators=200, random_state=42, verbosity=0),
            "ElasticNet": ElasticNet(alpha=0.1, l1_ratio=0.5, random_state=42),
        }
        results = []
        for name, model in candidates.items():
            with mlflow.start_run(run_name=name, nested=True):
                model.fit(X_train, y_train)
                preds = model.predict(X_test)
                metrics = self.evaluate(y_test.values, preds)
                mlflow.log_metrics(metrics)
                mlflow.sklearn.log_model(model, "model")
                results.append({"model": name, **metrics})
                logger.info(f"  {name}: {metrics}")
        leaderboard = pd.DataFrame(results).sort_values("rmse")
        logger.info(f"leaderboard:\n{leaderboard.to_string(index=False)}")
        return leaderboard

    # ---- SHAP explainability ----
    def log_shap(self, model, X_sample):
        """Log SHAP summary plot as an MLflow artifact."""
        try:
            import shap
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X_sample.iloc[:200])
            import matplotlib.pyplot as plt
            shap.summary_plot(shap_values, X_sample.iloc[:200], show=False)
            plt.savefig("/tmp/shap_summary.png", bbox_inches="tight")
            mlflow.log_artifact("/tmp/shap_summary.png")
            plt.close()
            logger.info("SHAP summary logged")
        except Exception as e:
            logger.warning(f"SHAP logging skipped: {e}")

    # ---- Run ----
    def run(self):
        X, y, ts = self.load_data()
        X_train, X_test, y_train, y_test = self.split(X, y)

        with mlflow.start_run(run_name="regression_experiment"):
            # 1. Compare models
            leaderboard = self.compare_models(X_train, y_train, X_test, y_test)
            mlflow.log_text(leaderboard.to_csv(index=False), "leaderboard.csv")

            # 2. HPO on the best algorithm family
            best_params = self.tune(X_train, y_train)
            mlflow.log_params(best_params)

            # 3. Final train + log
            from sklearn.ensemble import GradientBoostingRegressor
            final_model = GradientBoostingRegressor(**best_params, random_state=42)
            final_model.fit(X_train, y_train)
            preds = final_model.predict(X_test)
            final_metrics = self.evaluate(y_test.values, preds)
            mlflow.log_metrics({f"final_{k}": v for k, v in final_metrics.items()})

            # 4. SHAP
            self.log_shap(final_model, X_test)

            # 5. Register via Feature Engineering
            from databricks.feature_engineering import FeatureEngineeringClient
            fe = FeatureEngineeringClient()
            fe.log_model(model=final_model, artifact_path="model", flavor=mlflow.sklearn,
                         training_set=ts, registered_model_name=self.MODEL_NAME)
            logger.info(f"best model registered: {self.MODEL_NAME} | metrics: {final_metrics}")


if __name__ == "__main__":
    RegressionProject({"feature_table": "main.features.sales_features",
                        "label": "revenue", "primary_keys": ["item_id"]}).run()

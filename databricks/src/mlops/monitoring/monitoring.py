"""
================================================================================
MODEL MONITORING + DRIFT DETECTION — [Databricks / Lakehouse Monitoring]
================================================================================
Purpose: Monitor a deployed model for data drift and prediction drift. Uses
         Databricks Lakehouse Monitoring (UC-native) where available, with a
         fallback manual PSI/KS implementation for any cluster.

Verified approaches (docs.databricks.com):
    A) Lakehouse Monitoring (recommended, DBR 15.3+):
       - spark.sql("CREATE MONITOR catalog.schema.predictions USING ...")
       - Auto-computes drift (PSI, KS, Chi-Sq) on a schedule
       - Results in _profile_metrics / _drift_metrics tables

    B) Manual drift (works on any cluster — portable):
       - Compute PSI (Population Stability Index) between baseline + current
       - KS test (Kolmogorov-Smirnov) per numeric feature
       - Chi-squared for categorical
       - Log to MLflow / Delta audit table

This template ships BOTH approaches (pick per your runtime/cost preference).
No Photon required for either.

Customize: PREDICTIONS_TABLE, BASELINE_TABLE, FEATURES_TO_MONITOR, THRESHOLDS.
AWS twin: aws/src/mlops/monitoring/monitoring.py (SageMaker Model Monitor + custom drift).
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Dict, List, Optional

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("monitoring_databricks")
spark = SparkSession.builder.getOrCreate()


# ===========================================================================
# A) Lakehouse Monitoring (DBR 15.3+ / UC tables)
# ===========================================================================
class LakehouseMonitor:
    """Create / manage a Databricks Lakehouse Monitor on a predictions table.
    This is a SQL DDL wrapper — no extra library needed, just the runtime."""

    PREDICTIONS_TABLE: str = "main.gold.predictions"           # CHANGE_ME
    SCHEDULE_CRON: str = "0 0 * * *"                           # daily at midnight

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)

    def create_monitor(self, timestamp_col: str = "prediction_ts",
                       granularities: List[str] = None):
        """Create a Lakehouse Monitor (idempotent — recreates if exists)."""
        gran = granularities or ["1 day"]
        gran_sql = ", ".join(f"'{g}'" for g in gran)
        spark.sql(f"""
            CREATE OR REPLACE MONITOR {self.PREDICTIONS_TABLE}
            TBLPROPERTIES (
                'schedule.quartz_cron_expression' = '{self.SCHEDULE_CRON}',
                'timestamp_col' = '{timestamp_col}',
                'granularities' = '{gran_sql}'
            )
        """)
        logger.info(f"Lakehouse Monitor created on {self.PREDICTIONS_TABLE}")

    def get_drift_metrics(self) -> DataFrame:
        """Read the auto-generated drift metrics table."""
        return spark.table(f"{self.PREDICTIONS_TABLE}_drift_metrics")

    def check_alerts(self, psi_threshold: float = 0.2) -> List[dict]:
        """Return features whose PSI exceeds the threshold (data drift alert)."""
        drift = self.get_drift_metrics()
        # Lakehouse Monitor stores PSI under different column names per version;
        # adapt to your runtime. Common: `psi` or `drift_statistic`.
        alerts = []
        for col_name in ("psi", "drift_statistic"):
            if col_name in drift.columns:
                bad = drift.filter(F.col(col_name) > psi_threshold).collect()
                for r in bad:
                    alerts.append({"feature": r.get("column_name", "?"),
                                   "psi": r[col_name], "threshold": psi_threshold})
        return alerts


# ===========================================================================
# B) Manual PSI / KS drift (works on any DBR — portable, no Lakehouse Monitor)
# ===========================================================================
class ManualDriftDetector:
    """Compute drift between a BASELINE and CURRENT dataset using PSI + KS.
    Logs results to MLflow and/or a Delta audit table."""

    FEATURES_TO_MONITOR: List[str] = ["feat1", "feat2"]        # CHANGE_ME
    PSI_THRESHOLD: float = 0.2
    KS_THRESHOLD: float = 0.05                                  # p-value threshold

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)

    @staticmethod
    def _psi(baseline: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
        """Population Stability Index (symmetric KL-like measure). >0.2 = significant drift."""
        eps = 1e-6
        breakpoints = np.histogram_bin_edges(baseline, bins=bins)
        b_pct = np.histogram(baseline, bins=breakpoints)[0] / len(baseline) + eps
        c_pct = np.histogram(current, bins=breakpoints)[0] / len(current) + eps
        return float(np.sum((c_pct - b_pct) * np.log(c_pct / b_pct)))

    @staticmethod
    def _ks_test(baseline: np.ndarray, current: np.ndarray) -> float:
        """Kolmogorov-Smirnov statistic (0-1, higher = more drift). p-value returned."""
        from scipy.stats import ks_2samp
        _, p = ks_2samp(baseline, current)
        return float(p)

    def detect(self, baseline_df: DataFrame, current_df: DataFrame) -> Dict[str, dict]:
        """Run PSI + KS per feature. Returns {feature: {psi, ks_pvalue, drifted}}."""
        baseline_pd = baseline_df.select(self.FEATURES_TO_MONITOR).toPandas()
        current_pd = current_df.select(self.FEATURES_TO_MONITOR).toPandas()
        results = {}
        for feat in self.FEATURES_TO_MONITOR:
            b = baseline_pd[feat].dropna().values
            c = current_pd[feat].dropna().values
            if len(b) == 0 or len(c) == 0:
                results[feat] = {"psi": 0, "ks_pvalue": 1, "drifted": False}
                continue
            psi = self._psi(b, c)
            ks_p = self._ks_test(b, c)
            drifted = psi > self.PSI_THRESHOLD or ks_p < self.KS_THRESHOLD
            results[feat] = {"psi": round(psi, 4), "ks_pvalue": round(ks_p, 4), "drifted": drifted}
            if drifted:
                logger.warning(f"DRIFT detected: {feat} PSI={psi:.4f} KS_p={ks_p:.4f}")
        logger.info(f"drift check complete: {sum(1 for v in results.values() if v['drifted'])} "
                    f"/ {len(results)} features drifted")
        return results

    def log_to_mlflow(self, results: Dict[str, dict]):
        """Log drift metrics to MLflow (visible in experiment)."""
        import mlflow
        with mlflow.start_run(run_name="drift_check"):
            for feat, vals in results.items():
                mlflow.log_metric(f"drift_psi_{feat}", vals["psi"])
                mlflow.log_metric(f"drift_ks_p_{feat}", vals["ks_pvalue"])
            mlflow.log_metric("features_drifted", sum(1 for v in results.values() if v["drifted"]))


if __name__ == "__main__":
    # Option A: Lakehouse Monitor (DBR 15.3+)
    # lm = LakehouseMonitor({"predictions_table": "main.gold.predictions"})
    # lm.create_monitor()

    # Option B: Manual drift (any cluster)
    detector = ManualDriftDetector({"features_to_monitor": ["total_purchases_30d", "avg_visits"]})
    baseline = spark.table("main.features.customer_features_baseline")  # CHANGE_ME
    current = spark.table("main.features.customer_features")            # CHANGE_ME
    results = detector.detect(baseline, current)
    detector.log_to_mlflow(results)
    # Alert if any feature drifted
    if any(v["drifted"] for v in results.values()):
        logger.error("DATA DRIFT DETECTED — consider retraining")

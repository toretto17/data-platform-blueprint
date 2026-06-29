# How to: Add a new ML model (train → register → deploy → monitor)

## Steps

### 1. Feature Engineering
- Copy `src/data_science/feature_engineering/feature_engineering.py`
- Customize the `compute()` method with your features
- Write to a feature table: `fe.create_table(name=..., primary_keys=[...], df=df)`

### 2. Training
- Copy `src/mlops/training/training_pipeline.py`
- Fill: `FEATURE_TABLE`, `LABEL`, `_train_model()`, eval thresholds
- Run: the template auto-logs to MLflow + registers in UC/SageMaker

### 3. Evaluation
- Copy `src/mlops/evaluation/evaluate.py`
- Set `THRESHOLDS` (metric gates). Model only gets registered/deployed if gates pass.

### 4. Register
- **Databricks**: `fe.log_model(...)` auto-registers. Use aliases: `Champion`, `Challenger`.
- **AWS**: `registry.register(model_data_url=..., image_uri=..., approval_status="PendingManualApproval")`

### 5. Deploy
- **Databricks**: `deployment.deploy("3")` or `deploy_canary("2", "3", challenger_pct=10)`
- **AWS**: `deployment.deploy_canary()` → monitor → `promote_canary()` or `rollback()`

### 6. Monitor
- **Databricks**: `LakehouseMonitor` (DDL) or `ManualDriftDetector` (PSI/KS)
- **AWS**: `build_monitoring_config()` → DDB → Lambda auto-provisions Model Monitor

### 7. Retrain
- Trigger the training pipeline again (scheduled or on drift alert)
- New version auto-registered → deploy as Challenger → compare → promote

## Cost-effective defaults
- Batch inference: fe.score_batch (Databricks) or Batch Transform with spot (AWS)
- Realtime: scale_to_zero (Databricks) or Serverless Inference (AWS)
- HPO: Optuna (single-node, cheap; RayTune for distributed if needed)

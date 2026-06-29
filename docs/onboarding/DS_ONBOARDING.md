# 🎓 Data Science Onboarding

## Day 1
1. Read `README.md` + `docs/architecture/END_TO_END_FLOW.md`
2. Understand the Feature Store: `src/feature_store/README.md`
3. See what features exist: query the UC feature table / describe FG

## Day 2-3
4. Pick your project type: `src/data_science/{classification,regression,forecasting,anomaly_detection}/`
5. Copy the template, fill CHANGE_ME
6. Run locally (notebook/IDE), verify metrics
7. Register via `fe.log_model` (Databricks) or `registry.register` (AWS)

## Key tools
- Optuna for HPO (not Hyperopt — deprecated on Databricks)
- MLflow for experiment tracking (auto-integrated on both platforms)
- SHAP for explainability (log as artifact)
- Feature Store for PIT-correct training data (never manual joins)

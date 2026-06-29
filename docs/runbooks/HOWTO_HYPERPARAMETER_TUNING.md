# 🧪 How to: Run Hyperparameter Tuning (Optuna + MLflow)

## Quick one-liner (using our helper)
```python
from databricks.src.data_science.experimentation.experimentation import quick_tune

def objective(trial):
    lr = trial.suggest_float("lr", 0.001, 0.3, log=True)
    depth = trial.suggest_int("max_depth", 3, 12)
    n_est = trial.suggest_int("n_estimators", 50, 500)
    # Train + evaluate your model here
    model = GradientBoostingClassifier(learning_rate=lr, max_depth=depth, n_estimators=n_est)
    score = cross_val_score(model, X_train, y_train, cv=5, scoring="f1_weighted").mean()
    return score

best_params = quick_tune(objective, n_trials=50, experiment_name="/Shared/my_experiment")
```
Every trial auto-logged to MLflow (metrics + params).

## Step-by-step (full control)
```python
import optuna, mlflow
from databricks.src.data_science.experimentation.experimentation import OptunaMLflowCallback

mlflow.set_experiment("/Shared/my_experiment")

with mlflow.start_run(run_name="hpo_session"):
    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=100, callbacks=[OptunaMLflowCallback()])
    mlflow.log_params(study.best_params)
    mlflow.log_metric("best_score", study.best_value)

print(f"Best: {study.best_params} → {study.best_value:.4f}")
```

## Compare results
```python
from databricks.src.data_science.experimentation.experimentation import ExperimentManager
em = ExperimentManager("/Shared/my_experiment")
top_runs = em.compare_runs(metric="f1", top_n=5)
```

## Why Optuna (not Hyperopt)
- Databricks deprecated Hyperopt SparkTrials (2025)
- Optuna is portable (works on AWS, Databricks, local)
- Better pruning (stops bad trials early)
- Cleaner API

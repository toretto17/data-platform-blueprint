# <Model Name>

## Problem
<What does this model predict/detect?>

## Features
- Source: `CHANGE_ME` (Feature Store table)
- Key features: ...

## Training
- Algorithm: CHANGE_ME
- HPO: Optuna (N trials)
- Metrics: F1/RMSE/MAPE

## Serving
- Batch: `fe.score_batch` / Batch Transform
- Realtime: Model Serving / SageMaker Endpoint

## Monitoring
- Data drift: PSI threshold = 0.2
- Retrain trigger: when drift detected

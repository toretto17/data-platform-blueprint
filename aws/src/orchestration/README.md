# 🔄 Orchestration — AWS — ☁️ AWS


| File | Purpose |
|---|---|
| `orchestration.py` | `StepFunctionManager` (create/update/trigger/poll SF), `EventBridgeScheduler` (cron→SF), `build_etl_sf_definition()` |

## Pattern
EventBridge (schedule) → Step Functions (DAG) → Glue/SageMaker jobs

## Platform twin
`databricks/src/orchestration/`

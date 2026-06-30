# 🏋️ training — 🧱 Databricks

Train ML models with Feature Store integration, evaluation gate, and auto-registration.

## Files

- `training_pipeline.py`

## Quick usage

```python
# Quick usage:
from databricks.src.mlops.training.training_pipeline import ModelTrainerDatabricks
trainer = ModelTrainerDatabricks(cfg)
trainer.run()  # → loads FS features → trains → evaluates → registers if passes gate
```

## Related runbook

[📖 Full guide: HOWTO_ADD_NEW_MODEL](../../docs/runbooks/HOWTO_ADD_NEW_MODEL.md)

---

> 🔄 **Platform twin:** `./aws/src/mlops/training/`

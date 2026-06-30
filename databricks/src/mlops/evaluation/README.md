# 📊 evaluation — 🧱 Databricks

Evaluate a model against holdout data. Threshold gate blocks bad models from production.

## Files

- `evaluate.py`

## Quick usage

```python
evaluator = ModelEvaluatorDatabricks(cfg)
result = evaluator.run()  # → {"metrics": {...}, "passed": True/False}
```

## Related runbook

[📖 Full guide: HOWTO_ADD_NEW_MODEL](../../../../docs/runbooks/HOWTO_ADD_NEW_MODEL.md)

---

> 🔄 **Platform twin:** `./aws/src/mlops/evaluation/`

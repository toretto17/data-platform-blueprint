# 🔗 pipelines — ☁️ AWS

End-to-end ML pipeline orchestration (train → eval → register → deploy in one DAG).

## Files

- `ml_pipeline.py`

## Quick usage

```python
# Creates/updates the pipeline definition, then triggers a run
pipeline = build_pipeline()
pipeline.start()
```

## Related runbook

[📖 Full guide: HOWTO_ADD_NEW_MODEL](../../../../docs/runbooks/HOWTO_ADD_NEW_MODEL.md)

---

> 🔄 **Platform twin:** `./databricks/src/mlops/pipelines/`

# 📋 registry — ☁️ AWS

Register model versions + promote (Champion/Challenger). Source of truth for production models.

## Files

- `registry.py`

## Quick usage

```python
reg = ModelRegistryAWS(cfg)
reg.register(run_id="abc123")
reg.promote_to_champion("3")
```

## Related runbook

[📖 Full guide: HOWTO_ADD_NEW_MODEL](../../../../docs/runbooks/HOWTO_ADD_NEW_MODEL.md)

---

> 🔄 **Platform twin:** `./databricks/src/mlops/registry/`

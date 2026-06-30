# 🎯 inference — 🧱 Databricks

Score data using a registered model. Batch (auto feature lookup) + realtime (endpoint).

## Files

- `inference.py`

## Quick usage

```python
# Batch:
scorer = BatchInferenceDatabricks(cfg)
predictions = scorer.score(batch_df)

# Realtime:
endpoint = RealtimeEndpointDatabricks(cfg)
endpoint.create_or_update()
```

## Related runbook

[📖 Batch guide](../../docs/runbooks/HOWTO_RUN_BATCH_INFERENCE.md) · [📖 Realtime guide](../../docs/runbooks/HOWTO_DEPLOY_REALTIME_ENDPOINT.md)

---

> 🔄 **Platform twin:** `./aws/src/mlops/inference/`

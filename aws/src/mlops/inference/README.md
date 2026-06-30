# 🎯 inference — ☁️ AWS

Score data using a registered model. Batch (auto feature lookup) + realtime (endpoint).

## Files

- `inference.py`

## Quick usage

```python
# Batch:
scorer = BatchInferenceAWS(cfg)
predictions = scorer.score(batch_df)

# Realtime:
endpoint = RealtimeEndpointAWS(cfg)
endpoint.create_or_update()
```

## Related runbook

[📖 Batch guide](../../docs/runbooks/HOWTO_RUN_BATCH_INFERENCE.md) · [📖 Realtime guide](../../docs/runbooks/HOWTO_DEPLOY_REALTIME_ENDPOINT.md)

---

> 🔄 **Platform twin:** `./databricks/src/mlops/inference/`

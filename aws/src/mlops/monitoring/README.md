# 👁️ monitoring — ☁️ AWS

Detect data drift + model degradation. PSI/KS metrics + managed monitoring.

## Files

- `monitoring.py`

## Quick usage

```python
detector = ManualDriftDetector(cfg)
results = detector.detect(baseline_df, current_df)
# results: {"feat1": {"psi": 0.05, "drifted": False}, ...}
```

## Related runbook

[📖 Full guide: HOWTO_SETUP_MODEL_MONITORING](../../../../docs/runbooks/HOWTO_SETUP_MODEL_MONITORING.md)

---

> 🔄 **Platform twin:** `./databricks/src/mlops/monitoring/`

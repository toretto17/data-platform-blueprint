# validation

Validate a UC feature table's health before serving it to training /  inference. Checks: freshness, null rate on key features, PK uniqueness,  row-count thresholds, distribution drift vs a baseline.

## Files

- `feature_store_validation.py`

## Platform twin

`./aws/src/feature_store/validation/`

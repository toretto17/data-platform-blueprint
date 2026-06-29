# How Feature Store connects to Training

```
Feature Table (UC / SageMaker FG)
    ↓
fe.create_training_set(df=labels, feature_lookups=[FeatureLookup(...)])
    ↓ (auto PIT join if timeseries_col set)
training_df = training_set.load_df()
    ↓
model = train(training_df)
    ↓
fe.log_model(model, training_set=training_set)  ← packages feature lineage
    ↓
Model in registry (knows which features it needs)
```

## Key points
- Training set does LEFT JOIN from labels → features on `lookup_key`
- PIT join activated by `timeseries_columns` on the table + `timestamp_lookup_key` on FeatureLookup
- `fe.log_model` packages the lookup metadata INTO the model artifact
- At inference, `fe.score_batch(model_uri, df)` auto-fetches features — no manual join needed

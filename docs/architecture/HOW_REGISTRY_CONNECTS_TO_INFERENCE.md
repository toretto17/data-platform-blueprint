# How Registry connects to Inference

## Databricks
```
# Batch (auto feature lookup):
predictions = fe.score_batch(model_uri="models:/catalog.schema.model@Champion", df=batch_df)

# Realtime:
w.serving_endpoints.create(name="my-endpoint", config=EndpointCoreConfigInput(
    served_entities=[ServedEntityInput(entity_name="catalog.schema.model", entity_version="3")]))
```

## AWS
```
# Resolve latest approved model:
arn = get_latest_approved_model(sm_client, "MyModelGroup")

# Batch Transform:
sm.create_transform_job(ModelName=..., TransformInput={S3DataSource: {S3Uri: input_path}}, ...)

# Realtime Endpoint:
sm.create_endpoint(EndpointName=..., EndpointConfigName=...)
```

## Key: registry is the single source of truth for "which model is in production"
- Databricks: the `@Champion` alias (or latest version)
- AWS: the latest Approved package in the group

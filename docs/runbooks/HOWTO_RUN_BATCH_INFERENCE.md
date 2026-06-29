# 🎯 How to: Run batch inference

## Databricks
```python
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
predictions = fe.score_batch(model_uri="models:/catalog.schema.model@Champion", df=batch_df)
predictions.write.format("delta").mode("append").saveAsTable("catalog.gold.predictions")
```

## AWS (SageMaker Batch Transform)
```python
from aws.src.mlops.inference.inference import BatchTransformAWS
bt = BatchTransformAWS({"model_package_group": "MyGroup", "input_s3": "s3://...", "output_s3": "s3://..."})
bt.submit()
```

Key: batch_df only needs PRIMARY KEY columns — features are auto-fetched from the Feature Store.

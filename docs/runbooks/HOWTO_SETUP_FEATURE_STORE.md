# 📦 How to: Set up a Feature Store table

## Databricks
1. `fe.create_table(name="catalog.schema.features", primary_keys=["id"], df=features_df)`
2. Schedule `feature_store_job.py` to run `fe.write_table(mode="merge")` daily
3. Training: `fe.create_training_set(labels_df, [FeatureLookup(...)])`
4. Inference: `fe.score_batch(model_uri, batch_df)` — auto-looks up features

## AWS
1. Create FG: `FeatureGroupManager(...).create_feature_group(feature_defs)`
2. Ingest: `FeatureStoreManager().ingest_data(df, feature_group_arn, target_stores=["OfflineStore"])`
3. Training: Athena PIT query (`ROW_NUMBER OVER PARTITION BY record_id ORDER BY event_time DESC`)
4. Inference: same Athena query or direct Spark read of the Iceberg table

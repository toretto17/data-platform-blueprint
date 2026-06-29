# How to: Set up model monitoring

## Databricks (Lakehouse Monitor)
```sql
CREATE OR REPLACE MONITOR catalog.schema.predictions
TBLPROPERTIES ('schedule.quartz_cron_expression' = '0 0 * * *', 'timestamp_col' = 'prediction_ts');
```
Auto-computes drift (PSI, KS, Chi-Sq). Results in `_drift_metrics` table.

## Databricks (Manual PSI/KS — works on any cluster)
```python
from databricks.src.mlops.monitoring.monitoring import ManualDriftDetector
detector = ManualDriftDetector({"features_to_monitor": ["feat1", "feat2"]})
results = detector.detect(baseline_df, current_df)
```

## AWS (SageMaker Model Monitor)
1. Write monitoring config to DynamoDB: `build_monitoring_config(group, bucket, emails)`
2. Approve the model package → Lambda auto-creates DataQuality monitor schedule
3. Violations appear in S3 + CloudWatch → SNS alert

## When to retrain
- PSI > 0.2 on any key feature → data distribution has shifted
- Model quality degraded (RMSE increased, F1 dropped) → signal from Model Monitor
- Scheduled: monthly/quarterly regardless (recalibration)

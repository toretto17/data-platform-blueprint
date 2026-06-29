# 🔄 AWS ↔ Databricks Service Mapping

| Concept | AWS Service | Databricks Equivalent |
|---|---|---|
| ETL engine | Glue (Spark) | Databricks Runtime (Spark) |
| Data Catalog | Glue Data Catalog | Unity Catalog |
| Table format | Parquet / Delta / Iceberg | Delta Lake (default) |
| Feature Store | SageMaker FeatureGroup | UC feature table (Delta + PK) |
| Model Registry | SageMaker Model Package Group | MLflow + UC (aliases) |
| Model Serving (batch) | SageMaker Batch Transform | fe.score_batch / Spark batch |
| Model Serving (realtime) | SageMaker Endpoint / Serverless | Databricks Model Serving |
| Model Monitoring | SageMaker Model Monitor | Lakehouse Monitor |
| HPO | Optuna (portable) | Optuna (recommended over Hyperopt) |
| Orchestration | Step Functions + EventBridge | Databricks Workflows |
| CI/CD | CodeBuild + CodePipeline | Databricks Asset Bundles |
| Secrets | Secrets Manager / SSM | Databricks secret scopes |
| CDC | DMS + Job Bookmarks / Delta CDF | Delta Change Data Feed |
| Streaming | Kinesis / Kafka + Glue Streaming | Autoloader / Structured Streaming |
| Data warehouse | Redshift (Spectrum) | Databricks SQL (DBSQL) |
| IAM | IAM roles + Lake Formation | Unity Catalog privileges |
| IaC | Terraform | Terraform + Asset Bundles |

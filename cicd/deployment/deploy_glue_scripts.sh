#!/usr/bin/env bash
# ============================================================
# Upload ALL ETL/ML scripts to the S3 artifactory bucket.
# Usage: ./deploy_glue_scripts.sh <env> <region>
# ============================================================
set -euo pipefail
ENV=${1:?usage: deploy_glue_scripts.sh <env> <region>}
REGION=${2:-us-east-1}  # CHANGE_ME: your default AWS region
BUCKET="s3-CHANGE_ME-${ENV}-artifactory-CHANGE_ME"   # CHANGE_ME

echo "🚀 Deploying scripts → s3://${BUCKET}/scripts/ (env=${ENV})"
echo ""

count=0
for dir in aws/src/ingestion/batch aws/src/ingestion/streaming \
           aws/src/bronze/jobs aws/src/silver/jobs aws/src/gold/marts \
           aws/src/consumption/jobs aws/src/consumption/warehouse aws/src/consumption/snowflake \
           aws/src/de_patterns \
           aws/src/feature_store/creation aws/src/feature_store/ingestion aws/src/feature_store/validation \
           aws/src/mlops/training aws/src/mlops/evaluation aws/src/mlops/inference \
           aws/src/mlops/registry aws/src/mlops/deployment aws/src/mlops/monitoring aws/src/mlops/pipelines \
           aws/src/data_science/forecasting aws/src/data_science/classification \
           aws/src/data_science/regression aws/src/data_science/anomaly_detection \
           aws/src/data_science/feature_engineering \
           aws/src/orchestration; do
  for f in $dir/*.py; do
    [ -f "$f" ] || continue
    fname=$(basename "$f")
    aws s3 cp "$f" "s3://${BUCKET}/scripts/${fname}" --region "$REGION" --quiet
    echo "  ✅ ${fname}"
    count=$((count+1))
  done
done

echo ""
echo "✅ Done — ${count} scripts uploaded to s3://${BUCKET}/scripts/"

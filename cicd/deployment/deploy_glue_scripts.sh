#!/usr/bin/env bash
# Upload all Glue scripts to the S3 artifactory bucket for the given ENV.
# Usage: ./deploy_glue_scripts.sh <env> <region>
set -euo pipefail
ENV=${1:?usage: deploy_glue_scripts.sh <env> <region>}
REGION=${2:-ap-southeast-1}
BUCKET="s3-CHANGE_ME-${ENV}-artifactory-CHANGE_ME"   # CHANGE_ME

echo "Deploying Glue scripts → s3://${BUCKET}/scripts/ (${ENV})"
for f in aws/src/*/jobs/*.py aws/src/*/ingestion/*.py aws/src/de_patterns/*.py aws/src/orchestration/*.py; do
  [ -f "$f" ] || continue
  fname=$(basename "$f")
  aws s3 cp "$f" "s3://${BUCKET}/scripts/${fname}" --region "$REGION"
  echo "  ✅ ${fname}"
done
echo "Done."

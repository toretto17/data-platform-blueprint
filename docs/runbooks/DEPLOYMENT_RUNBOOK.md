# Deployment Runbook

## Pre-Deployment Checklist

- [ ] All tests pass (`pytest tests/unit/ -v`)
- [ ] Code reviewed and approved
- [ ] DDB configs updated with correct env placeholders
- [ ] Terraform plan reviewed (no unexpected destroys)
- [ ] IAM permissions verified for target environment
- [ ] S3 scripts uploaded to artifactory bucket
- [ ] EventBridge schedules disabled during initial deploy

## Deployment Steps

### 1. Upload Glue Scripts
```bash
ENV=prod
BUCKET="s3-${PROJECT}-${FEATURE}-${ENV}-artifactory-${ACCOUNT_ID}"
aws s3 sync src/silver/jobs/ s3://$BUCKET/scripts/ --exclude "*.pyc"
aws s3 sync src/gold/marts/ s3://$BUCKET/scripts/ --exclude "*.pyc"
aws s3 sync src/consumption/ s3://$BUCKET/scripts/ --exclude "*.pyc"
aws s3 sync src/feature_store/ s3://$BUCKET/scripts/ --exclude "*.pyc"
```

### 2. Load DDB Configs
```bash
ENV=prod ./configs/scripts/load_ddb_config.sh
```

### 3. Terraform Apply
```bash
cd infrastructure/terraform/env/prod
terraform plan -var-file=etl-pipeline.tfvars -out=plan.out
# Review plan carefully!
terraform apply plan.out
```

### 4. Initial Data Load
```bash
# Set INITIAL_LOAD parameters in DDB
# LOOKBACK_DAYS=0, FORCE_RUN=true

# Trigger pipeline
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${ENV}-${PROJECT}-sales-master-pipeline" \
  --input '{"dl_date": "2026-01-01T00:00:00Z"}'

# Monitor
aws stepfunctions describe-execution --execution-arn <ARN>
```

### 5. Revert to Daily Parameters
```bash
# After initial load completes:
# LOOKBACK_DAYS=60, FORCE_RUN=false, INITIAL_LOAD=false
ENV=prod ./configs/scripts/load_ddb_config.sh
```

### 6. Enable Schedules
```bash
# Enable EventBridge rules via Terraform or console
```

## Rollback Procedure

1. Disable EventBridge schedules
2. Revert Terraform (`terraform apply` with previous tfvars)
3. Revert DDB configs (load previous version)
4. Verify data not corrupted (check Athena counts)

## Common Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| Glue job TIMEOUT | Data volume spike | Increase workers/timeout in locals.tf |
| DDB config not found | Lambda didn't process | Re-run `load_ddb_config.sh` |
| Step Function FAILED | Nested SF doesn't exist | Check SF names in DDB config |
| Redshift load fails | Wrong SF name in framework | Update framework SF definition |
| Feature Store permission denied | Missing IAM policy | Add SageMaker FS permissions to Glue role |

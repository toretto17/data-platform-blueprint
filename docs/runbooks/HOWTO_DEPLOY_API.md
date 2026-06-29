# 🌐 How to: Deploy the REST API (Step by Step)

Complete guide to get your API endpoint live — from zero to a working URL.

---

## Choose your deployment option

| Option | Cost | Complexity | Best for |
|---|---|---|---|
| **A. AWS Lambda + API Gateway** | 💰 Cheapest (pay per request) | Medium | Low traffic, serverless |
| **B. AWS ECS/Fargate** | 💰💰 Moderate | Medium | Sustained traffic, containers |
| **C. Local / EC2** | 💰 Cheap | Easy | Development / testing |
| **D. Databricks Apps** | 💰💰 Moderate | Easy | Already on Databricks |

---

## Option A: AWS Lambda + API Gateway (Recommended for most)

### Prerequisites
- AWS account with permissions (Lambda, API Gateway, IAM)
- AWS CLI configured (`aws configure`)
- Python 3.11+ locally

### Step 1: Install dependencies locally

```bash
mkdir api-deploy && cd api-deploy
cp /path/to/data-platform-blueprint/aws/src/consumption/apis/api_serving.py .
pip install fastapi mangum boto3 -t ./package/
```

### Step 2: Enable the Lambda handler

Open `api_serving.py` and **uncomment** the last 2 lines:
```python
from mangum import Mangum
handler = Mangum(app)
```

### Step 3: Package for Lambda

```bash
cd package
zip -r ../api-lambda.zip .
cd ..
zip api-lambda.zip api_serving.py
```

### Step 4: Create the Lambda function

```bash
# Create IAM role for Lambda (one-time)
aws iam create-role --role-name api-lambda-role \
  --assume-role-policy-document '{
    "Version":"2012-10-17",
    "Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},
    "Action":"sts:AssumeRole"}]}'

# Attach policies (Athena + S3 + CloudWatch)
aws iam attach-role-policy --role-name api-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess
aws iam attach-role-policy --role-name api-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
aws iam attach-role-policy --role-name api-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Wait 10 seconds for role to propagate
sleep 10

# Create the Lambda function
aws lambda create-function \
  --function-name data-platform-api \
  --runtime python3.11 \
  --handler api_serving.handler \
  --role arn:aws:iam::CHANGE_ME:role/api-lambda-role \
  --zip-file fileb://api-lambda.zip \
  --timeout 30 \
  --memory-size 256 \
  --environment Variables='{
    "API_KEY":"CHANGE_ME_your_secret_key",
    "ATHENA_DATABASE":"insights_consumption_layer",
    "ATHENA_OUTPUT":"s3://CHANGE_ME/athena-results/",
    "ALLOWED_TABLES":"sales_mart,forecast_inference,anomaly_inference",
    "REGION":"ap-southeast-1"
  }'
```

### Step 5: Create API Gateway (HTTP API — simple + cheap)

```bash
# Create the HTTP API
API_ID=$(aws apigatewayv2 create-api \
  --name data-platform-api \
  --protocol-type HTTP \
  --query ApiId --output text)

echo "API ID: $API_ID"

# Create Lambda integration
INTEGRATION_ID=$(aws apigatewayv2 create-integration \
  --api-id $API_ID \
  --integration-type AWS_PROXY \
  --integration-uri arn:aws:lambda:CHANGE_ME_REGION:CHANGE_ME_ACCOUNT:function:data-platform-api \
  --payload-format-version 2.0 \
  --query IntegrationId --output text)

# Create catch-all route (FastAPI handles routing internally)
aws apigatewayv2 create-route \
  --api-id $API_ID \
  --route-key '$default' \
  --target integrations/$INTEGRATION_ID

# Create deployment stage
aws apigatewayv2 create-stage \
  --api-id $API_ID \
  --stage-name prod \
  --auto-deploy

# Grant API Gateway permission to invoke Lambda
aws lambda add-permission \
  --function-name data-platform-api \
  --statement-id apigateway-invoke \
  --action lambda:InvokeFunction \
  --principal apigateway.amazonaws.com \
  --source-arn "arn:aws:execute-api:CHANGE_ME_REGION:CHANGE_ME_ACCOUNT:$API_ID/*"
```

### Step 6: Get your API URL

```bash
echo "Your API URL:"
echo "https://$API_ID.execute-api.CHANGE_ME_REGION.amazonaws.com/prod"
```

### Step 7: Test it!

```bash
API_URL="https://$API_ID.execute-api.CHANGE_ME_REGION.amazonaws.com/prod"

# Health check (no auth)
curl $API_URL/health

# List tables
curl -H "X-API-Key: CHANGE_ME_your_secret_key" $API_URL/v1/tables

# Query data
curl -H "X-API-Key: CHANGE_ME_your_secret_key" "$API_URL/v1/sales_mart?limit=10"

# Get schema
curl -H "X-API-Key: CHANGE_ME_your_secret_key" $API_URL/v1/sales_mart/schema
```

---

## Option B: AWS ECS/Fargate (for sustained traffic)

### Step 1: Create Dockerfile

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY api_serving.py .
RUN pip install fastapi uvicorn boto3
EXPOSE 8080
CMD ["uvicorn", "api_serving:app", "--host", "0.0.0.0", "--port", "8080"]
```

### Step 2: Build + push to ECR

```bash
# Create ECR repo
aws ecr create-repository --repository-name data-platform-api

# Login, build, push
aws ecr get-login-password | docker login --username AWS --password-stdin CHANGE_ME.dkr.ecr.CHANGE_ME.amazonaws.com
docker build -t data-platform-api .
docker tag data-platform-api:latest CHANGE_ME.dkr.ecr.CHANGE_ME.amazonaws.com/data-platform-api:latest
docker push CHANGE_ME.dkr.ecr.CHANGE_ME.amazonaws.com/data-platform-api:latest
```

### Step 3: Create ECS service

Use AWS Console → ECS → Create cluster (Fargate) → Create service → use the ECR image above.
Set environment variables same as Lambda (API_KEY, ATHENA_DATABASE, etc.).
Add an ALB (Application Load Balancer) in front → gives you a stable URL.

---

## Option C: Local / EC2 (dev/testing)

```bash
# Install deps
pip install fastapi uvicorn boto3

# Set env vars
export API_KEY="my-dev-key"
export ATHENA_DATABASE="insights_consumption_layer"
export ATHENA_OUTPUT="s3://CHANGE_ME/athena-results/"
export ALLOWED_TABLES="sales_mart,forecast_inference"
export REGION="ap-southeast-1"

# Run
cd aws/src/consumption/apis/
uvicorn api_serving:app --reload --port 8080

# Test
curl -H "X-API-Key: my-dev-key" http://localhost:8080/v1/tables
```

For EC2: same steps but run with `--host 0.0.0.0` and open port 8080 in security group.

---

## Option D: Databricks Apps

```bash
# Install deps
pip install fastapi uvicorn databricks-sql-connector

# Set env vars
export API_KEY="my-key"
export DATABRICKS_HOST="your-workspace.cloud.databricks.com"
export DBX_HTTP_PATH="/sql/1.0/warehouses/CHANGE_ME"
export DATABRICKS_TOKEN="dapi..."  # or use secret scope
export CONSUMPTION_SCHEMA="main.consumption"

# Run
cd databricks/src/consumption/apis/
uvicorn api_serving:app --host 0.0.0.0 --port 8080
```

For Databricks Apps: package as a Databricks App (see Databricks docs for "Custom Apps").

---

## After deployment: verify everything works

```bash
API_URL="https://your-url-here"
KEY="your-api-key"

echo "=== 1. Health check ==="
curl $API_URL/health

echo "=== 2. List tables ==="
curl -H "X-API-Key: $KEY" $API_URL/v1/tables

echo "=== 3. Get data (paginated) ==="
curl -H "X-API-Key: $KEY" "$API_URL/v1/sales_mart?limit=5"

echo "=== 4. Filter + sort ==="
curl -H "X-API-Key: $KEY" "$API_URL/v1/sales_mart?filter=mnth_id=202606&order_by=tm_key_day%20DESC&limit=10"

echo "=== 5. Schema ==="
curl -H "X-API-Key: $KEY" $API_URL/v1/sales_mart/schema

echo "=== 6. Stats ==="
curl -H "X-API-Key: $KEY" $API_URL/v1/sales_mart/stats

echo "=== 7. Custom SQL ==="
curl -X POST -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"sql":"SELECT product, COUNT(*) as cnt FROM sales_mart GROUP BY product","limit":10}' \
  $API_URL/v1/query
```

---

## Security checklist before going live

- [ ] Replace `API_KEY` env var with a strong random key (or Secrets Manager)
- [ ] Set `ALLOWED_TABLES` to only the tables that should be exposed
- [ ] For production: replace API key auth with Cognito/JWT/OAuth
- [ ] Enable HTTPS (API Gateway does this automatically; for ECS use ALB + ACM cert)
- [ ] Set rate limiting (API Gateway → throttling settings; or use WAF)
- [ ] Monitor: enable CloudWatch access logs on API Gateway
- [ ] Remove `/v1/query` endpoint if you don't want custom SQL access

---

## Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| `401 Unauthorized` | Wrong/missing API key | Check `X-API-Key` header matches `API_KEY` env var |
| `404 Table not found` | Table not in allow-list | Add to `ALLOWED_TABLES` env var |
| `500 Query FAILED` | Athena can't access data | Check IAM role has S3 + Athena + Glue permissions |
| `Timeout` | Query too slow | Increase Lambda timeout (max 900s) or add `LIMIT` |
| `Connection refused` (local) | App not running | Check `uvicorn` is running on the right port |

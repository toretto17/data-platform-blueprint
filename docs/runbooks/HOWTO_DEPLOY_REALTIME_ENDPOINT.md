# How to: Deploy a realtime model endpoint

## Databricks (Model Serving, scale-to-zero)
```python
from databricks.src.mlops.deployment.deployment import ModelDeploymentDatabricks
d = ModelDeploymentDatabricks({"endpoint_name": "my-endpoint", "model_name": "catalog.schema.model"})
d.deploy("3")  # deploy version 3 at 100%
# OR canary: d.deploy_canary("2", "3", challenger_pct=10)
```
- Cost: scale_to_zero_enabled=True (no idle cost)
- Rollback: `d.rollback("2")` (instant)

## AWS (SageMaker Serverless Inference)
```python
from aws.src.mlops.deployment.deployment import ModelDeploymentAWS
d = ModelDeploymentAWS({"endpoint_name": "my-ep", "model_package_group": "MyGroup"})
d.deploy_canary()  # 10% new, 90% old
# Monitor... then: d.promote_canary() OR d.rollback()
```
- Cost: Serverless Inference (pay per invocation, no idle)
- Rollback: shifts traffic weights back to Champion in <60s

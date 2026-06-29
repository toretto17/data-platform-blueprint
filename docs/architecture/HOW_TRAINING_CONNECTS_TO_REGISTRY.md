# How Training connects to Registry

## Databricks (UC + MLflow)
```
fe.log_model(..., registered_model_name="catalog.schema.model")
    → auto-registers in Unity Catalog
    → version number auto-incremented
    → lineage to feature tables tracked
```
Promotion: `client.set_registered_model_alias(name, "Champion", version)`

## AWS (SageMaker Model Registry)
```
sm.create_model_package(
    ModelPackageGroupName="MyModelGroup",
    InferenceSpecification={Containers: [{Image, ModelDataUrl}]},
    ModelApprovalStatus="PendingManualApproval",
    CustomerMetadataProperties={...monitoring contract...}
)
```
Promotion: `sm.update_model_package(ModelPackageArn=..., ModelApprovalStatus="Approved")`
→ triggers monitoring Lambda → sets up Model Monitor schedules

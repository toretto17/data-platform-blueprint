# 📁 templates/


Starter files for common tasks. These are POINTERS to the full implementations
in `aws/src/` and `databricks/src/`. Copy the referenced file to your project
and fill in the CHANGE_ME placeholders.

| File | Points to |
|---|---|
| `glue_job_template.py` | `aws/src/silver/jobs/silver_job.py` |
| `databricks_job_template.py` | `databricks/src/silver/jobs/silver_job.py` |
| `dq_template.py` | `*/src/common/validations/dq_framework.py` |
| `sagemaker_training_template.py` | `aws/src/mlops/training/training_pipeline.py` |
| `sagemaker_inference_template.py` | `aws/src/mlops/inference/inference.py` |
| `databricks_training_template.py` | `databricks/src/mlops/training/training_pipeline.py` |
| `databricks_inference_template.py` | `databricks/src/mlops/inference/inference.py` |
| `step_function_template.json` | `aws/src/orchestration/orchestration.py` |
| `eventbridge_template.json` | `aws/src/orchestration/orchestration.py` |
| `databricks_workflow_template.json` | `databricks/src/orchestration/orchestration.py` |
| `feature_store_template.py` | `*/src/feature_store/creation/feature_group.py` |
| `readme_templates/` | Skeletons for job + model documentation |

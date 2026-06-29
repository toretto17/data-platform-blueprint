# 📁 configs/


Environment-specific configuration. One file per environment.

| Path | Purpose |
|---|---|
| `templates/project.yaml.template` | Master template — copy to `<env>/project.yaml` |
| `templates/ddb_config.json.template` | DynamoDB job config template (AWS) |
| `dev/project.yaml` | Filled example for dev |
| `qa/project.yaml` | Filled example for QA |
| `uat/project.yaml` | Filled example for UAT |
| `prod/project.yaml` | Filled example for prod |
| `scripts/load_ddb_config.sh` | Deploy DDB configs (substitutes ${env}/${account_id}) |

## How to use

1. Copy `templates/project.yaml.template` → `configs/<env>/project.yaml`
2. Fill in account_id, region, bucket names, IAM roles
3. All other configs reference these values via `${variable}` substitution

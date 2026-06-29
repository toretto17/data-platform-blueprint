# tests/

| Path | Purpose |
|---|---|
| `conftest.py` | Shared fixtures (SparkSession, sample DataFrames) |
| `aws/unit/` | Unit tests for AWS-specific code |
| `databricks/unit/` | Unit tests for Databricks-specific code |
| `shared/` | Platform-neutral tests |

## Run
```bash
make test-unit            # all unit tests
pytest tests/aws -v       # AWS only
pytest tests/databricks -v # Databricks only
```

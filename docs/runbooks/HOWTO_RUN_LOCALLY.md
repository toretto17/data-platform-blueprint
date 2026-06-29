# 💻 How to: Run and Test Locally

## Step 1: Install dependencies
```bash
cd data-platform-blueprint
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
```

## Step 2: Install pre-commit hooks
```bash
pre-commit install
```

## Step 3: Run linting
```bash
make lint    # ruff check + black --check
make fmt     # auto-fix formatting
```

## Step 4: Run unit tests
```bash
make test-unit                 # all unit tests
pytest tests/aws/unit/ -v      # AWS only
pytest tests/databricks/ -v    # Databricks only
pytest tests/shared/ -v        # platform-neutral
```

## Step 5: Test a specific job locally (with local Spark)
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

# Example: test your silver transform logic
from aws.src.silver.jobs.silver_job import SilverSalesJob
# (Override _read_sources to return sample data instead of Glue Catalog)
```

## Step 6: Run the API locally
```bash
export API_KEY="test-key"
export ATHENA_DATABASE="my_db"
cd aws/src/consumption/apis/
uvicorn api_serving:app --reload --port 8080
# → http://localhost:8080/docs (Swagger UI!)
```

## Tips
- FastAPI gives you free Swagger docs at `/docs` (interactive API testing)
- Use `pytest -k "test_dq"` to run a single test
- `make clean` removes __pycache__ / .pytest_cache
- Set `AWS_PROFILE=dev` if testing with real AWS resources locally

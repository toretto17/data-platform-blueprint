# ============================================================
# Enterprise Data Platform Template — Makefile
# ============================================================
# Common developer tasks. Run `make help` to list targets.
# ============================================================

.PHONY: help install lint fmt test test-unit test-integration \
        load-ddb tf-plan tf-apply deploy-glue deploy-databricks clean

ENV ?= dev                      # override: make tf-plan ENV=prod
REGION ?= ap-southeast-1
PYTHON ?= python3

help:                           ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:                        ## Install dev dependencies + pre-commit hooks
	$(PYTHON) -m pip install -r requirements-dev.txt
	pre-commit install

fmt:                            ## Auto-format (black + isort + ruff --fix)
	black src tests
	isort src tests
	ruff check --fix src tests

lint:                           ## Lint (ruff + black --check) — no changes
	ruff check src tests
	black --check src tests

test: test-unit                 ## Run all tests (alias for unit by default)

test-unit:                      ## Run unit tests
	pytest tests/unit -v

test-integration:               ## Run integration tests
	pytest tests/integration -v

# ---- Config / deploy (AWS) ----
load-ddb:                       ## Load DDB job configs for ENV (e.g. make load-ddb ENV=dev)
	ENV=$(ENV) REGION=$(REGION) ./configs/scripts/load_ddb_config.sh

tf-plan:                        ## Terraform plan for ENV
	cd infrastructure/terraform/workload/etl-pipeline && \
	  terraform init -backend-config=../../env/$(ENV)/etl-pipeline.backend.hcl && \
	  terraform plan -var-file=../../env/$(ENV)/etl-pipeline.tfvars

tf-apply:                       ## Terraform apply for ENV
	cd infrastructure/terraform/workload/etl-pipeline && \
	  terraform apply -var-file=../../env/$(ENV)/etl-pipeline.tfvars

deploy-glue:                    ## Upload Glue scripts to S3 artifactory for ENV
	./cicd/deployment/deploy_glue_scripts.sh $(ENV) $(REGION)

# ---- Deploy (Databricks) ----
deploy-databricks:              ## Deploy Databricks Asset Bundle for ENV
	cd infrastructure/databricks && databricks bundle deploy -t $(ENV)

clean:                          ## Remove caches / temp
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -type d -name '.pytest_cache' -prune -exec rm -rf {} +
	rm -rf .ruff_cache

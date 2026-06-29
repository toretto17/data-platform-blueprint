# Changelog

All notable changes to this template are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `ROADMAP.md` — master tracking document (14 phases, ~209 items)
- `docs/BLUEPRINT_STATUS.md` — snapshot dashboard
- Phase 1 meta files: `LICENSE` (MIT), `CONTRIBUTING.md`, `CHANGELOG.md`,
  `Makefile`, `pyproject.toml`, `.pre-commit-config.yaml`, `.editorconfig`

### Planned (see ROADMAP.md)
- Dual-platform (AWS + Databricks) templates for every capability
- DE: ingestion, bronze, load patterns (incremental/full/CDC/SCD1/SCD2), audit
- Feature Store + full MLOps lifecycle (train→eval→register→deploy→monitor)
- Data Science: forecasting, classification, regression, experiment tracking, HPO
- `templates/` starter files
- How-to runbooks + architecture/connection docs

## [0.1.0] - 2026-06-25

### Added
- Initial blueprint scaffold (folder structure + first AWS Glue templates)
- AWS Silver / Gold / Consumption / Feature Store job templates
- `src/common/utils/etl_utils.py`, `production_patterns.py`
- `src/common/validations/dq_framework.py`
- Glue + Databricks job optimizers
- Terraform glue + sfn modules, Lambda config-loader, 3 Step Functions examples
- Docs: PATTERNS, REDSHIFT_SPECTRUM_PATTERN, GETTING_STARTED, DEPLOYMENT_RUNBOOK

# MLOps Onboarding

## Day 1
1. Read `README.md` + the MLOps section
2. Read `docs/runbooks/HOWTO_ADD_NEW_MODEL.md` (the full lifecycle)
3. Understand the flow: `docs/architecture/HOW_*_CONNECTS_TO_*.md`

## Day 2-3
4. Walk through `src/mlops/` — one file per lifecycle stage
5. Set up your first pipeline: `src/mlops/pipelines/ml_pipeline.py`
6. Deploy a test model: `src/mlops/deployment/deployment.py`
7. Set up monitoring: `src/mlops/monitoring/monitoring.py`

## Key decisions
- Registry = the source of truth for "what's in production"
- Canary deploys (never 100% traffic on day 1)
- Drift detection triggers retrain (PSI > 0.2)
- All models packaged with Feature Store lineage (enables auto-lookup at inference)

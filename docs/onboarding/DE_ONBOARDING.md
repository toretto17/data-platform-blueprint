# Data Engineering Onboarding

Welcome! Here's how to get productive as a DE on this platform.

## Day 1
1. Clone the repo, run `make install` (sets up pre-commit + deps)
2. Read `README.md` (architecture overview)
3. Read `docs/architecture/END_TO_END_FLOW.md` (the big picture)
4. Pick your platform folder (`aws/` or `databricks/`)

## Day 2-3
5. Read `src/silver/jobs/silver_job.py` (understand the Base*Job pattern)
6. Read `src/de_patterns/README.md` (when to use which load pattern)
7. Follow `docs/runbooks/HOWTO_ADD_NEW_ETL_PIPELINE.md` to add your first table

## Conventions
- All job code follows Base*Job → override `_define_sources`, `_apply_transformations`
- Every table has a DQ config (even if just row_count + pk_not_null)
- `CHANGE_ME` = fill this; `# TODO:` = implement this
- `make lint test` before every PR

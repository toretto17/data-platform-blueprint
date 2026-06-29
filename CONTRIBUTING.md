# Contributing Guide

Thanks for contributing to the Data Platform Blueprint. This guide explains how to add new templates, the conventions to follow, and the PR workflow.

---

## Golden rule — every capability ships for BOTH platforms

For each new capability, create **two implementation files plus docs**:

```
<capability>_aws.py          # AWS Glue / boto3 / SageMaker
<capability>_databricks.py   # Databricks / PySpark / MLflow / Unity Catalog
README.md                    # what it does, what to fill in, how to run on each platform
```

If a capability is truly platform-specific (e.g., a Step Functions JSON), document why no twin exists.

---

## File header standard

Every code file starts with this header block:

```python
"""
================================================================================
<TITLE> — <LAYER/CAPABILITY>  [AWS | Databricks]
================================================================================
Purpose : One-line what this does.
Pattern : Numbered steps of the flow.
Customize:
    - method_a(): what to change
    - CONST_B:    what to set
Args    : --ARG1, --ARG2 ...
Platform notes:
    - AWS:        Glue 5.x / Spark 3.5 / boto3 ...
    - Databricks: DBR 15.x LTS / MLflow 2.x / Unity Catalog ...
Version : <date> — <one-line change>
================================================================================
"""
```

---

## Placeholders — make them obvious

| Placeholder | Use for |
|---|---|
| `CHANGE_ME` | values a developer must replace (table, bucket, role) |
| `${variable}` | values resolved at deploy time (env, account_id) |
| `# TODO:` | logic a developer must implement |

Never leave a real account ID, bucket, or secret in a template.

---

## Coding conventions

- Python 3.11+, formatted with **black**, linted with **ruff**, imports sorted with **isort** (all configured in `pyproject.toml`).
- Type hints on public functions.
- No hardcoded environment fallbacks — **fail fast** on missing required args (`raise SystemExit("missing required arg --X")`).
- Idempotent + re-runnable: dynamic partition overwrite, MERGE, or upsert — never blind append that duplicates.
- Round float columns before write (2dp default).
- Log with the shared logger from `src/common/logging/`.

---

## Adding a new template — checklist

1. Find the right folder (or create one with a `README.md`).
2. Write `*_aws.py` and `*_databricks.py`.
3. Add/extend the folder `README.md` (purpose, fill-in guide, run commands).
4. Add a unit test under `tests/unit/`.
5. Tick the item in `ROADMAP.md` and bump the progress table.
6. Update `docs/BLUEPRINT_STATUS.md` if a whole phase moved.
7. Add a `CHANGELOG.md` entry.

---

## PR workflow

- Branch from `dev`: `feature/<area>-<short-desc>`.
- Run `make lint test` before pushing.
- PR title < 70 chars. Description: what changed, what was tested, follow-ups.
- Do not push to `main`/`master` directly.
- One reviewer minimum.

---

## Commit message style

Conventional commits:
```
feat(silver): add Databricks Autoloader silver template
fix(gold): correct window partition for product dimension
docs(runbooks): add HOWTO_ADD_NEW_MODEL
```

---

## Questions

Open an issue or ping the Data Platform owning team.

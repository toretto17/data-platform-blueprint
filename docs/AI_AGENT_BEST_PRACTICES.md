# 🤖 AI Agent Best Practices — Tips & Tricks for Kiro / AI-Assisted Development

How to use AI coding agents (Kiro, Cursor, Copilot) effectively for data engineering and MLOps work.

---

## 🎯 Context is King

The single most impactful thing you can do is give the AI **good context**.

### Create a project context file
```markdown
# my_project_context.md (put at root of your workspace)

## Who am I
- Role: Data Engineer
- Project: Revenue Analytics
- Stack: AWS Glue / PySpark / Delta / SageMaker

## Architecture
- Raw → Silver → Gold → Consumption
- Databases: silver_db, gold_db, consumption_db
- S3: s3://my-bucket-{env}-{layer}-{account}/

## Conventions
- Table naming: {layer}_{domain}_{entity} (e.g. silver_sales_daily)
- Partition: mnth_id (YYYYMM format)
- PK columns: always listed in BRD
- DQ: warn+skip pattern (never crash on missing rulesets)

## Active bugs / patterns to watch for
- COALESCE(col, 0) doesn't work when col is 0 (not NULL) — use NULLIF
- Window PARTITION BY must include ALL dimensions
- Rates: ratio-of-sums, NOT sum-of-ratios
```

### Use `@Docs` or `@context` to feed it
- In Kiro: the context file is auto-read at session start
- In Cursor: add docs to @Docs, reference with @context
- In Copilot: open the context file in a tab (it reads open files)

---

## 💡 Effective prompting for DE/MLOps

### Be specific about the task
```
❌ Bad:  "Write a Glue job"
✅ Good: "Write a Glue job that reads from silver_db.sales (partitioned by mnth_id),
         aggregates daily_ga by (partner_code, product, data_dt), computes MTD using
         a window function partitioned by (partner_code, product, mnth_id), rounds all
         floats to 2dp, and writes to gold_db.sales_mart with dynamic partition overwrite."
```

### Ask it to check documentation first
```
✅ "Check the latest Databricks docs for FeatureEngineeringClient.create_table —
    what are the exact parameters? Then build a feature table for my sales features."
```

### Decompose complex tasks
```
✅ "First, read the existing silver_sales ETL code and understand the schema.
    Then design the gold aggregation. Show me the plan before writing code."
```

---

## 🏗️ Agent workflow (how to structure sessions)

### For new ETL development:
```
1. "Read the BRD/spec for table X — what columns, what grain, what logic?"
2. "Check existing similar code (e.g. silver_sales) — what pattern does it follow?"
3. "Write the job following the same pattern. Use BaseSilverJob, override _define_sources
    and _apply_transformations."
4. "Add DQ checks (row_count, pk_not_null, freshness at minimum)."
5. "Run syntax check + lint."
6. "Create the DDB config for the SF framework."
```

### For debugging:
```
1. "Check the Glue job logs for [ERROR] lines — what's the actual error?"
2. "Read the ETL code at the line that failed — trace the variable/column."
3. "Check the source table schema — does the column exist? What type?"
4. "Compare source vs target aggregation — where's the mismatch?"
5. "Don't assume — prove with a query."
```

### For validation:
```
1. "Run STEP 0: SHOW COLUMNS, sample data, check granularity"
2. "Compare source vs target row counts and metric totals"
3. "Check derived columns (gap = target - actual) for consistency"
4. "Verify window partitions include all required dimensions"
5. "Round-trip: query both layers, diff, explain any non-zero rows"
```

---

## ⚡ Speed tips

| Tip | Why |
|---|---|
| Keep the context file updated | Agent gives better answers when it knows your conventions |
| Use "check before doing" | Avoids wasted work on wrong assumptions |
| Ask for a plan first (complex tasks) | Catches wrong direction before 500 lines of code |
| Reference existing code explicitly | "Follow the pattern in silver_sales.py" → consistent output |
| One task per message | Clearer → better output vs. multi-task messages |
| Ask it to validate its own output | "Syntax check this. Does it handle NULL edge cases?" |

---

## 🛡️ Safety rules (critical for production)

1. **Never let AI push to main/master** — always feature branch + PR
2. **Never let AI modify prod resources** without explicit approval
3. **Always review generated code** before deploying
4. **Watch for hardcoded values** — AI sometimes bakes in specific account IDs, regions
5. **Verify against docs** — AI can hallucinate API names/parameters
6. **Don't trust "it worked in the response"** — always run the actual test

---

## 📋 Template for starting a new AI session

```markdown
## Session goal
[one sentence: what do you want to accomplish today]

## Context
- Working on: [repo/project name]
- Branch: [current branch]
- Last session: [what was done last time / where you left off]

## Constraints
- Must follow: [specific patterns, naming conventions, etc.]
- Don't touch: [files/resources that should not be modified]
- Reference: [BRD doc, existing similar code, etc.]
```

---

## 🔧 Kiro-specific tips

| Feature | How to use | Benefit |
|---|---|---|
| Agent context file | `/home/user/rahul_kiro_agent_context.md` | Auto-loaded every session |
| Session files | `/home/user/kiro-sessions/YYYY-MM-DD_topic.md` | Continuity across sessions |
| Permission model | Agent asks before S3/Glue/DDB actions | Safety for production |
| Tool use | Reads files, runs commands, queries AWS | Hands-on debugging |
| Validation framework | Built into context (7-step checklist) | Systematic DQ process |

### Creating a good context file:
1. **Who** — your role, team, workspace paths
2. **What** — databases, tables, S3 paths, Glue jobs
3. **How** — conventions, naming, patterns, bug-patterns to watch
4. **Reference** — BRD locations, doc links, session history

---

## 📚 LLM-specific documentation approach

When using AI with your data platform:

1. **Keep docs machine-readable** — structured markdown with headers (not PDFs)
2. **Include examples** — AI learns better from examples than rules alone
3. **Document the "why"** — AI can then make better judgment calls
4. **Version your context** — update when architecture changes
5. **Session continuity** — save session notes so next session picks up seamlessly

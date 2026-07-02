# 🧱 Partitioning, File Sizing & Table Formats — DE Decision Guide

> **Scope:** How to choose partition columns, hit correct file sizes, handle skew
> (salting), and decide between **plain Parquet / Iceberg / Delta** on **AWS Glue**
> and **Databricks**. This is the decision layer behind the `WriteStrategy` classes
> in `common/utils/etl_utils.py`.
>
> Read this before choosing a partition scheme or table format for a new table.

---

## 0. The one rule that fixes most confusion

> **Partition grain follows the QUERY pattern, NOT the WRITE pattern.**

- Partition by the column consumers filter on (e.g. a monthly mart → partition by month).
- "Only a few days changed but I rewrite the whole month" is a **write-amplification**
  problem — it is **NOT** solved by making partitions finer (e.g. daily).
- Finer partitions fix write amplification but create the **small-files problem** AND
  make every month-query scan many prefixes instead of one.
- The correct fix is to **decouple the write mechanism from the partition layout**
  using a table format (Iceberg/Delta) that operates at **file** granularity (§4–§5).

---

## 1. Partition column: `YYYYMM` (int) vs `year=/month=`

For monthly data, prefer a **single integer `YYYYMM`** column over split `year`/`month`.

| Aspect | `mnth=202504` ✅ | `year=2025/month=04` |
|---|---|---|
| Single month | `WHERE mnth = 202504` | `WHERE year=2025 AND month=4` |
| **Cross-year range** | `BETWEEN 202511 AND 202602` ✅ contiguous, always prunes | `(year=2025 AND month>=11) OR (year=2026 AND month<=2)` ❌ OR-predicate may fail to prune → full scan |
| Monotonic ordering | ✅ enables range pruning / min-max skipping | ❌ `month=2` sorts before `month=12` of the prior year |
| Dynamic overwrite | ✅ single-column key | 2-column key (marginal extra shuffle) |
| Human readable | ✅ `202504` obvious | ✅ also clear |

**Why it matters technically:** partition elimination happens at the **catalog/metastore**
level *before* any object-store read. A contiguous `BETWEEN` on one monotonic column always
prunes cleanly. A disjunctive `OR` across two partition columns is where planners (older
Presto/Athena, some Spark pushdown configs) can fall back to a full scan.

**Use split `year/month/day/hour`** only when you have **millions of partitions** (deep
nesting helps object-store listing), e.g. high-frequency event data — not for coarse
(monthly/weekly) aggregates.

---

## 2. Choosing the GRAIN

| Grain | Partitions/year | Files/partition | Main risk |
|---|:--:|:--:|---|
| Monthly (`YYYYMM`) | 12 | large | write amplification (rewrite month for a few-day change) |
| Daily (`YYYYMMDD`) | 365 | small | small files; range-query scans many prefixes |
| Hourly | 8,760 | tiny | severe small-files; huge catalog |

**Decision rule:**
- Query cadence **monthly / ad-hoc reporting** → monthly grain.
- Query cadence **daily operational** + large daily volume → daily grain.
- **Streaming / event** data → daily or hourly + a table format with compaction.

Keep total partition count manageable (thousands, not millions) for catalog + planner health.
Manage write-amplification with a **table format** (§5), not by over-partitioning.

---

## 3. File sizing — the small-files problem

**Target: 128 MB – 1 GB per data file (256 MB is a good default).**

- Too small (<64 MB): object-store GET overhead, parquet footer reads, task overhead, slow reads.
- Too big (>1 GB): poor parallelism, coarse data-skipping, executor memory pressure.

### Illustrative example (why it happens)
A job with wide schema + window functions shuffles into ~160 Spark partitions. Written
as-is to a single monthly partition:

```
mnth=YYYYMM/  → 160 files × ~4 MB each   ❌  (≈60× too small)
```
Every downstream read then pays 160× GET + 160× footer parse. **Fix** — size the output
before write:

```python
# total_bytes / 256 MB  →  N files
target_files = max(1, total_bytes // (256 * 1024 * 1024))
df = df.repartition(int(target_files))      # or coalesce(N) if no upstream skew
```

**`repartition` vs `coalesce`:**
- `coalesce(N)` — no shuffle, only *reduces* partitions. Cheap. Use when current count ≥ N
  and data is evenly distributed.
- `repartition(N)` — full shuffle, can increase or rebalance. Use when you must *grow*
  partitions or fix skew.

### File-size decision (in code)
```
estimate output bytes  = est_rows × est_row_size
target_files           = ceil(bytes / target_file_bytes)   # target ~256 MB
if current_partitions  > target_files * 2:  coalesce(target_files)
elif current_partitions < target_files:     repartition(target_files)
else:                                        leave as-is
```
For Delta/Iceberg, prefer setting the engine's **target-file-size** property and running
**OPTIMIZE / rewrite_data_files** *after* write, rather than shuffling before write.

---

## 4. Data skew & SALTING

Skew = one key (or NULL) holds a disproportionate share of rows. Symptoms: one straggler
task runs 10–100× longer; OOM on a single executor; a few huge output files.

### How to detect
```sql
-- skew ratio = max partition rows / avg partition rows   (>3 is significant)
WITH c AS (SELECT <key>, COUNT(*) n FROM <table> WHERE <filter> GROUP BY <key>)
SELECT MAX(n) AS max_rows, AVG(n) AS avg_rows, MAX(n)/NULLIF(AVG(n),0) AS skew_ratio FROM c;

-- NULL concentration on the join/group key (>80% → treat as skew)
SELECT SUM(CASE WHEN <key> IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*) AS null_pct
FROM <table> WHERE <filter>;
```

### When to salt (decision)
| Condition | Action |
|---|---|
| `skew_ratio ≤ 3` and `null_pct < 50%` | No salting. Rely on **AQE skew join** (`spark.sql.adaptive.skewJoin.enabled=true`). |
| `skew_ratio > 3` on a **join** key | Enable AQE skew join first; if still slow, **salt** the skewed side. |
| `null_pct > 80%` on join key | Salt (or filter NULLs out and union back) — AQE alone often insufficient. |
| Skew on a **group-by/aggregation** key | Salt → partial aggregate → remove salt → final aggregate (two-stage agg). |
| Broadcastable small side (<~50 MB) | **Broadcast join** instead of salting (no shuffle at all). |

### Salting pattern (join)
```python
# Add salt to the skewed (large) side; explode the small side across salt buckets.
SALT_N = 16
large = large.withColumn("_salt", (F.rand() * SALT_N).cast("int"))
small = (small
         .withColumn("_salt", F.explode(F.array([F.lit(i) for i in range(SALT_N)]))))
joined = large.join(small, on=[join_key, "_salt"], how="inner").drop("_salt")
```

### Salting pattern (aggregation — two stage)
```python
SALT_N = 16
salted = df.withColumn("_salt", (F.rand() * SALT_N).cast("int"))
partial = salted.groupBy(group_key, "_salt").agg(F.sum("x").alias("x"))   # stage 1
final   = partial.groupBy(group_key).agg(F.sum("x").alias("x"))            # stage 2
```

> **Prefer AQE first.** Modern Spark AQE handles moderate skew automatically. Salt only
> when profiling proves AQE is insufficient — salting adds shuffle + code complexity.

---

## 5. File-level Copy-on-Write (the CoW myth)

**Myth:** "My data is under `mnth=YYYYMM/`, so any change rewrites the whole month."
**Reality:** A partition directory holds **many files**. Table formats operate on **files**, not the directory.

```
mnth=YYYYMM/
  part-00000.parquet   (early rows)
  part-00015.parquet   ← only this changed
  part-00039.parquet   (late rows)
```

| | Plain Parquet + Hive | Delta / Iceberg |
|---|---|---|
| One row changes | deletes ALL files in the dir, rewrites everything | rewrites ONLY the file(s) holding the changed rows (marks old removed in the log) |
| How it knows | it doesn't — directory is the atomic unit | per-file **min/max column stats** in the metadata layer |

### The dependency people miss: **file-level CoW needs CLUSTERING**
Data skipping is only effective if each file holds a **tight, non-overlapping** range of
the merge key. If keys are scattered across all files, every file's min/max spans the whole
range → no skipping → you rewrite most of the partition anyway (on any platform).

- **Delta:** `OPTIMIZE t ZORDER BY (key)` or **Liquid Clustering** (`CLUSTER BY`).
- **Iceberg:** `CALL system.rewrite_data_files(strategy => 'sort', sort_order => 'key')`
  or table-level `ALTER TABLE ... WRITE ORDERED BY key`.

> Inspect Iceberg file stats: `SELECT file_path, lower_bounds, upper_bounds FROM db.table.files;`

---

## 6. Table format decision: Parquet vs Iceberg vs Delta

| Capability | Plain Parquet + Catalog | **Iceberg** | **Delta** |
|---|:--:|:--:|:--:|
| File-level MERGE/UPDATE/DELETE | ❌ whole-partition rewrite | ✅ | ✅ |
| ACID / concurrent writers | ❌ | ✅ | ✅ |
| Time travel / rollback | ❌ | ✅ | ✅ |
| Data skipping (file stats) | ⚠️ partition-only | ✅ + clustering | ✅ + Z-ORDER/Liquid |
| Schema evolution | ⚠️ manual ALTER + recrawl | ✅ | ✅ |
| No `MSCK REPAIR` / catalog-lag | ❌ needs repair | ✅ metadata is source of truth | ✅ |
| Multi-engine (Spark/Trino/Athena/Flink) | Athena/Trino | ✅ best | Spark (+Trino/Athena via UniForm) |
| Auto compaction | ❌ manual | `rewrite_data_files` | Auto Optimize (managed) |

### A table format is still parquet on object storage — it ADDS a metadata layer
```
data/     mnth=YYYYMM/part-*.parquet     ← ordinary parquet files
metadata/ vN.metadata.json               ← schema, partition spec, current snapshot
          snap-*.avro (manifest list)     ← which manifests belong to a snapshot
          manifest-*.avro                  ← every data file + per-column min/max stats
```
That metadata layer is what turns a directory into a **transactional, file-tracked** table
— enabling ACID, MERGE, time-travel, and data skipping.

---

## 7. When to use WHICH — scenario matrix

| Scenario | AWS | Databricks |
|---|---|---|
| Batch mart, full refresh acceptable, reads dominate | Plain **Parquet + Catalog** (simple) or **Iceberg CoW** | **Delta** (default) |
| Few keys change per run; want incremental writes | **Iceberg + `MERGE INTO` (Copy-on-Write)** | **Delta + `MERGE` (+ CDF)** |
| High-frequency updates / streaming upserts | **Iceberg Merge-on-Read** or Hudi | **Delta MoR / deletion vectors** |
| Multi-engine (Spark + Trino + Athena + Flink) | **Iceberg** | Delta via UniForm |
| Row-level audit / rollback / time-travel needed | **Iceberg** | **Delta** |
| Pure append log, no updates | Plain Parquet append | Delta append |
| CDC from an OLTP source | DMS → object store → **Iceberg MERGE** (bookmarks for file-level) | **Delta Change Data Feed** + `APPLY CHANGES` |

### Copy-on-Write vs Merge-on-Read
- **CoW:** rewrites affected files on write → best for **read-heavy** (marts, dashboards).
- **MoR:** writes delete-files + deltas, merges at read → best for **write-heavy / frequent updates**.

### Change detection
- **Databricks:** Delta **CDF** (`table_changes()`) gives row-level inserts/updates/deletes cheaply.
- **AWS:** Glue **Job Bookmarks** (file-level) or an explicit **watermark** column; apply via Iceberg `MERGE INTO`.

---

## 8. Reference target design (generic, all layers)

```
Table → Iceberg (or Delta on Databricks)
  1. Partition by the query column        (e.g. months(event_ts) — hidden partitioning)
  2. Sort/cluster within partition by the MERGE/PK key(s)
        → tight file min/max → data skipping works → efficient MERGE
  3. Set target-file-size = 256 MB         (engine auto-sizes on write)
  4. Incremental ETL: MERGE INTO on the key → only changed files rewritten
  5. Periodic maintenance: rewrite_data_files (compact + re-sort) + expire_snapshots
```

**Databricks equivalent:**
```
Delta table, PARTITIONED BY (query_col)   [or Liquid CLUSTER BY (query_col, key)]
  delta.autoOptimize.optimizeWrite=true, autoCompact=true
  MERGE INTO ... ;  source changes via CDF table_changes()
  OPTIMIZE t ZORDER BY (key)   (or rely on Liquid Clustering)
```

### Honest tradeoff — when NOT to bother
For a batch table where a full-partition rewrite takes minutes and correctness > write cost,
**plain Parquet is genuinely fine.** Move to Iceberg/Delta when: rewrites get slow at
volume, you keep hitting catalog-lag / `MSCK` / permission-`ALTER` pain, you need row-level
correction / rollback, or the schema changes often.

---

## 9. Implementation checklist (wire into every layer job)

```
[ ] Partition column matches the consumer query pattern (§0–§1)
[ ] Grain chosen from query cadence + volume, not write frequency (§2)
[ ] Output file sizing applied before write (plain Parquet) OR
    target-file-size set + OPTIMIZE/rewrite scheduled (Delta/Iceberg) (§3)
[ ] Skew profiled; AQE skewJoin enabled; salting applied only if AQE insufficient (§4)
[ ] partitionOverwriteMode = dynamic (only touched partitions rewritten)
[ ] Table format chosen from the scenario matrix (§7)
[ ] If MERGE/CoW used: table sorted/clustered by the merge key (§5)
[ ] Floats rounded before write; no .count() for emptiness (use isEmpty())
```

---

## 10. Quick answers (cheat sheet)

- **YYYYMM vs year/month?** → `YYYYMM` (clean range predicates, monotonic).
- **Right grain?** → match query cadence; monthly for monthly-queried aggregates.
- **A few keys changed → rewrite whole partition?** → only with **plain Parquet**. Iceberg/Delta rewrite only the affected **files**.
- **Ideal file size?** → 128 MB–1 GB (256 MB default). Set target-file-size on Iceberg/Delta; coalesce/repartition on plain Parquet.
- **When to salt?** → only when `skew_ratio > 3` or `null_pct > 80%` and AQE is insufficient.
- **Z-order Databricks-only?** → No. Iceberg has sort-based clustering (`rewrite_data_files strategy=sort` / `WRITE ORDERED BY`).
- **Does CoW inspect my files?** → Yes, via per-file min/max stats. Efficiency depends on clustering by the merge key.
- **CDC easier on Databricks?** → Yes for change detection (CDF); AWS gets the equivalent via bookmarks/watermark + Iceberg `MERGE`.

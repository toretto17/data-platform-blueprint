# 📚 How to: Add an SCD Type 2 dimension table

1. Copy `src/de_patterns/scd_type2.py` from your platform tree
2. Set `KEYS` (business key), `TRACKED_COLS` (columns that trigger new versions)
3. Set `TARGET_TABLE` / `TARGET_PATH`
4. First run creates the table with `is_current`, `effective_start`, `effective_end` columns
5. Subsequent runs: changed rows get closed (is_current=false) + new version inserted
6. Query current state: `WHERE is_current = true`
7. Query point-in-time: `WHERE effective_start <= @date AND (effective_end IS NULL OR effective_end > @date)`

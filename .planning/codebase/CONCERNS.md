# Technical Concerns

**Analysis Date:** 2026-03-15

## Technical Debt

### High Priority

1. **No automated linting or formatting** — No `ruff`, `flake8`, `black`, or `pyproject.toml`. Code style is implicit PEP 8 only.
   - Files: entire codebase
   - Impact: inconsistency risk as codebase grows

2. **Duplicated constants across modules** — Some patterns defined in `pipeline/constants.py` are re-derived or partially duplicated in command functions.
   - Files: `pipeline/constants.py`, `pipeline/fixers.py`, `pipeline/ingest.py`

3. **Monolithic ingest module** — `pipeline/ingest.py` (951 lines) handles merge, ATS collection, and raw ingestion — three distinct concerns.
   - File: `pipeline/ingest.py`

4. **No connection context manager** — `get_db()` returns raw connection; callers manually close. Risk of connection leaks on exceptions.
   - File: `pipeline/db.py`

### Medium Priority

5. **Hardcoded database paths** — DB paths derived from `Path(__file__).resolve().parent.parent` rather than configuration.
   - File: `pipeline/db.py`

6. **No data rollback mechanism** — Pipeline fixes and backfills are idempotent but there's no undo/rollback capability.

7. **`typing` module uses old-style annotations** — Uses `List[str]` instead of `list[str]` (Python 3.9+), `Optional[Dict]` instead of `dict | None` (3.10+).
   - Files: throughout codebase

8. **Large command functions** — Some `cmd_*` functions exceed 100 lines (e.g., `cmd_ingest_raw`, `cmd_collect_ats`, `cmd_health_check`).

## Security Considerations

1. **f-string SQL in schema operations** — `ALTER TABLE` and `DELETE` statements use f-strings with variable interpolation:
   ```python
   cur.execute(f"ALTER TABLE job_postings_gold ADD COLUMN {col} {dtype}")
   cur.execute(f"DELETE FROM job_postings_gold WHERE {agg_conditions}")
   ```
   - Files: `pipeline/db.py:203`, `pipeline/fixers.py:167`
   - Risk: Low (values are developer-controlled constants, not user input), but pattern could be copied unsafely.

2. **HTTP requests without timeout** — `urllib.request.urlopen()` calls may not specify explicit timeouts in all paths.
   - File: `pipeline/enrichment.py`

3. **Environment variables for credentials** — `.env.example` exists but no validation that required vars are set before operations that need them.

## Performance Considerations

1. **Full-table scans** — Several commands do `SELECT * FROM job_postings_gold` and iterate all rows in Python.
   - Files: `pipeline/fixers.py`, `pipeline/qa.py`, `pipeline/enrichment.py`
   - Impact: Acceptable for current data size (~hundreds/low thousands of rows), won't scale to millions.

2. **Sequential HTTP requests** — ATS enrichment processes URLs one-by-one with no concurrency.
   - File: `pipeline/enrichment.py`
   - Impact: Slow for large batches; could benefit from `asyncio` or thread pool.

3. **In-memory processing** — All data loaded into memory for processing. No streaming or chunked processing.
   - Impact: Fine for current scale, limitation for growth.

4. **O(n*m) keyword matching** — `match_ai_keywords()` checks each keyword against full text with regex.
   - File: `pipeline/filters.py`

## Fragile Areas

1. **Title normalization pipeline** — Complex multi-step transformation in `pipeline/titles.py` (482 lines) with many regex patterns and edge cases. Most likely to break when adding new title patterns.

2. **Location parsing** — `_parse_location()` in `pipeline/ingest.py` uses hardcoded city/state lists. New locations require manual addition.

3. **Salary extraction** — `extract_salary()` in `pipeline/parsers.py` handles many formats (hourly, annual, K-suffix, ranges). Edge cases with non-USD currencies or unusual formats.

4. **AI role signature classification** — `classify_ai_role_signature()` in `pipeline/filters.py` has complex priority logic with many branches. Changes can shift classification for existing data.

## Scaling Limits

1. **SQLite single-writer** — WAL mode helps reads but only one writer at a time. Won't support concurrent pipeline runs.

2. **No database migration system** — Schema changes via `ALTER TABLE ADD COLUMN` with existence checks. No migration versioning (no Alembic, no migration history).

## Dependency Risks

1. **python-jobspy** — Used for job collection (`scripts/collect_jobs.py`). Third-party scraping library that may break with target site changes.

2. **plotly** — Used for dashboard generation in `pipeline/export.py`. Heavy dependency for a single feature.

3. **No dependency pinning** — `requirements.txt` likely doesn't pin exact versions (needs verification).

## Missing Features

1. **No database backup** — No automated backup before destructive operations (fix_data_quality deletes rows).

2. **No CLI help/discoverability** — Commands registered in COMMANDS dict but no `--help` descriptions per command.

3. **No CI/CD** — No GitHub Actions, no automated test runs on push.

4. **No logging to file** — `log()` function only writes to stdout. No log persistence.

## Test Coverage Gaps

Modules with **zero test coverage:**
- `pipeline/enrichment.py` (451 lines) — HTTP calls, ATS API
- `pipeline/export.py` (533 lines) — CSV, dashboard, approval
- `pipeline/companies.py` (251 lines) — Company list builder
- `pipeline/fixers.py` (455 lines) — Data quality fixes
- `pipeline/runner.py` (~50 lines) — Orchestration

Total untested: ~1,740 lines out of ~4,600 pipeline lines (**~38% uncovered**).

---

*Concerns analysis: 2026-03-15*

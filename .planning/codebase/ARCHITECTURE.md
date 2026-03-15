# Architecture

**Analysis Date:** 2026-03-15

## Pattern Overview

**Overall:** CLI-driven Data Pipeline (ETL + QA + Reporting)

**Key Characteristics:**
- Command-dispatch architecture: a CLI shim (`scripts/job_db_pipeline.py`) maps named commands to handler functions across pipeline modules
- SQLite as the single data store: all state lives in `db/job_postings_gold.db` (gitignored), no ORM
- Every command is idempotent: safe to re-run without side effects
- Strict separation: pure text parsers (no DB), classification/filter functions (no DB), and command functions (DB + I/O)
- Two-phase approval gate: QA checks must pass (0 CRITICAL violations) before analysis/dashboard generation is unlocked

## Layers

**CLI Entry / Dispatch:**
- Purpose: Parse CLI arguments and dispatch to the correct command handler
- Location: `scripts/job_db_pipeline.py`
- Contains: `argparse` setup, `COMMANDS` dict mapping string names to `cmd_*` functions
- Depends on: All `pipeline.*` modules (imports their `cmd_*` functions)
- Used by: User via `python scripts/job_db_pipeline.py <command>`

**Database Layer:**
- Purpose: Connection management, path constants, schema initialization, migrations
- Location: `pipeline/db.py`
- Contains: `get_db()`, `row_to_dict()`, `log()`, `cmd_init_db()`, all `Path` constants (`DB_PATH`, `REVIEW_DIR`, `OUTPUT_DIR`, `CLAUDE_DB`, `CODEX_DB`)
- Depends on: `pipeline/constants.py` (for `PLATFORM_CANONICAL`)
- Used by: Every other pipeline module

**Constants Layer:**
- Purpose: All data constants, compiled regex patterns, and lookup tables. No runtime logic.
- Location: `pipeline/constants.py`
- Contains: `AI_KEYWORDS`, `TITLE_AI_TERMS`, `ROLE_CLUSTERS_INCLUDED`, `ROLE_EXCLUSION_PATTERNS`, `PLATFORM_CANONICAL`, `SALARY_PATTERNS`, `SKILL_PATTERNS`, `TITLE_SEGMENTS`, `US_STATES`, `NON_US_MARKERS`, `EMERGING_AI_PATTERNS`, `LLM_GENAI_TERMS`, `AI_TEAM_TERMS`, etc.
- Depends on: Nothing (pure declarations)
- Used by: `pipeline/parsers.py`, `pipeline/filters.py`, `pipeline/ingest.py`, `pipeline/fixers.py`, `pipeline/titles.py`, `pipeline/qa.py`

**Pure Parsers Layer:**
- Purpose: Text-to-structured-data transformations with zero database access
- Location: `pipeline/parsers.py`
- Contains: `normalize_text()`, `normalize_company()`, `normalize_url()`, `canonical_job_key()`, `compute_title_ai_terms()`, `extract_salary()`, `extract_skills()`, `extract_company_from_url()`, `canonicalize_platform()`, `window_bucket()`, `normalize_title_to_segment()`, `strip_html()`, `is_aggregator_url()`
- Depends on: `pipeline/constants.py`
- Used by: `pipeline/ingest.py`, `pipeline/fixers.py`, `pipeline/enrichment.py`, `pipeline/qa.py`

**Filters / Classification Layer:**
- Purpose: Classify, filter, and categorize job data (AI keyword matching, role exclusion, AI role signature classification)
- Location: `pipeline/filters.py`
- Contains: `match_ai_keywords()`, `is_role_excluded()`, `classify_ai_role_signature()`, `resolve_work_mode()`
- Depends on: `pipeline/constants.py`
- Used by: `pipeline/ingest.py`, `pipeline/fixers.py`, `pipeline/qa.py`

**Ingestion Layer:**
- Purpose: Merge source databases, collect from ATS APIs, promote raw postings to gold
- Location: `pipeline/ingest.py`
- Contains: `cmd_merge_dbs()`, `cmd_collect_ats()`, `cmd_ingest_raw()`, ATS fetch helpers (`_fetch_greenhouse()`, `_fetch_lever()`), location parsing (`_parse_location()`), seniority detection, pre-ingest validation
- Depends on: `pipeline/db.py`, `pipeline/parsers.py`, `pipeline/filters.py`, `pipeline/constants.py`
- Used by: CLI dispatch

**Enrichment Layer:**
- Purpose: ATS API enrichment (Greenhouse, Ashby, Lever, JSON-LD), salary mining from text, URL freshness checking
- Location: `pipeline/enrichment.py`
- Contains: `cmd_mine_salary_from_body()`, `cmd_verify_and_enrich()`, `cmd_check_freshness()`, HTTP helpers (`_http_get_json()`, `_http_get_html()`)
- Depends on: `pipeline/db.py`, `pipeline/parsers.py`
- Used by: CLI dispatch

**Fixers / Backfill Layer:**
- Purpose: Data quality fixes and computed-field backfills
- Location: `pipeline/fixers.py`
- Contains: `cmd_normalize_platforms()`, `cmd_backfill_title_ai()`, `cmd_backfill_ai_role_signature()`, `cmd_backfill_skills()`, `cmd_fix_data_quality()` (13-step automated cleanup)
- Depends on: `pipeline/db.py`, `pipeline/parsers.py`, `pipeline/filters.py`, `pipeline/constants.py`
- Used by: CLI dispatch, `pipeline/runner.py`

**Title Normalization Layer:**
- Purpose: Advanced title cleaning and normalization (Type A: AI identity, Type B: AI team context, verbatim copy fix, out-of-scope flagging)
- Location: `pipeline/titles.py`
- Contains: `cmd_fix_title_normalization()` (7-step process), `_normalize_type_a()`, `_normalize_type_b()`, `_normalize_verbatim()`, `_clean_title_base()`, title constant tables (`TYPE_A_FUNCTION_MAP`, `AI_TEAM_STRIP`, `OUT_OF_SCOPE_TITLES`, `TITLE_SEGMENTS_EXTENDED`)
- Depends on: `pipeline/db.py`, `pipeline/constants.py`
- Used by: CLI dispatch, `pipeline/runner.py`

**QA / Health Check Layer:**
- Purpose: Validate data quality, populate qa_violations table, compute health scores
- Location: `pipeline/qa.py`
- Contains: `cmd_qa_check()` (per-row CRITICAL/WARNING violations), `cmd_health_check()` (comprehensive per-column validation with health score + JSON report)
- Depends on: `pipeline/db.py`, `pipeline/filters.py`, `pipeline/parsers.py`, `pipeline/constants.py`
- Used by: CLI dispatch, `pipeline/export.py` (approval gate)

**Export / Analysis Layer:**
- Purpose: CSV export, QA report generation, approval gate, Plotly dashboard, markdown research report
- Location: `pipeline/export.py`
- Contains: `cmd_export_review()`, `cmd_approve_db()`, `cmd_analyze_approved()`, `_generate_dashboard()`, `_generate_markdown_report()`, `_build_qa_report()`
- Depends on: `pipeline/db.py`, `pipeline/qa.py`, plotly (optional, for dashboard)
- Used by: CLI dispatch

**Runner / Orchestrator:**
- Purpose: Sequence all fix steps in correct dependency order
- Location: `pipeline/runner.py`
- Contains: `cmd_fix_all()` (6-step ordered execution)
- Depends on: `pipeline/fixers.py`, `pipeline/titles.py`, `pipeline/db.py`
- Used by: CLI dispatch

**Company Data Layer:**
- Purpose: Hard-coded list of 200 US tech/AI companies with tiers, sectors, ATS platforms
- Location: `pipeline/companies.py`
- Contains: `COMPANIES` list (200 entries), `cmd_build_companies()`
- Depends on: `pipeline/db.py`
- Used by: CLI dispatch

**Standalone Collection Script:**
- Purpose: LinkedIn job scraping via JobSpy (separate from main pipeline due to non-stdlib dependencies)
- Location: `scripts/collect_jobs.py`
- Contains: `collect_linkedin()`, query grid logic, rate limiting
- Depends on: `python-jobspy`, `pandas` (external), own `get_db()` (not shared with pipeline)
- Used by: User via `python scripts/collect_jobs.py linkedin`

## Data Flow

**Full Pipeline Flow (Happy Path):**

1. **Initialize**: `cmd_init_db()` creates all 7 SQLite tables + indexes + runs ALTER TABLE migrations
2. **Ingest Sources**: `cmd_merge_dbs()` imports Claude DB + Codex DB with URL + combo dedup, merging fields from both sources
3. **Collect New Data** (optional): `cmd_collect_ats()` fetches from Greenhouse/Lever APIs into `raw_postings`; `scripts/collect_jobs.py` scrapes LinkedIn into `raw_postings`
4. **Promote to Gold**: `cmd_ingest_raw()` filters (role exclusion, non-US, AI signal), validates, deduplicates, computes derived fields, and inserts into `job_postings_gold`
5. **Fix & Normalize**: `cmd_fix_all()` runs 6 ordered steps: normalize platforms -> fix data quality (13 sub-steps) -> backfill skills -> backfill title AI -> backfill AI role signature -> fix title normalization (7 sub-steps)
6. **Enrich**: `cmd_verify_and_enrich()` queries ATS APIs (Greenhouse/Ashby/Lever pay transparency) + JSON-LD from job pages + salary regex from descriptions
7. **QA Gate**: `cmd_qa_check()` populates `qa_violations` table with CRITICAL/WARNING violations
8. **Export for Review**: `cmd_export_review()` writes CSVs + JSON to `review/`
9. **Approve**: `cmd_approve_db()` blocks if CRITICAL > 0, otherwise inserts `approval_state`
10. **Analyze**: `cmd_analyze_approved()` (hard blocked until approved) generates Plotly dashboard + markdown report to `output/`

**Deduplication Strategy:**

1. **Primary**: Normalized URL exact match (`normalize_url()` strips params, protocol, www, trailing slashes)
2. **Secondary**: `(normalized_company, normalized_title, posted_date[:10])` combo match
3. **Canonical key**: SHA-256 of `{platform}|{source_job_id}|{normalized_url}|{company_id}`

**State Management:**
- All state is in SQLite (`db/job_postings_gold.db`), gitignored
- 7 tables: `job_postings_gold` (core), `companies_200`, `raw_postings` (staging), `qa_violations`, `source_attempts`, `approval_state`, `scrape_runs`
- `approval_state` table acts as a gate: `analyze_approved` hard-exits if not approved
- `enrich_status` column tracks per-row enrichment progress: `pending` -> `api_enriched` / `enriched` / `failed` / `skipped`
- `raw_postings.processed` column tracks ingestion status: `0`=pending, `1`=inserted/deduped, `2`=excluded role, `3`=non-US, `4`=no AI signal, `5`=validation failed

## Key Abstractions

**Command Functions (`cmd_*`):**
- Purpose: Each represents one pipeline step, invokable by name from CLI
- Examples: `pipeline/db.py::cmd_init_db`, `pipeline/ingest.py::cmd_merge_dbs`, `pipeline/qa.py::cmd_qa_check`, `pipeline/export.py::cmd_approve_db`
- Pattern: Each function opens its own DB connection, does work, commits, closes. No shared state between commands.

**AI Role Signature (7-tier classification):**
- Purpose: Categorize how strongly a job posting is related to AI
- Defined in: `pipeline/filters.py::classify_ai_role_signature()`, constants in `pipeline/constants.py`
- Tiers (priority order): `emerging_ai_named_role` > `ai_in_title` > `ai_team_or_platform_in_title` > `llm_or_genai_in_scope` > `agentic_in_scope` > `ai_team_or_platform_in_scope` > `ai_in_description_only`

**Title Normalization Types:**
- Purpose: Reduce raw job titles to canonical segments for analysis
- Defined in: `pipeline/titles.py`
- Type A (AI identity): Titles with AI in the role itself -> "AI Analyst", "AI Scientist"
- Type B (AI team context): Titles on AI teams -> "Data Analyst (AI Team)"
- Verbatim fallback: Clean junk, match against `TITLE_SEGMENTS` + `TITLE_SEGMENTS_EXTENDED`

**Pre-Ingest Validation Gate:**
- Purpose: Reject bad rows before they enter gold table
- Defined in: `pipeline/ingest.py::validate_pre_ingest()`
- Checks: Required fields, URL format, salary sanity, enum validation

## Entry Points

**Primary CLI Entry:**
- Location: `scripts/job_db_pipeline.py`
- Triggers: `python scripts/job_db_pipeline.py <command>`
- Responsibilities: Parse command name, dispatch to the corresponding `cmd_*` function via `COMMANDS` dict

**Standalone Collection Entry:**
- Location: `scripts/collect_jobs.py`
- Triggers: `python scripts/collect_jobs.py linkedin [options]`
- Responsibilities: Scrape LinkedIn via JobSpy, write to `raw_postings` table in the shared DB

## Error Handling

**Strategy:** Log-and-continue for individual rows; hard-block for critical pipeline violations

**Patterns:**
- Individual row errors during merge/ingest are caught, logged with `log(f"WARN: ...")`, and skipped
- `validate_pre_ingest()` returns a list of error strings; non-empty = row rejected with `processed=5`
- QA violations are recorded in `qa_violations` table, not raised as exceptions
- `cmd_approve_db()` blocks (returns False) if CRITICAL violation count > 0
- `cmd_analyze_approved()` calls `sys.exit(1)` if DB is not approved
- ATS API/HTTP errors are caught per-request; individual failures do not abort the batch
- SQLite constraint violations (e.g., unique index) handled via `INSERT OR IGNORE`

## Cross-Cutting Concerns

**Logging:** `pipeline/db.py::log()` prints `[HH:MM:SS] message` to stdout. Used consistently across all modules. No log file, no log levels.

**Validation:** Multi-layer: pre-ingest validation (before INSERT), QA checks (after INSERT, blocking approval), health check (comprehensive per-column audit with JSON report)

**Authentication:** None for the pipeline itself. Optional API keys (`FIRECRAWL_API_KEY`, `TAVILY_API_KEY`, `PROXY_URL`) loaded from `.env` for enrichment/collection. ATS APIs (Greenhouse, Lever, Ashby) are public and require no auth.

**Idempotency:** Every `cmd_*` function is designed to be re-runnable. Uses `INSERT OR IGNORE`, `CREATE TABLE IF NOT EXISTS`, `ALTER TABLE` migrations check existing columns, and all fix steps produce the same result on re-run.

---

*Architecture analysis: 2026-03-15*

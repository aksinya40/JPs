# Technology Stack

**Analysis Date:** 2026-03-15

## Languages

**Primary:**
- Python 3.10+ - All pipeline code, CLI, data processing, and analysis
  - Type hints used throughout (`typing.List`, `typing.Tuple`, `typing.Dict`, `typing.Optional`)
  - f-strings for string formatting everywhere
  - Walrus operator not used; compatible down to 3.8 for core pipeline (stdlib-only), but `collect_jobs.py` uses `int | None` union syntax requiring 3.10+

**Secondary:**
- SQL (SQLite dialect) - Inline SQL in Python via `sqlite3`, no ORM
- HTML/CSS/JavaScript - Dashboard output generation (Plotly-rendered, inline `<script>` tags in `pipeline/export.py`)

## Runtime

**Environment:**
- CPython 3.10+ (comment in `requirements.txt` recommends 3.11)
- No `.python-version` file present

**Package Manager:**
- pip (or uv recommended in comments: `uv venv --python 3.11 .venv`)
- Lockfile: **missing** - only `requirements.txt` with loose version pins (`>=`)

## Frameworks

**Core:**
- No web framework - this is a CLI data pipeline, not a web app
- `argparse` (stdlib) - CLI argument parsing in `scripts/job_db_pipeline.py` and `scripts/collect_jobs.py`

**Testing:**
- pytest - test runner (tests discovered via `tests/` directory with `conftest.py`)

**Build/Dev:**
- No build system (no `setup.py`, `pyproject.toml`, or `setup.cfg`)
- No Makefile or task runner
- Manual CLI invocation: `python scripts/job_db_pipeline.py <command>`

## Key Dependencies

**Critical (stdlib - zero install required for core pipeline):**
- `sqlite3` - All data storage and querying
- `urllib.request` / `urllib.error` - All HTTP requests (no requests/httpx)
- `json` - JSON parsing for API responses, JSON-LD extraction, report generation
- `re` - Extensive regex usage for salary parsing, AI keyword matching, title normalization, URL parsing
- `hashlib` - SHA-256 for `canonical_job_key` generation (`pipeline/parsers.py`)
- `csv` - CSV export (`pipeline/export.py`)
- `argparse` - CLI interface
- `pathlib` - All path handling

**Optional (external packages in `requirements.txt`):**
- `python-jobspy` >= 1.1.0 - LinkedIn job scraping via JobSpy library (only used by `scripts/collect_jobs.py`)
- `pandas` >= 2.0.0 - DataFrame handling for JobSpy results (only used by `scripts/collect_jobs.py`)
- `plotly` - Interactive dashboard chart generation (only used by `pipeline/export.py` in `_generate_dashboard()`, gracefully degrades if missing)

**Not in requirements.txt but imported:**
- `plotly.graph_objects`, `plotly.subplots`, `plotly.io` - Used in `pipeline/export.py` with a try/except ImportError guard

## Configuration

**Environment:**
- `.env.example` present with 3 optional variables (see INTEGRATIONS.md for details)
- `.env` file loaded manually in `scripts/collect_jobs.py` via custom `load_dotenv()` function (no `python-dotenv` dependency)
- Core pipeline (`scripts/job_db_pipeline.py`) does NOT read `.env` at all - the enrichment module uses only public APIs

**Build:**
- No build configuration files
- No `pyproject.toml`, `setup.py`, or `setup.cfg`
- Package structure: `pipeline/` is an importable package (`pipeline/__init__.py` exists with `__version__ = "1.0.0"`)

**Database:**
- All paths hardcoded in `pipeline/db.py`:
  - `DB_PATH`: `<project_root>/db/job_postings_gold.db`
  - `CLAUDE_DB`: `~/Documents/Claude Code/ai_analyst_roles_2026/db/job_postings_gold.db`
  - `CODEX_DB`: `~/Documents/Codex/artifacts/ai_analyst_roles_2026/db/job_postings_gold.db`
  - `REVIEW_DIR`: `<project_root>/review/`
  - `OUTPUT_DIR`: `<project_root>/output/`

## Platform Requirements

**Development:**
- Python 3.10+ (3.11 recommended)
- macOS (paths reference `~/Documents/Claude Code/` and `~/Documents/Codex/`)
- No Docker, no containerization
- SQLite3 (included with Python)

**Production:**
- Not a deployed application - runs locally as a research pipeline
- Output is static files: `dashboard.html`, `AI_Analyst_Roles_Research_2026.md`, CSV exports
- Database is a local SQLite file (`db/job_postings_gold.db`, gitignored)

## Architecture Notes

**Zero-dependency core:** The main pipeline (`scripts/job_db_pipeline.py` and everything in `pipeline/`) uses only Python stdlib. This is intentional - HTTP requests use `urllib.request` instead of `requests`, and the Plotly import is guarded with try/except.

**Separate collection script:** `scripts/collect_jobs.py` is isolated from the main pipeline specifically because it requires external deps (`python-jobspy`, `pandas`). It writes to `raw_postings` table, then `ingest_raw` promotes to gold.

**No ORM:** All database access is raw SQL via `sqlite3`. The `get_db()` function in `pipeline/db.py` returns a connection with WAL mode and foreign keys enabled. Row factory is `sqlite3.Row` for dict-like access.

---

*Stack analysis: 2026-03-15*

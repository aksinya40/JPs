# Directory Structure

**Analysis Date:** 2026-03-15

## Top-Level Layout

```
JPs/
├── pipeline/          # Core Python package — all business logic
├── scripts/           # CLI entry points
├── tests/             # pytest test suite
├── review/            # Generated reports, CSVs, health JSONs
├── output/            # Final output artifacts (research docs)
├── .env.example       # Environment variable template
├── .gitignore
├── requirements.txt   # Python dependencies
└── README.md
```

## Directory Purposes

### `pipeline/` — Core Package
All business logic lives here. 13 modules, single-concern design.

| File | Purpose | Lines |
|------|---------|-------|
| `__init__.py` | Package marker, `__version__` only | ~5 |
| `constants.py` | All regex patterns, keyword lists, lookup tables | 298 |
| `parsers.py` | Pure text transformations (no DB) | 241 |
| `filters.py` | Classification & filtering (no DB) | 129 |
| `db.py` | Database connection, schema init, helpers | 262 |
| `ingest.py` | Data ingestion: merge, collect ATS, ingest raw | 951 |
| `enrichment.py` | ATS API enrichment, salary mining, freshness | 451 |
| `fixers.py` | Data quality fixes & backfills (13 steps) | 455 |
| `titles.py` | Title normalization pipeline | 482 |
| `qa.py` | QA checks & health check reporting | 648 |
| `export.py` | CSV export, approval, dashboard generation | 533 |
| `companies.py` | Company list builder (200 target companies) | 251 |
| `runner.py` | Pipeline orchestration (dependency ordering) | ~50 |

### `scripts/` — Entry Points
| File | Purpose |
|------|---------|
| `job_db_pipeline.py` | Main CLI: `python scripts/job_db_pipeline.py <command>` |
| `collect_jobs.py` | Job collection script (python-jobspy integration) |

### `tests/` — Test Suite
| File | Tests For | Lines |
|------|-----------|-------|
| `conftest.py` | Shared fixtures (`in_memory_db`, `sample_job_row`) | 54 |
| `test_parsers.py` | `pipeline/parsers.py` | 372 |
| `test_filters.py` | `pipeline/filters.py` | 239 |
| `test_skills.py` | Skill extraction from `pipeline/parsers.py` | 229 |
| `test_titles.py` | `pipeline/titles.py` | 205 |
| `test_ingest.py` | `pipeline/ingest.py` (location, seniority, validation) | 317 |
| `test_qa.py` | `pipeline/qa.py` | 341 |
| `test_health_check.py` | Health check system in `pipeline/qa.py` | 653 |

### `review/` — Generated Reports
Contains output from pipeline commands:
- `qa_report.json` — QA violation report
- `qa_violations.csv` — Violations in CSV format
- `health_*.json` — Health check snapshots (timestamped)
- `job_postings_gold.csv` — Full export of approved data
- `companies_200.csv` — Target company list
- `random_spot_check_30.csv` — Random sample for manual review
- `source_attempts.csv` — ATS source attempt log
- `*.md` — Change plans, design docs, review notes

### `output/` — Final Artifacts
- `AI_Analyst_Roles_Research_2026.md` — Final research output document

## Key File Locations

| What | Where |
|------|-------|
| Main CLI entry point | `scripts/job_db_pipeline.py` |
| Job collection | `scripts/collect_jobs.py` |
| Database schema | `pipeline/db.py:cmd_init_db()` |
| All constants/patterns | `pipeline/constants.py` |
| Pure text functions | `pipeline/parsers.py` |
| AI keyword matching | `pipeline/filters.py` |
| Title normalization | `pipeline/titles.py` |
| Data quality fixes | `pipeline/fixers.py` |
| Health check | `pipeline/qa.py:cmd_health_check()` |
| Test fixtures | `tests/conftest.py` |

## Naming Conventions

**Files:** `snake_case.py` everywhere
**Test files:** `test_<module>.py` mirrors `pipeline/<module>.py`
**Directories:** lowercase, no hyphens or underscores
**Generated reports:** `<type>_<timestamp>.json` for snapshots

## Where to Add New Code

- **New parser/transformer:** `pipeline/parsers.py` (pure function, no DB)
- **New filter/classifier:** `pipeline/filters.py` (pure function, no DB)
- **New pipeline command:** Add `cmd_<name>()` in appropriate module, register in `scripts/job_db_pipeline.py` COMMANDS dict
- **New constant/pattern:** `pipeline/constants.py`
- **New test:** `tests/test_<module>.py` with class per function group
- **New data quality fix:** `pipeline/fixers.py`, add to `cmd_fix_data_quality()` sequence

## Database Files (gitignored)

- `data/ai_jobs.db` — Main gold database (SQLite)
- `data/claude_jobs.db` — Claude-collected jobs (pre-merge)
- `data/codex_jobs.db` — Codex-collected jobs (pre-merge)

---

*Structure analysis: 2026-03-15*

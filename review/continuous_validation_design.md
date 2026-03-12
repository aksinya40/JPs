# Continuous Validation Architecture
_Goal: run anytime, fix what can be fixed, validate everything, report health score_

---

## The Problem with the Current Design

Right now the system has two separate, manual commands:
- `cmd_fix_data_quality()` → fixes things, then you stop
- `cmd_qa_check()` → checks 12 rules, then you stop

Neither one is the full picture. If you run ingest tomorrow and add 400 new rows,
nothing automatically fixes or validates them. You have to remember what to run
and in what order. The DB can silently drift back to a broken state.

---

## The New Architecture: Two Commands

### `cmd_fix_all()` — in `pipeline/runner.py`
One master idempotent fixer. Run it anytime. Safe to run 100 times.
Runs all fix steps in the correct dependency order.

### `cmd_health_check()` — in `pipeline/qa.py`
Validates EVERY column in the schema for EVERY row.
Returns a health score (0–100%) + full report saved to `review/health_YYYY-MM-DD.json`.

### Normal usage pattern
```python
# After any ingest / after any data change / anytime you want to know the state:
from pipeline.runner import cmd_fix_all
from pipeline.qa import cmd_health_check

cmd_fix_all()         # fix everything fixable
cmd_health_check()    # validate everything, print score, save report
```

That's it. Two commands, full pipeline, every time.

---

## `cmd_fix_all()` — Execution Order

All steps are **idempotent** (safe to re-run):

```
Step 01  cmd_normalize_platforms()          # canonical platform names
Step 02  cmd_fix_data_quality()             # 11-step master fix:
           └─ 01 delete non-US rows
           └─ 02 delete aggregator URLs
           └─ 03 fix Unknown companies from URL slug
           └─ 04 strip HTML from description_snippet
           └─ 05 re-extract skills (title + snippet)
           └─ 06 build title_normalized taxonomy
           └─ 07 fix work_mode contradictions
           └─ 08 fix salary_text corruption
           └─ 09 recheck date_uncertain flags
           └─ 10 classify Applied Scientist scope
           └─ 11 add/update in_target_list column
           └─ 12 [NEW] fallback posted_date from raw_postings.scraped_at
           └─ 13 [NEW] backfill short snippets from raw_postings.body_raw
Step 03  cmd_backfill_skills()              # full body_raw join for missing skills
Step 04  cmd_backfill_title_ai()            # has_ai_in_title + title_ai_terms
Step 05  cmd_backfill_ai_role_signature()   # ai_role_signature classification
Step 06  cmd_fix_title_normalization()      # 7-step: Type A, Type B, verbatim fix
```

---

## `cmd_health_check()` — Column-Level Validation Rules

Checks every column in `job_postings_gold` for every row with `status='Open'`.
Severity: **CRITICAL** = data is wrong or missing in a way that breaks the product.
           **WARNING**  = data is incomplete but the row is usable.

### Required fields (CRITICAL if empty/null)
| Column | Rule |
|---|---|
| `canonical_job_key` | NOT NULL, NOT '' |
| `company_name` | NOT NULL, NOT '' |
| `source_platform` | NOT NULL, NOT '', must be in canonical platform list |
| `source_job_id` | NOT NULL, NOT '' |
| `job_url` | NOT NULL, starts with 'https://', not aggregator domain |
| `title` | NOT NULL, NOT '', length >= 3 |
| `title_normalized` | NOT NULL, NOT '', NOT equal to raw `title` (verbatim = broken) |
| `role_cluster` | NOT NULL, NOT '' |
| `is_us` | must be 1 for all Open rows |
| `status` | must be in ('Open', 'Closed', 'Excluded') |
| `ai_keywords_hit` | NOT NULL, NOT '', NOT '[]', NOT 'null' |
| `date_uncertain` | must be 0 or 1 (not NULL) |

### Enum/value checks (CRITICAL if invalid value)
| Column | Allowed values |
|---|---|
| `source_platform` | Greenhouse, Lever, LinkedIn, Workday, ... (PLATFORM_CANONICAL values) |
| `work_mode` | On-site, Remote, Hybrid, Unknown |
| `status` | Open, Closed, Excluded |
| `enrich_status` | pending, enriched, failed, skipped |
| `has_python` | 0 or 1 |
| `has_sql` | 0 or 1 |
| `has_ai_in_title` | 0 or 1 |
| `date_uncertain` | 0 or 1 |

### Numeric range checks (CRITICAL if out of range)
| Column | Rule |
|---|---|
| `url_http_status` | if set: 404 or 410 → CRITICAL (dead URL) |
| `salary_min_usd` | if set: must be >= 15,000 and <= 500,000 |
| `salary_max_usd` | if set: must be >= salary_min and <= 600,000; ratio max/min <= 5 |

### Format checks (CRITICAL if format invalid)
| Column | Rule |
|---|---|
| `posted_date` | if set and date_uncertain=0: must match YYYY-MM-DD, must be in 2025-07-01..2026-03-31 |
| `job_url` | must match https?://... pattern |
| `ai_keywords_hit` | if set: must be valid JSON (parseable as list) |
| `created_at` | must be parseable datetime |

### Cross-column consistency (CRITICAL if contradicted)
| Rule | Check |
|---|---|
| work_mode_contradiction | work_mode='On-site' AND location contains 'remote' |
| salary_inverted | salary_min_usd > salary_max_usd |
| title_verbatim | title_normalized == title (normalization not applied) |
| not_us_open | is_us=0 AND status='Open' |

### Optional fields — WARNING if missing on Open rows
| Column | Why it matters |
|---|---|
| `description_snippet` | NULL or len < 50 → poor data quality |
| `skills_extracted` | NULL or '' → can't filter by skill |
| `location_raw` | NULL → unknown location |
| `location_standardized` | NULL → can't filter by location |
| `work_mode` | NULL → can't filter by remote/on-site |
| `posted_date` | NULL → can't tell if job is fresh |
| `seniority` | NULL → acceptable, just note coverage |
| `ai_role_signature` | NULL → classification not run |

### Aggregate checks (CRITICAL if threshold breached)
| Rule | Threshold |
|---|---|
| row_count_below_300 | active Open+US rows < 300 |
| companies_200_count_wrong | companies_200 != 200 rows |
| duplicate_job_key | any canonical_job_key appears > 1 time |
| title_verbatim_rate | verbatim title_normalized > 5% of Open rows |
| skills_missing_rate | skills_extracted empty > 20% of Open rows |
| description_missing_rate | description_snippet < 50 chars > 30% of Open rows |
| unknown_company_rate | company_name='Unknown' >= 5 Open rows |
| date_uncertain_rate | date_uncertain=1 > 30% of Open rows |

---

## Health Report Format

`cmd_health_check()` prints this to console and saves to `review/health_YYYY-MM-DD_HHMMSS.json`:

```
╔══════════════════════════════════════════════════════════╗
║  DB HEALTH CHECK  2026-03-12 14:32:17                    ║
╠══════════════════════════════════════════════════════════╣
║  Total rows:    3,061  |  Active (Open):  2,976           ║
║                                                           ║
║  CRITICAL violations:    0   ✅                           ║
║  WARNING  violations:   14                                ║
║                                                           ║
║  Health score:        99.5%                               ║
╠══════════════════════════════════════════════════════════╣
║  COLUMN COVERAGE (Open rows)                              ║
║  title_normalized valid:   2,976 / 2,976  100.0%  ✅     ║
║  ai_keywords_hit valid:    2,976 / 2,976  100.0%  ✅     ║
║  description_snippet≥50:  2,962 / 2,976   99.5%  ⚠️     ║
║  skills_extracted valid:   2,920 / 2,976   98.1%  ⚠️     ║
║  posted_date valid:        2,737 / 2,976   91.9%  ⚠️     ║
║  work_mode set:            2,976 / 2,976  100.0%  ✅     ║
╚══════════════════════════════════════════════════════════╝
```

---

## What Stays the Same vs What Changes

### Stays the same
- All existing fix functions in `fixers.py` (they become steps in `cmd_fix_all`)
- All existing QA rules in `qa.py` (they become part of `cmd_health_check`)
- Schema in `db.py` (no changes)
- Tests in `test_qa.py` (stays, add new tests for health_check)
- `qa_violations` table (still used, now more rows from column-level checks)

### Changes / New
1. **`pipeline/runner.py`** (NEW FILE) — contains `cmd_fix_all()`
2. **`pipeline/qa.py`** — add `cmd_health_check()` alongside `cmd_qa_check()`
3. **`pipeline/fixers.py`** — add Steps 12 and 13 to `cmd_fix_data_quality()`
4. **`pipeline/ingest.py`** — add `validate_pre_ingest(row)` before promotion to gold
5. **`tests/test_health_check.py`** (NEW FILE) — tests for per-column rules

---

## Pre-Ingest Validation (gate at entry, not just at check time)

Add to `cmd_ingest_raw()` in `ingest.py` before inserting into gold:

```python
INGEST_REQUIRED = ['title', 'job_url', 'source_job_id', 'company_name']
INGEST_ENUM = {
    'status': {'Open', 'Closed'},
    'work_mode': {'On-site', 'Remote', 'Hybrid', 'Unknown'},
}

def validate_pre_ingest(row: dict) -> list[str]:
    errors = []
    for f in INGEST_REQUIRED:
        if not row.get(f) or str(row[f]).strip() == '':
            errors.append(f'{f} missing')
    if row.get('job_url') and not row['job_url'].startswith('http'):
        errors.append('job_url not a valid URL')
    if row.get('salary_min_usd') and row['salary_min_usd'] > 0:
        if row['salary_min_usd'] < 10000:
            errors.append(f'salary_min_usd={row["salary_min_usd"]} too low')
    for col, allowed in INGEST_ENUM.items():
        if row.get(col) and row[col] not in allowed:
            errors.append(f'{col}={row[col]} not in allowed set')
    return errors
```

This means dirty data can't enter the gold table at all — errors are logged to `source_attempts` and skipped.

---

## Files to Create / Modify (implementation order)

```
1. pipeline/runner.py          NEW  — cmd_fix_all()
2. pipeline/fixers.py          EDIT — add steps 12 + 13
3. pipeline/qa.py              EDIT — add cmd_health_check() with full column coverage
4. pipeline/ingest.py          EDIT — add validate_pre_ingest()
5. tests/test_health_check.py  NEW  — per-column validation tests
```

That's 5 files. The rest of the codebase stays unchanged.

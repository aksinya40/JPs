# JPs — Unified AI Analyst Jobs Research Pipeline

A unified research pipeline for collecting, merging, enriching, and analyzing AI-adjacent analyst job postings from 200+ US tech companies.

## Overview

This project merges two independently collected SQLite databases of AI analyst job postings into a unified gold-standard dataset, then runs multi-tier enrichment, QA validation, and produces an interactive Plotly dashboard + markdown research report.

**Scope:** US-only, 200 top tech/AI companies, analyst & data-science roles with AI signals.

## Repository Structure

```
JPs/
├── scripts/
│   └── job_db_pipeline.py   # Main pipeline (all commands)
├── db/
│   └── job_postings_gold.db  # Unified DB (gitignored)
├── review/                    # CSV exports + QA report (committed)
│   ├── job_postings_gold.csv
│   ├── companies_200.csv
│   ├── qa_violations.csv
│   ├── random_spot_check_30.csv
│   ├── source_attempts.csv
│   └── qa_report.json
├── output/                    # Dashboard + report (gitignored)
│   ├── dashboard.html
│   └── research_report.md
├── .env.example               # API key template
├── .gitignore
└── README.md
```

## Quick Start

```bash
# 1. Clone
git clone https://github.com/aksinya40/JPs.git
cd JPs

# 2. Set up env (optional — needed only for Tier 3 enrichment)
cp .env.example .env
# Fill in FIRECRAWL_API_KEY and TAVILY_API_KEY

# 3. Initialize DB + merge sources
python scripts/job_db_pipeline.py init_db
python scripts/job_db_pipeline.py merge_dbs

# 4. Backfill computed fields
python scripts/job_db_pipeline.py backfill_title_ai
python scripts/job_db_pipeline.py backfill_ai_role_signature
python scripts/job_db_pipeline.py backfill_skills

# 5. Normalize platforms + mine salary from descriptions
python scripts/job_db_pipeline.py normalize_platforms
python scripts/job_db_pipeline.py mine_salary_from_body

# 6. QA + export for human review
python scripts/job_db_pipeline.py qa_check
python scripts/job_db_pipeline.py export_review

# 7. Review CSV files in review/, fix any CRITICAL violations

# 8. Approve (blocked if CRITICAL > 0)
python scripts/job_db_pipeline.py approve_db

# 9. Generate dashboard + report (requires approval)
python scripts/job_db_pipeline.py analyze_approved
```

## Pipeline Commands

| Command | Description |
|---------|-------------|
| `init_db` | Create/migrate all tables and indexes |
| `merge_dbs` | Import Claude DB + Codex DB with URL + combo dedup |
| `build_companies` | Populate companies_200 with 200 hardcoded companies |
| `normalize_platforms` | Map raw platform names → canonical forms |
| `mine_salary_from_body` | Parse salary from description_snippet via regex |
| `verify_and_enrich` | Multi-tier ATS API enrichment (Greenhouse, Ashby, Lever, JSON-LD) |
| `backfill_title_ai` | Compute `has_ai_in_title` + `title_ai_terms` |
| `backfill_ai_role_signature` | Classify AI role signature (7 priority tiers) |
| `backfill_skills` | Extract `skills_extracted`, `has_python`, `has_sql` |
| `qa_check` | Run all QA rules, write violations table |
| `export_review` | Export CSVs + qa_report.json to `review/` |
| `approve_db` | Insert approval_state (blocked if CRITICAL > 0) |
| `analyze_approved` | Generate Plotly dashboard + markdown report |

## Unified Schema

**job_postings_gold** — core table with 40+ columns including:
- Identity: `gold_id`, `canonical_job_key`, `company_id`, `company_name`
- Source: `source_platform`, `source_job_id`, `job_url`
- Content: `title`, `title_normalized`, `role_cluster`, `seniority`
- AI signals: `ai_signal_types`, `ai_keywords_hit`, `has_ai_in_title`, `title_ai_terms`, `ai_role_signature`
- Salary: `salary_currency`, `salary_min_usd`, `salary_max_usd`, `salary_period`, `salary_text`
- Location: `location_raw`, `location_standardized`, `country`, `is_us`, `work_mode`
- Skills: `skills_extracted`, `has_python`, `has_sql`
- Enrichment: `enrich_status`, `description_snippet`

**companies_200** — 200 US tech/AI companies with tier, sector, ATS platform, career page URL.

## Merge Strategy

- **Primary dedup:** normalized URL exact match
- **Secondary dedup:** `(company_name, title, posted_date[:10])` combo match
- **Field merge:** Codex salary preferred when Claude is NULL; Claude AI fields preserved

## QA Checks

**CRITICAL** (blocking — must fix before approval):
- `row_count_below_300` — fewer than 300 gold rows
- `companies_200_count_wrong` — not exactly 200 companies
- `duplicate_job_key` — duplicate canonical_job_key
- `required_field_null` — NULL company_name, title, or job_url
- `not_us` — non-US rows (country ≠ US or is_us ≠ 1)
- `url_not_reachable` — HTTP status 4xx/5xx
- `no_ai_signal` — no AI keywords or AI in title
- `role_excluded` — title matches exclusion pattern
- `date_out_of_window` — posted_date before 2025-01-01

**WARNING** (non-blocking):
- `salary_insane` — salary outside $15K–$600K or max/min ratio > 5
- `missing_description` — no description_snippet
- `date_uncertain_high_ratio` — > 50% rows with uncertain dates

## AI Role Signature Tiers

1. `emerging_ai_named_role` — title contains "AI Analyst", "LLM Analyst", etc.
2. `llm_or_genai_in_scope` — scope/skills mention LLM, GenAI, agentic
3. `ai_team_or_platform_in_title` — title has "AI Platform", "ML Team"
4. `ai_team_or_platform_in_scope` — description has AI team context
5. `ai_in_title` — title contains AI/ML/NLP keyword
6. `ai_in_skills` — skills include AI-related skills
7. `ai_in_description_only` — AI keywords only in description

## Source Databases

| DB | Path | Rows | With Salary |
|----|------|------|-------------|
| Claude | `~/Documents/Claude Code/ai_analyst_roles_2026/db/job_postings_gold.db` | 300 | 45 (15%) |
| Codex | `~/Documents/Codex/artifacts/ai_analyst_roles_2026/db/job_postings_gold.db` | 167 | 82 (49%) |

## Dependencies

- Python 3.8+
- sqlite3 (stdlib)
- plotly (for dashboard generation only)
- Optional: `FIRECRAWL_API_KEY` for Tier 3 stealth scraping
- Optional: `TAVILY_API_KEY` for search-based collection

## License

Private research project.

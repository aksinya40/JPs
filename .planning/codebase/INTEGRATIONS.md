# External Integrations

**Analysis Date:** 2026-03-15

## APIs & External Services

**ATS (Applicant Tracking System) Public APIs - No Auth Required:**

- **Greenhouse Boards API** - Fetch job listings and enrichment data
  - Collection endpoint: `https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true`
  - Enrichment endpoint: `https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{jid}?pay_transparency=true`
  - Auth: None (public API)
  - Client: `urllib.request` (stdlib)
  - Used in: `pipeline/ingest.py` (`_fetch_greenhouse()`), `pipeline/enrichment.py` (Tier 1A)
  - Rate limiting: 0.5s delay between requests (`time.sleep(0.5)`)
  - Data extracted: title, location, pay ranges (cents), work mode, posted date, job content HTML

- **Lever Public API** - Fetch job listings and enrichment data
  - Collection endpoint: `https://api.lever.co/v0/postings/{slug}?mode=json`
  - Enrichment endpoint: `https://api.lever.co/v0/postings/{slug}/{posting_id}?mode=json`
  - Auth: None (public API)
  - Client: `urllib.request` (stdlib)
  - Used in: `pipeline/ingest.py` (`_fetch_lever()`), `pipeline/enrichment.py` (Tier 1C)
  - Rate limiting: 0.5s delay between requests
  - Data extracted: title, location, salary range, work mode, creation timestamp

- **Ashby Posting API** - Enrichment only (no collection)
  - Endpoint: `https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true`
  - Auth: None (public API)
  - Client: `urllib.request` (stdlib)
  - Used in: `pipeline/enrichment.py` (Tier 1B)
  - Rate limiting: 0.5s delay between requests
  - Data extracted: location, workplace type, compensation tier summary

**Web Scraping - JSON-LD Extraction:**

- **Generic job page scraping** - Tier 2A enrichment
  - Fetches raw HTML from `job_url` for any platform
  - Parses `<script type="application/ld+json">` blocks for `JobPosting` schema.org data
  - Client: `urllib.request` with browser-like User-Agent headers
  - Used in: `pipeline/enrichment.py` (Tier 2A, batched to 100 URLs per run)
  - Rate limiting: 1.5s delay between requests
  - Data extracted: location (city/state/country), work mode, posted date, salary (baseSalary), description

**LinkedIn Job Collection (via JobSpy):**

- **LinkedIn** - Job search and scraping via python-jobspy library
  - SDK: `python-jobspy` >= 1.1.0 (wraps LinkedIn job search)
  - Auth: Optional `PROXY_URL` env var for residential proxy (recommended to avoid rate limiting)
  - Used in: `scripts/collect_jobs.py` (standalone script, not part of main pipeline)
  - Rate limiting: 3s base + 5s extra every 5th query
  - Data extracted: title, company, location, description (HTML), salary, job type, remote flag

**Planned but NOT Implemented:**

- **Firecrawl API** - Referenced in `.env.example` as `FIRECRAWL_API_KEY` for "Tier 3 stealth scraping"
  - No code in the codebase actually uses this key
  - Auth: `FIRECRAWL_API_KEY` env var

- **Tavily API** - Referenced in `.env.example` as `TAVILY_API_KEY` for "search-based collection"
  - No code in the codebase actually uses this key
  - Auth: `TAVILY_API_KEY` env var

## Data Storage

**Databases:**
- SQLite 3 (stdlib) - Single local database file
  - Connection: Hardcoded path `<project_root>/db/job_postings_gold.db` in `pipeline/db.py`
  - Client: `sqlite3` stdlib module, no ORM
  - WAL mode enabled, foreign keys enforced
  - Row factory: `sqlite3.Row` for dict-like access
  - Tables:
    - `job_postings_gold` - Core table, 40+ columns (see `pipeline/db.py` lines 77-119)
    - `companies_200` - 200 target companies with ATS info (lines 62-75)
    - `raw_postings` - Staging table for newly collected jobs (lines 121-134)
    - `qa_violations` - Quality gate violations (lines 136-143)
    - `source_attempts` - HTTP request tracking (lines 145-153)
    - `approval_state` - QA approval gate (lines 155-162)
    - `scrape_runs` - Collection run tracking (lines 164-175)

- SQLite source databases (read-only, for initial merge):
  - `~/Documents/Claude Code/ai_analyst_roles_2026/db/job_postings_gold.db` (Claude DB)
  - `~/Documents/Codex/artifacts/ai_analyst_roles_2026/db/job_postings_gold.db` (Codex DB)

**File Storage:**
- Local filesystem only
  - `review/` - CSV exports, QA reports, health check JSON (committed to git)
  - `output/` - Dashboard HTML, research report markdown (gitignored)
  - `db/` - SQLite database (gitignored)

**Caching:**
- None - no caching layer
- Enrichment results stored directly in database columns (`enrich_status`, `url_http_status`, `url_checked_at`)
- Dedup via `canonical_job_key` (SHA-256 hash) prevents re-processing

## Authentication & Identity

**Auth Provider:**
- None - no user authentication
- This is a single-user local research pipeline
- "Approval" is a data quality gate (`approval_state` table), not user auth

## Monitoring & Observability

**Error Tracking:**
- None (no Sentry, no error reporting service)

**Logs:**
- `pipeline/db.py` `log()` function: `print(f"[{timestamp}] {msg}")`
- All output goes to stdout
- No structured logging, no log levels, no log files
- Health check reports saved as timestamped JSON: `review/health_YYYY-MM-DD_HHMMSS.json`

## CI/CD & Deployment

**Hosting:**
- Not deployed - runs locally on macOS
- Output is static files viewed in browser (dashboard.html) or text editor (report.md)

**CI Pipeline:**
- None detected (no `.github/workflows/`, no `.gitlab-ci.yml`, no `Jenkinsfile`)

## Environment Configuration

**Required env vars:**
- None for core pipeline operation (all public APIs, no auth needed)

**Optional env vars:**
- `FIRECRAWL_API_KEY` - For planned Tier 3 stealth scraping (not implemented in code)
- `TAVILY_API_KEY` - For planned search-based collection (not implemented in code)
- `PROXY_URL` - For LinkedIn collection via JobSpy (`scripts/collect_jobs.py`), format: `http://user:pass@host:port`

**Secrets location:**
- `.env` file in project root (gitignored)
- `.env.example` committed as template

## Webhooks & Callbacks

**Incoming:**
- None

**Outgoing:**
- None

## URL Freshness Checking

- `pipeline/enrichment.py` `cmd_check_freshness()` - HEAD requests to job URLs
  - Checks up to 200 URLs per run (oldest-checked first)
  - Only re-checks URLs not checked in 3+ days
  - Marks 404/410 responses as `status='Closed'`
  - Rate limited: 0.3s between requests
  - Uses `urllib.request` with macOS User-Agent

## Integration Architecture Summary

The pipeline uses a tiered enrichment strategy, all via stdlib `urllib.request`:

1. **Tier 1A**: Greenhouse API (structured JSON, pay transparency)
2. **Tier 1B**: Ashby API (structured JSON, compensation)
3. **Tier 1C**: Lever API (structured JSON, salary range)
4. **Tier 2A**: JSON-LD extraction from raw HTML (any platform)
5. **Tier 2B**: Salary regex mining from stored descriptions (no HTTP)
6. **Tier 3**: Firecrawl stealth scraping (planned, not implemented)

All external HTTP calls include polite rate limiting (0.3-1.5s delays) and browser-like User-Agent headers. No API keys are required for the implemented tiers.

---

*Integration audit: 2026-03-15*

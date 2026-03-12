# JPs Research Plan: AI Signal Taxonomy + LinkedIn Scraping
**Date:** 2026-03-09  
**Status:** Ready for implementation  
**Scope:** Research only — no code implementation here

---

# PART 1: AI Signal in `title_normalized`

## Background

The current dataset (351 rows, 43 columns) has two problems with AI-in-title representation:

1. **Signal loss**: 51 rows have `has_ai_in_title=1` but their `title_normalized` contains no AI terminology — the normalization step strips AI context
2. **Signal gap**: 12 rows ("Applied Scientist (Contract), Artificial General Intelligence") have `has_ai_in_title=0` despite "Artificial General Intelligence" appearing literally in the title — the current regex misses it

### Current `ai_role_signature` distribution (351 rows):
| Value | Count | Meaning |
|---|---|---|
| `ai_in_description_only` | 292 | AI only in job description, not title |
| `ai_team_or_platform_in_title` | 46 | AI is the department/team/platform |
| `ai_in_title` | 9 | AI IS the job (e.g. "AI Analyst") |
| `llm_or_genai_in_scope` | 2 | LLM/GenAI in scope/responsibilities |
| `emerging_ai_named_role` | 1 | Novel AI role name |
| `ai_team_or_platform_in_scope` | 1 | AI team mentioned in scope only |

---

## The Core Distinction

There are **two types** of AI signal in a job title, and they get different treatment in `title_normalized`:

### Type A — AI IS the Role Identity
The job itself is an AI role. AI stays IN `title_normalized`, stripped down to its base form.

**Rule**: Remove ALL qualifiers — team names, specialties, seniority — keep only `"AI [base function]"`.

| Raw title | → `title_normalized` | Reasoning |
|---|---|---|
| `Senior AI Red Team Analyst` | `AI Analyst` | "Senior" = seniority; "Red Team" = team spec |
| `AI Red Team Analyst` | `AI Analyst` | "Red Team" = team specification, not the role |
| `Applied AI Analyst` | `AI Analyst` | "Applied" = qualifier |
| `AI Analyst` | `AI Analyst` | Already base form |
| `Agentic AI Researcher` | `AI Researcher` | "Agentic" = qualifier |

**Key insight**: "Red Team", "Applied", "Agentic" etc. describe the *team* or *flavor* of the AI role — not the role function itself. The canonical role is just `AI Analyst` or `AI Researcher`.

### Type B — AI is Team / Department Context
The person is a standard analyst/scientist, but working ON an AI team. AI context goes INTO `title_normalized` as a suffix `(AI Team)`.

**Rule**: Normalize the base role as usual, then append `(AI Team)` suffix. All AI team variants — GenAI, Agentic, AGI, LLM, ML — collapse into the single tag `(AI Team)`.

| Raw title | → `title_normalized` | Reasoning |
|---|---|---|
| `Data Scientist, AGI` | `Data Scientist (AI Team)` | DS role on AGI team |
| `Applied Scientist (Contract), Artificial General Intelligence` | `Applied Scientist (AI Team)` | Applied Scientist on AGI team |
| `Research Scientist, GenAI` | `Research Scientist (AI Team)` | Researcher on GenAI team |
| `Lead Data Scientist - Agentic AI` | `Data Scientist (AI Team)` | DS role on Agentic team (seniority stripped per existing convention) |
| `Business Intelligence Analyst, Ads & GenAI Insights` | `Business Intelligence Analyst (AI Team)` | BI Analyst on GenAI team |
| `Senior Business Analyst - AI` | `Business Analyst (AI Team)` | BA supporting AI initiative |
| `Data Analyst - AI Products` | `Data Analyst (AI Team)` | DA on AI product team |

**Why collapse GenAI / AGI / Agentic / LLM all to `(AI Team)`?**  
From a job search perspective, a Data Scientist on the AGI team and one on the GenAI team are functionally equivalent — both are DS roles working in an AI context. The distinction between which AI subfield is domain detail, not a role-level distinction. Keeping them separate would fragment the taxonomy unnecessarily.

### No AI Signal
All other roles: `title_normalized` stays as the clean base role name — no suffix, no AI tag.

| Raw title | → `title_normalized` |
|---|---|
| `Senior Data Analyst` | `Data Analyst` |
| `Data Scientist` | `Data Scientist` |
| `Business Analyst II` | `Business Analyst` |

---

## No Separate `title_ai_context` Column Needed

The AI signal is now self-contained in `title_normalized`:
- Starts with `"AI "` → Type A (AI is the role)
- Ends with `" (AI Team)"` → Type B (AI is the team context)
- Neither → no AI signal

Any downstream filter, query, or model can derive this directly from `title_normalized` without a separate column. This keeps the schema simpler.

---

## Final `title_normalized` Taxonomy (AI-related entries)

| `title_normalized` | Type | Source raw titles |
|---|---|---|
| `AI Analyst` | A — AI identity | "AI Analyst", "AI Red Team Analyst", "Senior AI Red Team Analyst", "Applied AI Analyst" |
| `AI Researcher` | A — AI identity | "Agentic AI Researcher" |
| `Data Scientist (AI Team)` | B — AI team context | "Data Scientist, AGI", "Lead Data Scientist - Agentic AI", "Senior Data Scientist, GenAI" |
| `Applied Scientist (AI Team)` | B — AI team context | "Applied Scientist (Contract), Artificial General Intelligence" (12 rows) |
| `Research Scientist (AI Team)` | B — AI team context | "Research Scientist, GenAI" |
| `Business Intelligence Analyst (AI Team)` | B — AI team context | "BI Analyst, Ads & GenAI Insights" |
| `Business Analyst (AI Team)` | B — AI team context | "Senior Business Analyst - AI" |
| `Data Analyst (AI Team)` | B — AI team context | "Data Analyst - AI Products" |

All existing non-AI normalized titles remain unchanged.

---

## Fixes Required

### Fix 1: Verbatim copies (76 rows)
76 rows have `title_normalized` = exact copy of raw job title (including company suffixes, team names, contract notes, etc.).  
These need proper normalization per the rules above:
- Strip `(Contract)`, `(Temp)`, `(Remote)` suffixes
- Strip `, [Team Name]` department context → apply Type B rule
- Strip seniority (Senior, Lead, Principal, Staff, Junior)
- Strip level suffixes (I, II, III)

### Fix 2: Type A AI roles — reduce to base form (9 rows)
All rows with `ai_role_signature = ai_in_title` must have `title_normalized` set to `"AI [base function]"`, stripping any and all qualifiers.

Examples to fix:
- `"AI Red Team Analyst"` → `"AI Analyst"`
- `"Senior AI Red Team Analyst"` → `"AI Analyst"`
- `"Applied AI Analyst"` → `"AI Analyst"`
- `"Agentic AI Researcher"` → `"AI Researcher"`

### Fix 3: Type B roles — add `(AI Team)` suffix (46 rows)
All rows with `ai_role_signature = ai_team_or_platform_in_title` must have `title_normalized` updated to include `(AI Team)`:
- Strip the AI team context from the title
- Normalize the base role
- Append `(AI Team)`

### Fix 4: `has_ai_in_title` bug (12 rows)
The 12 rows with raw title `Applied Scientist (Contract), Artificial General Intelligence` all have `has_ai_in_title=0`.  
**Root cause**: Regex only matches `"AI"` (acronym) and short `ai_terms_list`, but not the full phrase `"Artificial General Intelligence"`.  
**Fix**: Add `"Artificial General Intelligence"` and `"AGI"` to the title AI term detection regex/list.  
**After fix**: These 12 rows get `has_ai_in_title=1` and `title_normalized = "Applied Scientist (AI Team)"`.

### Fix 5: `ai_signal_types` inconsistency (37 rows)
37 rows have `has_ai_in_title=1` but `ai_signal_types` doesn't include `"title"`.  
**Fix**: Recompute `ai_signal_types` from scratch based on where terms actually appear (title vs. description vs. scope), rather than relying on the current inconsistent flags.

### Fix 6: `ai_role_signature` update for AGI rows
The 12 AGI rows currently have wrong or missing `ai_role_signature`.  
**Fix**: Set `ai_role_signature = "ai_team_or_platform_in_title"` for all rows where raw title contains `"Artificial General Intelligence"` or `"AGI"` (as team context, not role identity).

---

## Out-of-Scope Roles to Remove

The audit found roles that are not AI Analyst jobs and should be excluded:
- HR roles: `HR Analyst`, `Human Resources Analyst`, `People Analytics Analyst`
- Finance roles: `Finance Analyst`, `Financial Analyst`
- ERP roles: `ERP Analyst`, `SAP Analyst`
- Non-tech Operations Analyst roles — review case-by-case by company

Check `role_cluster` values to identify these. Current fragmentation: 14 clusters → target 8 core clusters.

---

## Summary: Part 1 Implementation Checklist

1. Fix `has_ai_in_title` regex → add "Artificial General Intelligence" and "AGI" (fixes 12 rows, **do this first** as it affects downstream)
2. Recompute `ai_signal_types` from source fields (fixes 37-row inconsistency)
3. Fix 76 verbatim `title_normalized` entries → apply normalization rules
4. Fix 9 Type A rows → reduce to `"AI [base function]"` stripping all qualifiers
5. Fix 46 Type B rows → strip AI team context from title, normalize base role, append `(AI Team)`
6. Fix 12 AGI rows → `title_normalized = "Applied Scientist (AI Team)"`, update `ai_role_signature`
7. Review and remove out-of-scope roles (HR, Finance, ERP)

---

---

# PART 2: LinkedIn Scraping Strategy

## Problem Statement

Current dataset: ~300-370 job postings  
Target: 3,000-8,000 job postings  
Source: LinkedIn (largest job board, best structured data for AI/data analyst roles)  
Constraint: LinkedIn has no official public API; official API requires partner agreement

---

## How LinkedIn Job Data Works (Technical Internals)

### The Guest API (Unofficial but Working)
LinkedIn exposes two unauthenticated endpoints used by their own search experience:

**Endpoint 1 — Job Search (list):**
```
GET https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search
  ?keywords=data+analyst
  &location=United+States
  &geoId=103644278
  &trk=public_jobs_jobs-search-bar_search-submit
  &start=0
```
- Returns HTML (not JSON) with job cards
- Each card contains: job_id, title, company, location, posted_date (relative)
- Pagination: `start=0, 25, 50, ...` up to **975 max** (LinkedIn enforces a hard cap)
- This means max 1,000 results per search query

**Endpoint 2 — Job Detail:**
```
GET https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}
```
- Returns HTML with full job description
- Contains `<script type="application/ld+json">` with structured data:
  - `title`, `datePosted`, `employmentType`, `description` (full HTML)
  - `hiringOrganization.name`, `hiringOrganization.sameAs` (company LinkedIn URL)
  - `jobLocation.address` (city, state, country)
  - `baseSalary` (when available)
- This is the detail page that yields the richest data per job

### Rate Limits Observed in Practice
- **Search endpoint**: 2-5 requests/second tolerated; beyond that, temporary 429 errors
- **Detail endpoint**: 1-3 requests/second tolerated
- **Session duration**: After ~50-100 rapid requests, IP soft-blocked for 15-60 minutes
- **Datacenter IPs**: Immediately blocked (LinkedIn detects AWS/GCP/Azure ranges)
- **Residential IPs**: Work much better; rotate after every 25-50 requests

---

## Best Existing Implementation: JobSpy

**Repo**: `speedyapply/JobSpy` (formerly `cullenwatson/JobSpy`)  
**Stars**: 2,900+ (most actively maintained multi-board scraper)  
**URL**: https://github.com/speedyapply/JobSpy  

### Why JobSpy is the Right Base:
1. Uses LinkedIn guest API (no credentials, no Selenium required for basic use)
2. Multi-board: LinkedIn + Indeed + Glassdoor + ZipRecruiter in one call
3. Returns structured pandas DataFrame with consistent schema
4. Handles pagination automatically up to `results_wanted` parameter
5. Supports `linkedin_fetch_description=True` for full description pull
6. Active maintenance as of 2025-2026

### JobSpy Key Parameters:
```python
scrape_jobs(
    site_name=["linkedin"],
    search_term="data analyst",
    location="United States",
    results_wanted=100,           # per call
    hours_old=168,                # past 7 days
    country_indeed="USA",
    linkedin_fetch_description=True,  # hits detail endpoint too
    proxies=["user:pass@host:port"],  # residential proxy
)
```

### JobSpy Output Schema (relevant fields):
| Field | Source | Notes |
|---|---|---|
| `id` | List page | LinkedIn job ID |
| `title` | List page | Raw job title |
| `company` | List page | Company name |
| `location` | List page | City, State |
| `date_posted` | Detail page | ISO date |
| `job_type` | Detail page | Full-time, Contract, etc. |
| `description` | Detail page | Full HTML description |
| `job_url` | List page | LinkedIn job URL |
| `salary_source` | Detail page | From JSON-LD if available |
| `min_amount` / `max_amount` | Detail page | Parsed salary numbers |
| `is_remote` | List page | Boolean |

---

## The 1,000-Job Cap: How to Break It

LinkedIn returns max 1,000 results per search query (hard cap at start=975).  
To reach 5,000-8,000 unique postings, use **query decomposition**:

### Strategy: Multi-Dimensional Query Grid

**Dimension 1 — Keywords (8-10 variants):**
```
"data analyst"
"business analyst"  
"AI analyst"
"analytics analyst"
"business intelligence analyst"
"data science analyst"
"product analyst"
"operations analyst"
"insights analyst"
"quantitative analyst"
```

**Dimension 2 — Locations (4-6 regions):**
```
"United States" (nationwide)
"San Francisco Bay Area"
"New York City"
"Seattle, WA"
"Austin, TX"
"Chicago, IL"
```

**Dimension 3 — Work Type:**
```
on-site
remote
hybrid
```

**Dimension 4 — Time Windows:**
```
Past 24 hours
Past week (7 days)
Past month (30 days)
```

**Estimated yield:**
- 10 keywords × 6 locations = 60 query combinations
- Each returns up to 1,000 results → 60,000 theoretical maximum
- After deduplication (jobs appear in multiple queries): ~5,000-12,000 unique jobs
- After relevance filtering: ~2,000-5,000 target-relevant jobs

### Deduplication:
- Deduplicate by `job_id` (LinkedIn job ID is stable across queries)
- Run all queries first, collect all IDs, then fetch details only for unique IDs
- This dramatically reduces detail-endpoint calls and proxy usage

---

## Proxy Requirements

### Why Residential Proxies Are Mandatory:
- LinkedIn detects and blocks datacenter IPs (AWS, GCP, Azure, Digital Ocean) immediately
- Residential IPs appear as real user traffic
- LinkedIn's anti-bot detects request cadence, not just IP class

### Recommended Proxy Providers:
| Provider | Cost | Quality | Notes |
|---|---|---|---|
| **Oxylabs** | ~$8/GB | ★★★★★ | Best LinkedIn success rate; residential pool 100M+ IPs |
| **Bright Data** | ~$8.40/GB | ★★★★★ | Largest pool; has pre-built LinkedIn scraper |
| **ScraperAPI** | $49/month (250k req) | ★★★★☆ | `ultra_premium=True` for LinkedIn; easier setup |
| **Smartproxy** | ~$7/GB | ★★★★☆ | Good residential pool, US focus |
| **IPRoyal** | ~$3/GB | ★★★☆☆ | Budget option; less reliable for LinkedIn |

**Recommendation**: Start with ScraperAPI (easiest to integrate with JobSpy, flat fee pricing) or Oxylabs (best raw performance).

### Proxy Rotation Rules:
- Rotate IP after every 25-50 requests
- Never send more than 2 requests/second per IP
- Use US-geolocated residential IPs for US job searches
- Run searches during off-peak hours (UTC 02:00-08:00 = US night)

### Estimated Proxy Cost:
- Each search results page: ~5KB
- Each detail page: ~50KB
- 10,000 jobs × 50KB detail + 400 search pages × 5KB = ~502MB
- Cost: $502MB × $8/GB ≈ **$4 per full run**
- Monthly for weekly runs: ~$16-20/month

---

## Data Extraction: JSON-LD Structured Data

The detail endpoint (`/jobs-guest/jobs/api/jobPosting/{id}`) contains JSON-LD:

```html
<script type="application/ld+json">
{
  "@context": "http://schema.org",
  "@type": "JobPosting",
  "title": "Data Analyst",
  "datePosted": "2026-03-07",
  "employmentType": "FULL_TIME",
  "description": "<full HTML description>",
  "hiringOrganization": {
    "@type": "Organization",
    "name": "Acme Corp",
    "sameAs": "https://www.linkedin.com/company/acme-corp"
  },
  "jobLocation": {
    "@type": "Place",
    "address": {
      "addressLocality": "San Francisco",
      "addressRegion": "CA",
      "addressCountry": "US"
    }
  },
  "baseSalary": {
    "@type": "MonetaryAmount",
    "currency": "USD",
    "value": {
      "@type": "QuantitativeValue",
      "minValue": 95000,
      "maxValue": 140000,
      "unitText": "YEAR"
    }
  }
}
</script>
```

**Key fields to extract from JSON-LD:**
- `datePosted` → `posted_date` (exact date, not "3 days ago")
- `baseSalary.value.minValue/maxValue` → `salary_min`, `salary_max` (pre-parsed numbers)
- `employmentType` → `employment_type` (FULL_TIME, PART_TIME, CONTRACTOR)
- `jobLocation.address` → clean city/state/country
- `hiringOrganization.sameAs` → LinkedIn company URL (for company ID extraction)

This structured data is more reliable than parsing the HTML job description for salary/location.

---

## Getting Direct ATS Apply Links

LinkedIn job pages show an "Apply" button that redirects to the actual ATS (Greenhouse, Lever, Workday, etc.). Getting the direct ATS URL requires following the redirect.

### Option A: Simple redirect follow (fast)
```
GET https://www.linkedin.com/jobs/view/{job_id}/apply  
→ 302 redirect to https://boards.greenhouse.io/company/jobs/12345
```
This works for ~70% of jobs. Requires following HTTP redirects (requests library does this automatically).

### Option B: py-linkedin-jobs-scraper with `apply_link=True` (slow but thorough)
**Repo**: `joeyism/linkedin-scraper` or `joeyism/py-linkedin-jobs-scraper`  
Uses Selenium; launches a headless browser, clicks the Apply button, captures the redirect.  
Rate: ~2-3 seconds per job → 10,000 jobs = 6-8 hours  
Better success rate for JavaScript-redirect ATSes (Workday, Oracle, SAP SuccessFactors)

**Recommendation**: Use Option A first (fast). For jobs where redirect fails or returns a LinkedIn redirect loop, flag for Option B in a second pass.

---

## Legal & Risk Assessment

### CFAA (Computer Fraud and Abuse Act):
- **Risk: LOW** for public LinkedIn data
- **hiQ Labs v. LinkedIn** (9th Circuit, 2022): Court ruled scraping publicly accessible LinkedIn data does NOT violate CFAA
- LinkedIn's own public job search pages require no login → not "unauthorized access"
- Ruling: Public = no CFAA violation

### LinkedIn Terms of Service:
- **Risk: MEDIUM** — LinkedIn ToS explicitly prohibits automated scraping
- LinkedIn settled with hiQ in late 2022; hiQ shut down (not from CFAA but from business failure + ToS enforcement via Cease & Desist)
- LinkedIn can: block your IP, issue C&D to your company, pursue civil ToS violation claims
- **Real risk**: Getting blocked, not criminal prosecution

### Mitigation:
- Don't scrape logged-in content (use guest endpoints only)
- Don't store PII (recruiter names, personal emails)
- Scrape only job listings, not user profiles
- Use reasonable rate limits (don't hammer the server)
- The risk posture is: accepted by thousands of companies running similar scrapers

---

## Paid Data Alternatives (If Scraping Fails)

If LinkedIn aggressively blocks the scraper mid-project, these are fallback options:

| Provider | Data | Price | Notes |
|---|---|---|---|
| **Bright Data LinkedIn Dataset** | 882M+ LinkedIn records | $250 minimum | Pre-scraped, ready to download; most comprehensive |
| **Coresignal** | LinkedIn job postings | ~$500+/month | Structured, refreshed regularly |
| **RocketReach** | Job data + contacts | ~$300/month | Better for company/contact enrichment |
| **PDL (People Data Labs)** | Job postings API | ~$0.02/record | Pay-per-record; good for targeted pulls |
| **Theirstack** | Tech job postings | ~$200/month | Tech-focused; good for AI/data roles |

**Recommendation**: Start with JobSpy + proxies. If LinkedIn blocks consistently after 3+ attempts with different proxy providers, buy a one-time Bright Data dataset pull ($250) to seed the database, then use scraping for incremental updates.

---

## Other Job Boards to Supplement LinkedIn

Don't put all eggs in one basket. JobSpy supports multiple boards simultaneously:

| Board | Via JobSpy | Unique value |
|---|---|---|
| **Indeed** | ✅ Yes | Largest volume; good for company-specific roles |
| **Glassdoor** | ✅ Yes | Salary transparency data |
| **ZipRecruiter** | ✅ Yes | Broad coverage; good for mid-market companies |
| **Google Jobs** | ❌ No | Aggregates everything; scrape via SerpAPI ($50/month) |
| **Greenhouse.io** | Indirect | All Greenhouse-hosted jobs publicly accessible: `https://boards.greenhouse.io/{company}/jobs.json` |
| **Lever** | Indirect | All Lever-hosted jobs: `https://api.lever.co/v0/postings/{company}?mode=json` |
| **Workday** | Indirect | Company-specific URLs; harder to enumerate |

**Greenhouse and Lever** are goldmines: they expose ALL job postings as public JSON endpoints, no scraping needed, no IP blocks. Just need a list of target company slugs.

---

## Implementation Plan (For Coding Session)

### Phase 1: JobSpy Setup + Query Grid (Week 1)
1. Install JobSpy from `speedyapply/JobSpy`
2. Set up proxy connection (ScraperAPI recommended for simplicity)
3. Define keyword × location query grid (10 × 6 = 60 combinations)
4. Run all 60 queries, collect job IDs only (no detail pages yet)
5. Deduplicate by job_id → get unique ID list
6. Save raw list to staging table/CSV

**Expected output**: 5,000-15,000 unique job IDs

### Phase 2: Detail Page Enrichment (Week 1-2)
1. For each unique job_id, fetch detail page
2. Extract JSON-LD structured data (datePosted, salary, location, employmentType)
3. Extract full description HTML
4. Try redirect follow for apply_link (Option A)
5. Store in staging DB

**Rate**: ~3 req/sec with proxy rotation = ~1 hour for 10,000 jobs

### Phase 3: Relevance Filtering (Week 2)
1. Apply existing relevance filters from current pipeline (has_python, has_sql, role_cluster logic)
2. Filter out non-US, non-analyst roles
3. Apply AI signal normalization from Part 1 (title_normalized with AI Team suffix where applicable)
4. Target: 2,000-5,000 high-quality analyst job postings

### Phase 4: Incremental Updates (Ongoing)
1. Run scraper weekly with `hours_old=168` (past 7 days)
2. Skip job_ids already in DB
3. Close/expire jobs that disappear from listings
4. Monitor for LinkedIn blocking patterns

---

## Summary: Part 2 Key Decisions

| Decision | Recommendation | Reason |
|---|---|---|
| Primary scraper | JobSpy (speedyapply fork) | Most maintained; no Selenium needed; multi-board |
| Auth | None (guest endpoints only) | Avoids login detection; legally cleaner |
| Proxy | ScraperAPI or Oxylabs residential | Mandatory; datacenter IPs instant-blocked |
| Rotation | Every 25-50 requests | Empirically tested threshold |
| Rate | 2-3 req/sec max | Safe for guest endpoints |
| Cap bypass | Query decomposition (keyword × location × time) | Only viable method within LinkedIn limits |
| ATS links | HTTP redirect follow first; Selenium fallback | Speed vs. completeness tradeoff |
| Legal risk | Accepted (CFAA-safe; ToS risk managed) | Standard industry practice |
| Paid fallback | Bright Data dataset ($250) | Fastest path to bulk data if needed |
| Supplement | Indeed + Glassdoor + Greenhouse.io + Lever | Diversify away from LinkedIn dependency |

---

## Combined Deliverable Summary

### Part 1: Changes to DB Schema
- **No new columns** — AI signal encoded directly in `title_normalized`
- **Modified column**: `title_normalized` — fix 76 verbatim entries; fix 9 Type A AI roles → `"AI [function]"`; fix 46 Type B AI team roles → `"[Role] (AI Team)"`; fix 12 AGI rows
- **Modified column**: `has_ai_in_title` — fix regex to catch "Artificial General Intelligence" / "AGI"
- **Modified column**: `ai_signal_types` — recompute from source fields
- **Modified column**: `ai_role_signature` — update 12 AGI rows

### Part 2: New Data Pipeline
- **New script**: `linkedin_scraper.py` — JobSpy wrapper with query grid + deduplication
- **New script**: `detail_enricher.py` — JSON-LD extraction from detail pages
- **New script**: `apply_link_resolver.py` — HTTP redirect follower for ATS URLs
- **Updated pipeline**: Integrate new jobs into existing `job_postings_gold.db` with dedup logic
- **New table** (optional): `scrape_runs` — log each scraper run (timestamp, query, results, errors)

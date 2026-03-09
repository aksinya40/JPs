# Data Quality Fix Recommendations — JPs Unified DB
**Analyzed:** 2026-03-09 | **DB:** `/Users/aksiniya/Documents/JPs/db/job_postings_gold.db` | **Total rows:** 370

---

## Summary Table

| # | Issue | Rows Affected | Priority |
|---|-------|--------------|----------|
| 1 | Non-US jobs leaking through | 7 | 🔴 CRITICAL |
| 2 | `company_name = "Unknown"` | 17 | 🔴 CRITICAL |
| 3 | `title_normalized` not normalized | 310/370 distinct (84%) | 🔴 CRITICAL |
| 4 | `description_snippet` contains raw HTML | 107 (29%) | 🟠 HIGH |
| 5 | `work_mode` contradicts location | 11 | 🟠 HIGH |
| 6 | `posted_date` missing / `date_uncertain=1` | 286 (77%) | 🟠 HIGH |
| 7 | `skills_extracted` — noise ("R", "analysis") | ~120+ | 🟠 HIGH |
| 8 | Aggregator URLs (not company ATS) | ~20 | 🟠 HIGH |
| 9 | `Applied Scientist` inclusion decision needed | 59 (16%) | 🟡 MEDIUM |
| 10 | `salary_text` corruption (value appended) | 2 | 🟡 MEDIUM |
| 11 | `company_id = 0` for known companies | 87 rows / 40 companies | 🟡 MEDIUM |

---

## 🔴 CRITICAL Issues

---

### Issue 1 — Non-US Jobs Leaking Through (7 rows)

**Root cause:** Greenhouse API returns the *board-level* default location (often set to "United States" by the company) instead of the *job-level* location. The pipeline trusts `location_raw` from the API response without cross-checking against the actual country stored in the DB.

**What we found:**
```
FR (France) | is_us=0 | location_raw="United States" → 2 rows
BR (Brazil)  | is_us=0 | location_raw="United States" → 1 row
CA (Canada)  | is_us=0 | location_raw="United States" → 1 row
IN (India)   | is_us=0 | location_raw="United States" → 1 row
IE (Ireland) | is_us=0 | location_raw="San Francisco, CA" → 1 row
Unknown      | is_us=0 | location_raw="Costa Rica - Remote" → 1 row
Unknown      | is_us=0 | location_raw="United States" → 1 row
```

**Fix:**
1. **Filter gate at ingest**: After Greenhouse API call, cross-check `offices[].location` field (not just `location`). If it contains country names that aren't US, reject the row.
2. **Hard filter in gold promotion**: `WHERE is_us = 1 AND country IN ('US', 'United States', NULL)` — never promote rows with non-US country codes.
3. **QA CRITICAL rule**: Add check `SELECT COUNT(*) FROM job_postings_gold WHERE is_us=0` — must be 0.
4. **For existing 7 rows**: `DELETE FROM job_postings_gold WHERE is_us = 0;`

---

### Issue 2 — `company_name = "Unknown"` (17 rows)

**Root cause:** Pipeline doesn't extract company name from the job URL slug when the ATS response doesn't include it. All 17 URLs have a clear company identifier in the path.

**What we found (URLs → correct company name):**
```
boards.greenhouse.io/mirakllabs/...  → Mirakl
boards.greenhouse.io/braze/...       → Braze
boards.greenhouse.io/nanonets/...    → Nanonets
boards.greenhouse.io/cloudflare/...  → Cloudflare (×2)
boards.greenhouse.io/fanduel/...     → FanDuel
jobs.lever.co/modulate/...           → Modulate
jobs.lever.co/netomi/...             → Netomi
jobs.lever.co/aircall/...            → Aircall
jobs.lever.co/cscgeneration-2/...    → CSC Generation
jobs.ashbyhq.com/trm-labs/...        → TRM Labs
jobs.cvshealth.com/...               → CVS Health
eicl.fa.em5.oraclecloud.com/...      → Oracle (EICL entity)
linkedin.com/jobs/view/.../at-evolutioniq-... → EvolutionIQ
wellfound.com/jobs/3826517-...       → extractable from title/description
www.wallstreetcareers.com/...        → needs description lookup
www.builtinnyc.com/...               → needs description lookup
```

**Fix:**
```python
import re

ATS_SLUG_PATTERNS = [
    (r'boards\.greenhouse\.io/([^/]+)/', lambda m: m.group(1).replace('-', ' ').title()),
    (r'jobs\.lever\.co/([^/]+)/', lambda m: m.group(1).replace('-', ' ').title()),
    (r'jobs\.ashbyhq\.com/([^/]+)/', lambda m: m.group(1).replace('-', ' ').title()),
    (r'jobs\.([^.]+)\.com/', lambda m: m.group(1).title()),  # e.g. jobs.cvshealth.com
]

def extract_company_from_url(url):
    for pattern, formatter in ATS_SLUG_PATTERNS:
        m = re.search(pattern, url)
        if m:
            return formatter(m)
    return None

# Then: if company_name == 'Unknown', try extract_company_from_url(job_url)
```

Also: add to pipeline as a post-processing `normalize_company_name` step that runs before gold promotion.

---

### Issue 3 — `title_normalized` Not Normalized (310 distinct values out of 370 rows = 84%)

**Root cause:** Pipeline copies `title` verbatim into `title_normalized`. No canonicalization logic was implemented.

**Current state:** 310 unique "normalized" titles. Examples of what should be the same bucket:
```
"Senior Data Scientist NLP/GenAI - Catalog"           → Data Scientist
"Staff Data Scientist, Marketing"                      → Data Scientist
"Data Scientist, Algorithms, Optimization - Fulfillment" → Data Scientist
"Applied Scientist (Contract), Artificial General..."  → Applied Scientist
"Data Operations Analyst"                              → Operations Analyst
"Sr. Language Data Scientist, Alexa AI"               → Data Scientist
```

**Fix — 15-segment canonical taxonomy:**
```python
TITLE_SEGMENTS = {
    # (regex pattern on lowercased title) → canonical segment
    r'\bapplied scientist\b': 'Applied Scientist',
    r'\bstaff data scientist\b': 'Staff Data Scientist',
    r'\bsenior data scientist\b|sr\.?\s+data scientist': 'Senior Data Scientist',
    r'\bprincipal data scientist\b': 'Principal Data Scientist',
    r'\blead data scientist\b': 'Lead Data Scientist',
    r'\bdata scientist\b': 'Data Scientist',           # must be after sr/staff/lead
    r'\banalytics engineer\b': 'Analytics Engineer',
    r'\bdata analyst\b': 'Data Analyst',
    r'\bproduct analyst\b': 'Product Analyst',
    r'\bbusiness analyst\b': 'Business Analyst',
    r'\boperations analyst\b|ops analyst': 'Operations Analyst',
    r'\bgrowth analyst\b': 'Growth Analyst',
    r'\bmarketing analyst\b': 'Marketing Analyst',
    r'\bdata science manager\b|manager.*data science': 'Data Science Manager',
    r'\bdirector.*data|data.*director\b': 'Director, Data',
}

def normalize_title(raw_title: str) -> str:
    t = raw_title.lower()
    for pattern, canonical in TITLE_SEGMENTS.items():
        if re.search(pattern, t):
            return canonical
    return raw_title  # fallback: keep raw if no match
```

**Expected result after fix:** ~15 distinct values covering 95%+ of rows. The remaining ~5% (rare titles) can stay as raw fallback.

---

## 🟠 HIGH Issues

---

### Issue 4 — `description_snippet` Contains Raw HTML (107 rows = 29%)

**Root cause:** The pipeline stores the raw HTML response from the ATS API directly into `description_snippet` without stripping tags. Greenhouse in particular returns HTML-formatted descriptions.

**Examples seen:** `<div class="content-intro"><div><strong>About Us</strong>...`

**Fix:**
```python
import re
from html.parser import HTMLParser

def strip_html(text: str) -> str:
    if not text:
        return text
    # Remove tags
    clean = re.sub(r'<[^>]+>', ' ', text)
    # Collapse whitespace
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

# Apply: description_snippet = strip_html(raw_description)[:500]
```

**Also fix:** description_snippet should be max 300-500 chars of *clean* text. Current snippets include job titles prepended (e.g., `"Senior Data Scientist <div..."`) — strip the title prefix too.

---

### Issue 5 — `work_mode` Contradicts Location (11 rows)

**Root cause:** `work_mode` is set to "On-site" as default, but the actual location contains "Remote". No reconciliation logic between `location_raw`/`location_standardized` and `work_mode`.

**Found:** 11 rows where `work_mode='On-site'` AND location contains "Remote".

**Fix — work_mode resolution priority:**
```python
REMOTE_PATTERNS = [
    r'\bremote\b', r'\bwork from home\b', r'\bwfh\b', r'\bfully remote\b'
]
HYBRID_PATTERNS = [
    r'\bhybrid\b', r'\bflexible\b', r'\b\d+\s*days.*office\b'
]

def resolve_work_mode(ats_work_mode, location_raw, location_standardized):
    # Priority 1: ATS-provided (Lever has best workplaceType field)
    if ats_work_mode and ats_work_mode.lower() not in ('unknown', ''):
        return ats_work_mode
    # Priority 2: Infer from location text
    loc = f"{location_raw or ''} {location_standardized or ''}".lower()
    if any(re.search(p, loc) for p in REMOTE_PATTERNS):
        return 'Remote'
    if any(re.search(p, loc) for p in HYBRID_PATTERNS):
        return 'Hybrid'
    return 'On-site'  # fallback only if truly no signal
```

---

### Issue 6 — `posted_date` Missing for 286 Rows (77%) / `date_uncertain=1`

**Root cause:** ATS APIs return different date field names. Pipeline isn't consistently extracting from all sources. Greenhouse uses `updated_at` and `first_published`. Lever uses `createdAt`. Ashby uses `publishedDate`.

**Fix — exhaustive date field mapping per ATS:**
```python
DATE_FIELD_MAP = {
    'greenhouse': ['first_published', 'published_at', 'updated_at'],
    'ashby':      ['publishedDate', 'updatedAt', 'createdAt'],
    'lever':      ['createdAt', 'updatedAt'],
    'workday':    ['postedDate', 'closingDate'],
    'smartrecruiters': ['releasedDate', 'updatedOn'],
}

def extract_date(api_response: dict, ats: str) -> tuple[str, bool]:
    """Returns (date_str_or_None, is_uncertain)"""
    for field in DATE_FIELD_MAP.get(ats, []):
        val = api_response.get(field)
        if val:
            return (parse_iso(val), False)
    # Fallback: check JSON-LD datePosted
    jld = api_response.get('json_ld', {})
    if jld.get('datePosted'):
        return (jld['datePosted'], False)
    return (None, True)
```

**For existing 286 rows:** Re-enrich via `re_enrich` command, which will re-hit ATS APIs and try all date fields.

---

### Issue 7 — `skills_extracted` Noise (120+ rows with junk)

**Root cause:** Regex skill extraction treats "R" as a match for the R programming language, but "R" appears everywhere in English text (e.g., "...R&D team...", "...greater..."). Same for "analysis" being extracted as a skill name.

**Scale of problem:**
- `skills_extracted = "R"` alone: **75 rows** (20% of all rows!)
- `skills_extracted = "analysis"`: 21 rows
- `skills_extracted = "R, Excel"`: 32 rows (R is spurious)
- `skills_extracted = "R, AWS"`: 4 rows

**Fix:**
```python
# Use word-boundary regex for single-letter skills
SKILL_PATTERNS = {
    'Python':    r'\bpython\b',
    'SQL':       r'\bsql\b',
    'R':         r'\bR\b(?!\s*&|\w)',  # NOT "R&D", "R-squared" as sentence context
    'Scala':     r'\bscala\b',
    'Spark':     r'\bspark\b',
    'dbt':       r'\bdbt\b',
    'Airflow':   r'\bairflow\b',
    'Tableau':   r'\btableau\b',
    'Looker':    r'\blooker\b',
    'Snowflake': r'\bsnowflake\b',
    'AWS':       r'\baws\b|\bamazon web services\b',
    'GCP':       r'\bgcp\b|\bgoogle cloud\b',
    'Azure':     r'\bazure\b',
    'Excel':     r'\bexcel\b',
    'Git':       r'\bgit\b(?!hub)',
    'GitHub':    r'\bgithub\b',
    # Remove "analysis" — not a skill, it's a job function
}

# Minimum: R requires 2+ other skills to be included (too noisy alone)
def validate_skills(skills_list):
    if skills_list == ['R']:
        return []  # R alone is noise
    return skills_list
```

---

### Issue 8 — Aggregator URLs (Not Company ATS Pages) (~20 rows)

**Root cause:** Pipeline accepted job listings from aggregator sites (Built In, The Ladders, The Muse, Towards AI, Wellfound, wallstreetcareers.com) which are not primary sources. These can't be enriched via ATS APIs, often have outdated data, and company_name is unknowable from URL alone.

**Aggregator domains found:**
```
builtin.com, builtinnyc.com, builtinsf.com  → 5 rows
theladders.com                               → 1 row
themuse.com                                  → 1 row
towardsai.net                               → 1 row
wallstreetcareers.com                        → 1 row
wellfound.com                               → 1 row
```
(LinkedIn ~9 rows is a special case — has guest API, keep but flag separately)

**Fix:**
```python
BLOCKED_DOMAINS = {
    'builtin.com', 'builtinnyc.com', 'builtinsf.com',
    'theladders.com', 'themuse.com', 'towardsai.net',
    'wallstreetcareers.com', 'datasciencessjobs.com',
    'technyjobs.com',  # add as found
}

def is_aggregator_url(url: str) -> bool:
    from urllib.parse import urlparse
    domain = urlparse(url).netloc.lstrip('www.')
    return domain in BLOCKED_DOMAINS
```
- Add `is_aggregator_url()` check at **ingest time** — reject before storing
- For existing rows: `DELETE FROM job_postings_gold WHERE job_url LIKE '%builtin%' OR job_url LIKE '%theladders%' OR job_url LIKE '%themuse%' OR job_url LIKE '%towardsai.net%' OR job_url LIKE '%wallstreetcareers%'`

---

## 🟡 MEDIUM Issues

---

### Issue 9 — `Applied Scientist` Classification Decision (59 rows = 16%)

**Current state:** 59 rows with "Applied Scientist" in title, all classified as `role_cluster = "Data Scientist"`. Most are from Amazon.

**The question:** Should Applied Scientists be in scope?

**Recommendation:**
- ✅ **Keep** Applied Scientists who work on *analytics/measurement* (e.g., "Applied Scientist - Analytics AI", "Applied Scientist - Ads Measurement")
- ❌ **Remove** Applied Scientists who are effectively ML Engineers (e.g., "Applied Scientist - LLM Code Agents", "Applied Scientist, AWS Agentic AI")
- Add `role_cluster = "Applied Scientist"` as a distinct cluster (not merged into Data Scientist)
- Add QA warning if Applied Scientist rows exceed 20% of total (too many = scope drift)

**Triage rule:**
```python
APPLIED_SCIENTIST_KEEP_KEYWORDS = [
    'analytics', 'measurement', 'insights', 'ads science',
    'experimentation', 'causal', 'decision'
]
APPLIED_SCIENTIST_REMOVE_KEYWORDS = [
    'llm agent', 'code agent', 'foundation model', 'pretraining',
    'robotics', 'computer vision', 'speech'
]
```

---

### Issue 10 — `salary_text` Corruption (2 rows)

**Root cause:** String concatenation bug — `has_ai_in_title` integer value (1) is appended to `salary_text` string during data merge step.

**Example:** `"$152290 - $2502001"` where the last digit `1` = `has_ai_in_title`.

**Fix in pipeline merge code:**
```python
# Bug: salary_text = row['salary_text'] + str(row['has_ai_in_title'])  ← wrong
# Fix: ensure these are always separate column assignments, never concatenated
# Also add QA check:
# SELECT COUNT(*) FROM job_postings_gold
# WHERE salary_max_usd > 0 AND salary_text NOT LIKE '%' || CAST(salary_max_usd AS TEXT) || '%'
```

**For 2 existing rows:** Clean manually:
```sql
UPDATE job_postings_gold
SET salary_text = '$152,290 - $250,200'
WHERE salary_text = '$152290 - $2502001';
```

---

### Issue 11 — `company_id = 0` for Known Companies (87 rows / 40 companies)

**Root cause:** `companies_200` table has 200 target companies, but 40 real companies in the DB aren't in that list (e.g., Smartsheet, Reddit, Okta, Twilio, Gusto, CrowdStrike, Cloudflare, Oracle).

**Fix options:**
1. **Expand companies_200** to companies_300+ — add the 40 missing companies
2. **Assign negative IDs** for "real but not in target list" companies (`company_id = -1`)
3. **Add `in_target_list` boolean column** — track separately from company_id

**Recommendation:** Option 3 — cleanest design:
```sql
ALTER TABLE job_postings_gold ADD COLUMN in_target_list INTEGER DEFAULT 0;
UPDATE job_postings_gold SET in_target_list = 1 WHERE company_id > 0;
-- Then dashboard can filter to target 200 vs. show all 300+
```

---

## Implementation Order

Run fixes in this exact order to avoid cascading bugs:

```
Step 1: DELETE non-US rows (7)                    → Fixes is_us integrity
Step 2: DELETE aggregator URLs (~10)              → Reduces noise before fixes
Step 3: Fix company_name from URL slug (17)       → Needed before title_normalized
Step 4: Fix description_snippet HTML stripping (107) → Needed before skills re-extract
Step 5: Re-extract skills_extracted with new regex (all)  → Cleaner skills
Step 6: Build title_normalized taxonomy (370)     → Needs clean titles
Step 7: Fix work_mode / location contradiction (11) → Post-clean
Step 8: Fix salary_text corruption (2)            → Small, safe
Step 9: Re-enrich dates (286)                     → Requires ATS re-call
Step 10: Decide Applied Scientist scope (59)      → Requires manual review
Step 11: Add in_target_list column (87)           → Schema addition
```

---

## New QA CRITICAL Rules to Add

Add these to the `qa_check` command in the pipeline:

```python
QA_CRITICAL = [
    # Existing
    ("Non-US rows", "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=0"),
    ("Unknown company", "SELECT COUNT(*) FROM job_postings_gold WHERE company_name='Unknown'"),
    # New
    ("HTML in snippet", "SELECT COUNT(*) FROM job_postings_gold WHERE description_snippet LIKE '%<%'"),
    ("Aggregator URLs", "SELECT COUNT(*) FROM job_postings_gold WHERE job_url LIKE '%builtin%' OR job_url LIKE '%theladders%' OR job_url LIKE '%wallstreetcareers%'"),
    ("work_mode contradiction", "SELECT COUNT(*) FROM job_postings_gold WHERE work_mode='On-site' AND (location_standardized LIKE '%Remote%' OR location_raw LIKE '%Remote%')"),
]

QA_WARNINGS = [
    ("skills = R only", "SELECT COUNT(*) FROM job_postings_gold WHERE skills_extracted='R'"),
    ("title_normalized = title", "SELECT COUNT(*) FROM job_postings_gold WHERE title_normalized=title"),
    ("date_uncertain > 70%", "SELECT CAST(SUM(date_uncertain) AS FLOAT)/COUNT(*) FROM job_postings_gold"),
    ("Applied Scientist > 20%", "SELECT CAST(COUNT(*) AS FLOAT)/(SELECT COUNT(*) FROM job_postings_gold) FROM job_postings_gold WHERE title LIKE '%Applied Scientist%'"),
]
```

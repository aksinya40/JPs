"""
AI Analyst Jobs — Data Ingestion
===================================
Merge source databases, collect ATS postings, parse locations,
and promote raw_postings → job_postings_gold.

Commands: cmd_merge_dbs, cmd_collect_ats, cmd_ingest_raw
"""
import json
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Dict, List, Tuple

from pipeline.constants import HYBRID_PATTERNS, REMOTE_PATTERNS
from pipeline.db import (
    CLAUDE_DB,
    CODEX_DB,
    cmd_init_db,
    get_db,
    log,
    row_to_dict,
)
from pipeline.filters import (
    classify_ai_role_signature,
    is_role_excluded,
    match_ai_keywords,
)
from pipeline.parsers import (
    canonical_job_key,
    canonicalize_platform,
    compute_title_ai_terms,
    extract_company_from_url,
    extract_salary,
    extract_skills,
    normalize_company,
    normalize_text,
    normalize_title_to_segment,
    normalize_url,
    strip_html,
    window_bucket,
)


# ═══════════════════════════════════════════════════════════════════════════════
# PRE-INGEST VALIDATION — gate at entry, reject bad rows before INSERT
# ═══════════════════════════════════════════════════════════════════════════════

_INGEST_REQUIRED = ['title', 'job_url', 'source_job_id', 'company_name']
_INGEST_ENUM = {
    'status': {'Open', 'Closed'},
    'work_mode': {'On-site', 'Remote', 'Hybrid', 'Unknown'},
}


def validate_pre_ingest(row: dict) -> list:
    """Validate a row dict before inserting into job_postings_gold.

    Returns list of error strings. Empty list = row is valid.
    Called before every INSERT to prevent bad data from entering gold table.
    """
    errors = []

    # Required fields
    for f in _INGEST_REQUIRED:
        if not row.get(f) or str(row[f]).strip() == '':
            errors.append(f'{f} missing')

    # URL format
    url = row.get('job_url') or ''
    if url and not url.startswith('http'):
        errors.append('job_url not a valid URL')

    # Salary sanity
    sal_min = row.get('salary_min_usd')
    if sal_min and sal_min > 0:
        if sal_min < 10000:
            errors.append(f'salary_min_usd={sal_min} too low')

    sal_max = row.get('salary_max_usd')
    if sal_min and sal_max and sal_min > sal_max:
        errors.append(f'salary_min_usd={sal_min} > salary_max_usd={sal_max}')

    # Enum validation
    for col, allowed in _INGEST_ENUM.items():
        val = row.get(col)
        if val and val not in allowed:
            errors.append(f'{col}={val} not in allowed set')

    return errors


# ═══════════════════════════════════════════════════════════════════════════════
# Location-parsing constants
# ═══════════════════════════════════════════════════════════════════════════════

_US_STATES = {
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga',
    'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md',
    'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj',
    'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc',
    'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy', 'dc',
}
_US_STATE_NAMES = {
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado',
    'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho',
    'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana',
    'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota',
    'mississippi', 'missouri', 'montana', 'nebraska', 'nevada',
    'new hampshire', 'new jersey', 'new mexico', 'new york',
    'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon',
    'pennsylvania', 'rhode island', 'south carolina', 'south dakota',
    'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington',
    'west virginia', 'wisconsin', 'wyoming', 'district of columbia',
}
_US_CITIES = {
    'new york', 'los angeles', 'chicago', 'houston', 'phoenix', 'san antonio',
    'san diego', 'dallas', 'san jose', 'austin', 'san francisco', 'seattle',
    'denver', 'boston', 'nashville', 'portland', 'las vegas', 'atlanta',
    'miami', 'minneapolis', 'raleigh', 'charlotte', 'pittsburgh',
    'salt lake city', 'washington', 'philadelphia', 'detroit', 'columbus',
    'indianapolis', 'memphis', 'milwaukee', 'baltimore', 'tampa',
    'st. louis', 'sacramento', 'kansas city', 'cincinnati', 'cleveland',
    'orlando', 'newark', 'palo alto', 'mountain view', 'menlo park',
    'sunnyvale', 'cupertino', 'redmond', 'bellevue', 'cambridge',
    'boulder', 'ann arbor', 'santa clara', 'irvine', 'plano',
}
_NON_US_MARKERS = {
    # Cities
    'london', 'toronto', 'vancouver', 'berlin', 'munich', 'paris',
    'amsterdam', 'dublin', 'bangalore', 'hyderabad', 'singapore',
    'sydney', 'melbourne', 'tokyo', 'tel aviv', 'zurich', 'geneva',
    'stockholm', 'copenhagen', 'manila', 'calgary', 'montreal',
    'mumbai', 'pune', 'chennai', 'delhi', 'noida', 'gurgaon',
    'sao paulo', 'buenos aires', 'bogota', 'lima', 'santiago',
    # Countries
    'uk', 'united kingdom', 'canada', 'germany', 'france', 'india',
    'australia', 'japan', 'israel', 'ireland', 'netherlands',
    'switzerland', 'sweden', 'denmark', 'brazil', 'mexico', 'china',
    'south korea', 'poland', 'spain', 'italy', 'austria', 'belgium',
    'czech republic', 'romania', 'portugal', 'finland', 'norway',
    'new zealand', 'philippines', 'indonesia', 'malaysia', 'thailand',
    'vietnam', 'taiwan', 'hong kong', 'colombia', 'argentina',
    'chile', 'peru', 'nigeria', 'kenya', 'south africa', 'egypt',
    'turkey', 'saudi arabia', 'uae', 'qatar', 'pakistan', 'bangladesh',
    'sri lanka', 'ukraine', 'hungary', 'greece', 'croatia', 'serbia',
    'bulgaria', 'slovakia', 'lithuania', 'latvia', 'estonia',
    'luxembourg', 'iceland', 'costa rica',
    # Provinces / regions
    'ontario', 'british columbia', 'quebec', 'alberta',
    'banten',
}
_STATE_ABBR_TO_NAME = {
    'al': 'Alabama', 'ak': 'Alaska', 'az': 'Arizona', 'ar': 'Arkansas',
    'ca': 'California', 'co': 'Colorado', 'ct': 'Connecticut',
    'de': 'Delaware', 'fl': 'Florida', 'ga': 'Georgia', 'hi': 'Hawaii',
    'id': 'Idaho', 'il': 'Illinois', 'in': 'Indiana', 'ia': 'Iowa',
    'ks': 'Kansas', 'ky': 'Kentucky', 'la': 'Louisiana', 'me': 'Maine',
    'md': 'Maryland', 'ma': 'Massachusetts', 'mi': 'Michigan',
    'mn': 'Minnesota', 'ms': 'Mississippi', 'mo': 'Missouri',
    'mt': 'Montana', 'ne': 'Nebraska', 'nv': 'Nevada', 'nh': 'New Hampshire',
    'nj': 'New Jersey', 'nm': 'New Mexico', 'ny': 'New York',
    'nc': 'North Carolina', 'nd': 'North Dakota', 'oh': 'Ohio',
    'ok': 'Oklahoma', 'or': 'Oregon', 'pa': 'Pennsylvania',
    'ri': 'Rhode Island', 'sc': 'South Carolina', 'sd': 'South Dakota',
    'tn': 'Tennessee', 'tx': 'Texas', 'ut': 'Utah', 'vt': 'Vermont',
    'va': 'Virginia', 'wa': 'Washington', 'wv': 'West Virginia',
    'wi': 'Wisconsin', 'wy': 'Wyoming', 'dc': 'District of Columbia',
}

_CITY_TO_STATE = {
    'San Francisco': 'California', 'Los Angeles': 'California',
    'San Jose': 'California', 'San Diego': 'California',
    'Palo Alto': 'California', 'Mountain View': 'California',
    'Menlo Park': 'California', 'Sunnyvale': 'California',
    'Cupertino': 'California', 'Santa Clara': 'California',
    'Irvine': 'California', 'Sacramento': 'California',
    'New York': 'New York', 'Boston': 'Massachusetts',
    'Cambridge': 'Massachusetts', 'Seattle': 'Washington',
    'Bellevue': 'Washington', 'Redmond': 'Washington',
    'Chicago': 'Illinois', 'Austin': 'Texas', 'Dallas': 'Texas',
    'Houston': 'Texas', 'San Antonio': 'Texas', 'Plano': 'Texas',
    'Denver': 'Colorado', 'Boulder': 'Colorado',
    'Portland': 'Oregon', 'Atlanta': 'Georgia',
    'Miami': 'Florida', 'Tampa': 'Florida', 'Orlando': 'Florida',
    'Nashville': 'Tennessee', 'Minneapolis': 'Minnesota',
    'Salt Lake City': 'Utah', 'Phoenix': 'Arizona',
    'Las Vegas': 'Nevada', 'Raleigh': 'North Carolina',
    'Charlotte': 'North Carolina', 'Pittsburgh': 'Pennsylvania',
    'Philadelphia': 'Pennsylvania', 'Detroit': 'Michigan',
    'Ann Arbor': 'Michigan', 'Columbus': 'Ohio',
    'Cleveland': 'Ohio', 'Cincinnati': 'Ohio',
    'Indianapolis': 'Indiana', 'Milwaukee': 'Wisconsin',
    'Baltimore': 'Maryland', 'Newark': 'New Jersey',
    'Kansas City': 'Missouri', 'St. Louis': 'Missouri',
    'Memphis': 'Tennessee', 'Washington': 'District of Columbia',
}


# ═══════════════════════════════════════════════════════════════════════════════
# Location / seniority helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_location(location_raw: str) -> Tuple[bool, str, str, str, str]:
    """Parse raw location → (is_us, city, state, standardized, work_mode)."""
    if not location_raw:
        return True, '', '', '', 'Unknown'

    loc = location_raw.lower().strip()

    # Detect work mode
    work_mode = 'On-site'
    if any(re.search(p, loc) for p in REMOTE_PATTERNS):
        work_mode = 'Remote'
    elif any(re.search(p, loc) for p in HYBRID_PATTERNS):
        work_mode = 'Hybrid'

    # Check for non-US markers first
    for marker in _NON_US_MARKERS:
        if marker in loc:
            return False, '', '', location_raw, work_mode

    # Strategy 1: State abbreviation "City, CA"
    state_match = re.search(r',\s*([A-Z]{2})\b', location_raw)
    if state_match:
        abbr = state_match.group(1).lower()
        if abbr in _US_STATES:
            city_part = location_raw[:state_match.start()].strip().rstrip(',')
            state_name = _STATE_ABBR_TO_NAME.get(abbr, abbr.upper())
            standardized = f"{city_part}, {state_name}" if city_part else state_name
            return True, city_part, state_name, standardized, work_mode

    # Strategy 2: Full state name "San Francisco, California[, US]"
    parts = re.split(r'[;,]', loc)
    parts_stripped = [p.strip() for p in parts]
    found_state = ''
    found_city = ''
    for part in parts_stripped:
        clean = re.sub(r'\(.*?\)', '', part).strip()
        if clean in _US_STATE_NAMES:
            found_state = clean.title()
        elif clean in _US_CITIES:
            found_city = clean.title()

    if found_state:
        if not found_city:
            for part in parts_stripped:
                clean = re.sub(r'\(.*?\)', '', part).strip()
                if clean not in _US_STATE_NAMES and clean not in (
                    'united states', 'usa', 'us', 'remote', 'hybrid',
                    'north america',
                ):
                    found_city = clean.title()
                    break
        standardized = f"{found_city}, {found_state}" if found_city else found_state
        return True, found_city, found_state, standardized, work_mode

    # Strategy 3: Known US city without state
    if found_city:
        inferred = _CITY_TO_STATE.get(found_city, '')
        if inferred:
            standardized = f"{found_city}, {inferred}"
            return True, found_city, inferred, standardized, work_mode
        return True, found_city, '', found_city, work_mode

    # Strategy 4: "United States" / "US" / "USA" only
    if re.search(r'\bunited states\b|\busa\b|\bus\b', loc):
        if work_mode == 'Remote':
            return True, '', '', 'Remote', work_mode
        return True, '', 'US', 'United States', work_mode

    # Strategy 5: Remote with no non-US marker → assume US
    if work_mode == 'Remote':
        return True, '', '', 'Remote', work_mode

    # Default: assume US
    return True, '', '', location_raw, work_mode


def _detect_seniority(title: str) -> str:
    """Detect seniority level from job title."""
    if not title:
        return 'Mid'
    t = title.lower()
    if re.search(r'\b(vp|vice president)\b', t):
        return 'VP'
    if re.search(r'\bdirector\b', t):
        return 'Director'
    if re.search(r'\bprincipal\b', t):
        return 'Principal'
    if re.search(r'\bstaff\b', t):
        return 'Staff'
    if re.search(r'\blead\b', t):
        return 'Lead'
    if re.search(r'\bsenior\b|\bsr\.?\b', t):
        return 'Senior'
    if re.search(r'\bjunior\b|\bjr\.?\b|\bentry[- ]level\b|\bassociate\b', t):
        return 'Junior'
    if re.search(r'\bmanager\b|\bhead of\b', t):
        return 'Manager'
    return 'Mid'


# ═══════════════════════════════════════════════════════════════════════════════
# ATS fetch helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_greenhouse(slug: str) -> List[Dict]:
    """Fetch jobs from Greenhouse public boards API (free, no auth)."""
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode('utf-8'))
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        raise RuntimeError(f"Greenhouse API error for {slug}: {e}")

    jobs = []
    for j in data.get('jobs', []):
        location = j.get('location', {}).get('name', '')
        posted = ''
        if j.get('updated_at'):
            posted = j['updated_at'][:10]
        content_html = j.get('content', '')
        body = strip_html(content_html)
        salary_info = extract_salary(body) or extract_salary(j.get('title', ''))
        salary_text = ''
        if salary_info and not salary_info.get('skip'):
            salary_text = salary_info.get('salary_text', '')
        jobs.append({
            'id': str(j.get('id', '')),
            'title': j.get('title', ''),
            'url': j.get('absolute_url', ''),
            'location': location,
            'body': body,
            'posted_date': posted,
            'salary_text': salary_text,
        })
    return jobs


def _fetch_lever(slug: str) -> List[Dict]:
    """Fetch postings from Lever public API (free, no auth)."""
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode('utf-8'))
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        raise RuntimeError(f"Lever API error for {slug}: {e}")

    if not isinstance(data, list):
        return []

    jobs = []
    for j in data:
        location = j.get('categories', {}).get('location', '')
        posted = ''
        if j.get('createdAt'):
            try:
                ts = int(j['createdAt']) / 1000
                posted = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
            except (ValueError, TypeError, OSError):
                pass
        desc_html = (j.get('description', '') or '') + '\n' + (j.get('additional', '') or '')
        body = strip_html(desc_html)
        salary_info = extract_salary(body) or extract_salary(j.get('text', ''))
        salary_text = ''
        if salary_info and not salary_info.get('skip'):
            salary_text = salary_info.get('salary_text', '')
        jobs.append({
            'id': str(j.get('id', '')),
            'title': j.get('text', ''),
            'url': j.get('hostedUrl', ''),
            'location': location,
            'body': body,
            'posted_date': posted,
            'salary_text': salary_text,
        })
    return jobs


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: merge_dbs
# ═══════════════════════════════════════════════════════════════════════════════

def _merge_codex_into_existing(cur, gold_id: int, codex_row):
    """Merge Codex data into an existing Claude row — prefer best per field."""
    existing = cur.execute(
        "SELECT * FROM job_postings_gold WHERE gold_id = ?", (gold_id,)
    ).fetchone()
    if not existing:
        return
    existing = row_to_dict(existing)
    codex_row = row_to_dict(codex_row) if not isinstance(codex_row, dict) else codex_row

    updates = {}

    # Prefer Codex salary if Claude has NULL
    if not existing['salary_min_usd'] and codex_row.get('salary_min_usd'):
        updates['salary_min_usd'] = codex_row['salary_min_usd']
        updates['salary_max_usd'] = codex_row.get('salary_max_usd')
        updates['salary_currency'] = 'USD'
        updates['salary_period'] = 'Annual'

    # Fill in Codex-specific fields if missing
    if not existing.get('ai_role_signature') and codex_row.get('ai_role_signature'):
        updates['ai_role_signature'] = codex_row['ai_role_signature']
    if not existing.get('has_python') and codex_row.get('has_python'):
        updates['has_python'] = codex_row['has_python']
    if not existing.get('has_sql') and codex_row.get('has_sql'):
        updates['has_sql'] = codex_row['has_sql']
    if not existing.get('location_city') and codex_row.get('location_city'):
        updates['location_city'] = codex_row['location_city']
    if not existing.get('location_state') and codex_row.get('location_state'):
        updates['location_state'] = codex_row['location_state']
    if not existing.get('description_snippet') and codex_row.get('description_snippet'):
        updates['description_snippet'] = codex_row['description_snippet']
    if not existing.get('verified_date') and codex_row.get('verified_date'):
        updates['verified_date'] = codex_row['verified_date']

    if updates:
        set_clause = ', '.join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [gold_id]
        cur.execute(f"UPDATE job_postings_gold SET {set_clause} WHERE gold_id = ?", vals)


def cmd_merge_dbs():
    """Merge Claude DB + Codex DB into unified schema with dedup."""
    if not CLAUDE_DB.exists():
        log(f"ERROR: Claude DB not found at {CLAUDE_DB}")
        sys.exit(1)
    if not CODEX_DB.exists():
        log(f"ERROR: Codex DB not found at {CODEX_DB}")
        sys.exit(1)

    cmd_init_db()
    conn = get_db()
    cur = conn.cursor()

    # Disable FK constraints during merge
    conn.execute("PRAGMA foreign_keys=OFF")

    seen_urls = {}      # normalized_url → gold_id
    seen_combos = {}    # (norm_company, norm_title, date) → gold_id

    # ── Phase 1: Import Claude DB ──
    log("merge_dbs: Importing Claude DB...")
    cdb = sqlite3.connect(str(CLAUDE_DB))
    cdb.row_factory = sqlite3.Row
    claude_rows = [row_to_dict(r) for r in cdb.execute(
        "SELECT * FROM job_postings_gold"
    ).fetchall()]
    log(f"  Claude DB: {len(claude_rows)} rows")

    imported_claude = 0
    for r in claude_rows:
        norm_url = normalize_url(r['job_url'])
        platform = canonicalize_platform(r['source_platform'])
        cjk = r['canonical_job_key'] or canonical_job_key(
            platform, r['source_job_id'], r['job_url'], r.get('company_id', 0) or 0
        )
        has_ai, t_ai_terms = compute_title_ai_terms(
            r.get('title_normalized', r.get('title', ''))
        )
        wb = r.get('window_bucket', '') or window_bucket(r.get('posted_date', ''))

        try:
            cur.execute("""
                INSERT OR IGNORE INTO job_postings_gold (
                    canonical_job_key, company_id, company_name, source_platform,
                    source_job_id, job_url, url_http_status, url_checked_at,
                    title, title_normalized, role_cluster, seniority,
                    posted_date, date_uncertain, window_bucket,
                    location_raw, location_standardized, country, is_us,
                    work_mode, status, ai_signal_types, ai_keywords_hit,
                    skills_extracted, salary_currency, salary_min_usd, salary_max_usd,
                    salary_period, salary_text, has_ai_in_title, title_ai_terms,
                    enrich_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                cjk, r.get('company_id'), r['company_name'], platform,
                r['source_job_id'], r['job_url'], r.get('url_http_status'),
                r.get('url_checked_at'),
                r.get('title', r.get('title_normalized', '')),
                r.get('title_normalized', ''), r.get('role_cluster', ''),
                r.get('seniority', 'Mid'),
                r.get('posted_date'), r.get('date_uncertain', 0), wb,
                r.get('location_raw', ''), r.get('location_standardized'),
                r.get('country', 'US'), r.get('is_us', 1),
                r.get('work_mode', 'Unknown'), r.get('status', 'Open'),
                r.get('ai_signal_types', ''), r.get('ai_keywords_hit', ''),
                r.get('skills_extracted', ''), r.get('salary_currency'),
                r.get('salary_min_usd'), r.get('salary_max_usd'),
                r.get('salary_period'), r.get('salary_text'),
                r.get('has_ai_in_title', has_ai),
                r.get('title_ai_terms', t_ai_terms),
                r.get('enrich_status', 'pending'),
                r.get('created_at', datetime.now().isoformat()),
            ))
            if cur.rowcount > 0:
                imported_claude += 1
                gid = cur.lastrowid
                seen_urls[norm_url] = gid
                combo = (normalize_company(r['company_name']),
                         normalize_text(r.get('title_normalized', '')),
                         (r.get('posted_date', '') or '')[:10])
                seen_combos[combo] = gid
        except Exception as e:
            log(f"  WARN: Claude row skip: {e}")

    conn.commit()
    log(f"  Imported {imported_claude} rows from Claude DB")

    # ── Phase 2: Import Codex DB with dedup ──
    log("merge_dbs: Importing Codex DB with dedup...")
    xdb = sqlite3.connect(str(CODEX_DB))
    xdb.row_factory = sqlite3.Row
    codex_rows = [row_to_dict(r) for r in xdb.execute(
        "SELECT * FROM job_postings_gold"
    ).fetchall()]
    log(f"  Codex DB: {len(codex_rows)} rows")

    imported_codex = 0
    merged_codex = 0
    for r in codex_rows:
        norm_url = normalize_url(r['job_url'])
        platform = canonicalize_platform(r.get('data_source', ''))

        # PRIMARY dedup: URL match
        if norm_url in seen_urls:
            gid = seen_urls[norm_url]
            _merge_codex_into_existing(cur, gid, r)
            merged_codex += 1
            continue

        # SECONDARY dedup: company+title+date
        combo = (normalize_company(r['company_name']),
                 normalize_text(r.get('job_title', '')),
                 (r.get('date_posted', '') or '')[:10])
        if combo in seen_combos:
            gid = seen_combos[combo]
            _merge_codex_into_existing(cur, gid, r)
            merged_codex += 1
            continue

        # New unique row from Codex
        cjk = r.get('canonical_job_key') or canonical_job_key(
            platform, r.get('source_job_id', ''), r['job_url'], 0
        )
        has_ai, t_ai_terms = compute_title_ai_terms(r.get('job_title', ''))
        wb = window_bucket(r.get('date_posted', ''))

        try:
            cur.execute("""
                INSERT OR IGNORE INTO job_postings_gold (
                    canonical_job_key, company_name, source_platform,
                    source_job_id, job_url, url_http_status,
                    title, title_normalized, role_cluster, seniority,
                    posted_date, date_uncertain, window_bucket,
                    location_city, location_state, country, is_us,
                    work_mode, status, ai_keywords_hit,
                    ai_role_signature, skills_extracted,
                    has_python, has_sql,
                    salary_min_usd, salary_max_usd,
                    description_snippet, has_ai_in_title, title_ai_terms,
                    enrich_status, verified_date, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                cjk, r['company_name'], platform,
                r.get('source_job_id', ''), r['job_url'],
                r.get('url_http_status'),
                r.get('job_title', ''), normalize_text(r.get('job_title', '')),
                r.get('title_cluster', ''), r.get('seniority', 'Mid'),
                r.get('date_posted'), r.get('date_uncertain', 0), wb,
                r.get('location_city', ''), r.get('location_state', ''),
                'US', 1,
                r.get('remote_type', 'Unknown'),
                r.get('status_observed', 'Open'),
                r.get('ai_llm_keywords_found', ''),
                r.get('ai_role_signature'),
                r.get('key_technical_skills', ''),
                r.get('has_python', 0), r.get('has_sql', 0),
                r.get('salary_min_usd'), r.get('salary_max_usd'),
                r.get('description_snippet', ''),
                has_ai, t_ai_terms,
                'pending', r.get('verified_date'),
                r.get('created_at', datetime.now().isoformat()),
            ))
            if cur.rowcount > 0:
                imported_codex += 1
                gid = cur.lastrowid
                seen_urls[norm_url] = gid
                seen_combos[combo] = gid
        except Exception as e:
            log(f"  WARN: Codex row skip: {e}")

    conn.commit()

    # ── Phase 3: Import companies_200 from Claude DB ──
    log("merge_dbs: Importing companies_200...")
    comp_count = cur.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
    if comp_count == 0:
        claude_companies = [row_to_dict(c) for c in cdb.execute(
            "SELECT * FROM companies_200"
        ).fetchall()]
        for c in claude_companies:
            cur.execute("""
                INSERT OR IGNORE INTO companies_200
                (company_name, canonical_name, tier, sector, hq_country,
                 ats_platform, career_page_url, in_scope)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                c['company_name'], c['canonical_name'], c['tier'],
                c['sector'], c.get('hq_country', 'US'),
                c.get('ats_platform'), c.get('career_page_url'),
                c.get('in_scope', 1),
            ))
        conn.commit()
        final_comp = cur.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
        log(f"  Imported {final_comp} companies from Claude DB")
    else:
        log(f"  companies_200 already has {comp_count} rows, skipping import")

    # ── Summary ──
    total = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    active = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]
    with_sal = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE salary_min_usd IS NOT NULL"
    ).fetchone()[0]

    conn.execute("PRAGMA foreign_keys=ON")
    conn.commit()

    cdb.close()
    xdb.close()
    conn.close()

    log(f"merge_dbs COMPLETE:")
    log(f"  Claude imported: {imported_claude}")
    log(f"  Codex new:       {imported_codex}")
    log(f"  Codex merged:    {merged_codex}")
    log(f"  Total rows:      {total}")
    log(f"  Active US:       {active}")
    log(f"  With salary:     {with_sal}")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: collect_ats
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_collect_ats():
    """Collect job postings from Greenhouse & Lever for companies with board slugs."""
    conn = get_db()
    cur = conn.cursor()

    companies = cur.execute("""
        SELECT company_id, company_name, ats_platform, ats_board_slug
        FROM companies_200
        WHERE ats_board_slug IS NOT NULL AND ats_board_slug != ''
          AND in_scope = 1
    """).fetchall()

    if not companies:
        log("collect_ats: No companies with ats_board_slug found. Run init_db first.")
        conn.close()
        return

    log(f"collect_ats: Found {len(companies)} companies with board slugs")
    total_found = 0
    total_inserted = 0
    total_errors = 0

    for row in companies:
        c = row_to_dict(row)
        slug = c['ats_board_slug']
        platform = (c['ats_platform'] or '').lower()
        company_name = c['company_name']

        cur.execute("""
            INSERT INTO scrape_runs (source, company_slug, company_name)
            VALUES (?, ?, ?)
        """, (platform, slug, company_name))
        run_id = cur.lastrowid
        conn.commit()

        try:
            if 'greenhouse' in platform:
                jobs = _fetch_greenhouse(slug)
                source_platform = 'Greenhouse'
            elif 'lever' in platform:
                jobs = _fetch_lever(slug)
                source_platform = 'Lever'
            else:
                log(f"  SKIP {company_name}: unknown ATS platform '{platform}'")
                continue

            found = len(jobs)
            inserted = 0
            deduped = 0

            for job in jobs:
                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO raw_postings
                        (company_name, source_platform, source_job_id, job_url,
                         title, location_raw, body_raw, posted_date, salary_text,
                         scrape_run_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        company_name, source_platform,
                        str(job.get('id', '')),
                        job.get('url', ''),
                        job.get('title', ''),
                        job.get('location', ''),
                        job.get('body', ''),
                        job.get('posted_date', ''),
                        job.get('salary_text', ''),
                        run_id,
                    ))
                    if cur.rowcount > 0:
                        inserted += 1
                    else:
                        deduped += 1
                except sqlite3.Error as e:
                    log(f"    DB error for job {job.get('id')}: {e}")

            conn.commit()
            total_found += found
            total_inserted += inserted

            cur.execute("""
                UPDATE scrape_runs
                SET rows_found = ?, rows_inserted = ?, rows_deduped = ?,
                    finished_at = datetime('now')
                WHERE run_id = ?
            """, (found, inserted, deduped, run_id))
            conn.commit()

            log(f"  {company_name} ({slug}): {found} jobs, {inserted} new, {deduped} dupes")
            time.sleep(0.3)

        except Exception as e:
            total_errors += 1
            error_msg = str(e)[:500]
            cur.execute("""
                UPDATE scrape_runs
                SET errors = ?, finished_at = datetime('now')
                WHERE run_id = ?
            """, (error_msg, run_id))
            conn.commit()
            log(f"  ERROR {company_name} ({slug}): {e}")

    conn.close()
    log(f"collect_ats COMPLETE:")
    log(f"  Companies processed: {len(companies)}")
    log(f"  Total jobs found:    {total_found}")
    log(f"  Total inserted:      {total_inserted}")
    log(f"  Errors:              {total_errors}")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: ingest_raw
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_ingest_raw():
    """Promote raw_postings (processed=0) → job_postings_gold with dedup + filtering."""
    conn = get_db()
    cur = conn.cursor()

    rows = cur.execute("SELECT * FROM raw_postings WHERE processed = 0").fetchall()

    if not rows:
        log("ingest_raw: No pending raw_postings (processed=0). Nothing to do.")
        conn.close()
        return

    log(f"ingest_raw: Processing {len(rows)} pending raw postings")

    # Pre-load company lookup: normalized name → company_id
    companies = cur.execute("""
        SELECT company_id, company_name, canonical_name
        FROM companies_200 WHERE in_scope = 1
    """).fetchall()
    company_lookup = {}
    for c in companies:
        cd = row_to_dict(c)
        company_lookup[normalize_company(cd['company_name'])] = cd['company_id']
        company_lookup[normalize_company(cd['canonical_name'])] = cd['company_id']

    # Pre-load existing gold for dedup
    existing_gold_urls = set()
    for r in cur.execute("SELECT job_url FROM job_postings_gold").fetchall():
        existing_gold_urls.add(normalize_url(r[0]))
    existing_keys = {r[0] for r in cur.execute(
        "SELECT canonical_job_key FROM job_postings_gold"
    ).fetchall()}

    stats = {'inserted': 0, 'excluded_role': 0, 'non_us': 0,
             'no_ai_signal': 0, 'dedup': 0, 'rejected': 0, 'error': 0}

    for raw in rows:
        rd = row_to_dict(raw)
        raw_id = rd['raw_id']
        title = rd.get('title') or ''
        body = rd.get('body_raw') or ''
        location = rd.get('location_raw') or ''
        company = rd.get('company_name') or ''
        job_url = rd.get('job_url') or ''
        platform = canonicalize_platform(rd.get('source_platform') or '')
        source_job_id = rd.get('source_job_id') or ''
        posted_date = rd.get('posted_date') or ''
        salary_text = rd.get('salary_text') or ''

        # Filter 1: Excluded roles
        if is_role_excluded(title):
            cur.execute("UPDATE raw_postings SET processed = 2 WHERE raw_id = ?", (raw_id,))
            stats['excluded_role'] += 1
            continue

        # Filter 2: Non-US
        is_us, loc_city, loc_state, loc_std, work_mode = _parse_location(location)
        if not is_us:
            cur.execute("UPDATE raw_postings SET processed = 3 WHERE raw_id = ?", (raw_id,))
            stats['non_us'] += 1
            continue

        # Filter 3: AI signal check
        combined_text = f"{title} {body}"
        ai_hits = match_ai_keywords(combined_text)
        if not ai_hits:
            cur.execute("UPDATE raw_postings SET processed = 4 WHERE raw_id = ?", (raw_id,))
            stats['no_ai_signal'] += 1
            continue

        # Company matching
        company_id = company_lookup.get(normalize_company(company))
        if not company_id:
            url_company = extract_company_from_url(job_url)
            if url_company:
                company_id = company_lookup.get(normalize_company(url_company))

        # Dedup: URL check
        norm_url = normalize_url(job_url)
        if norm_url and norm_url in existing_gold_urls:
            cur.execute("UPDATE raw_postings SET processed = 1 WHERE raw_id = ?", (raw_id,))
            stats['dedup'] += 1
            continue

        # Dedup: canonical_job_key
        cjk = canonical_job_key(platform, source_job_id, job_url, company_id or 0)
        if cjk in existing_keys:
            cur.execute("UPDATE raw_postings SET processed = 1 WHERE raw_id = ?", (raw_id,))
            stats['dedup'] += 1
            continue

        # Compute derived fields
        has_ai, title_ai_terms_val = compute_title_ai_terms(title)
        ai_sig = classify_ai_role_signature(title, body)
        skills_str, has_python, has_sql = extract_skills(f"{title} {body}")
        title_norm = normalize_title_to_segment(title)
        wb = window_bucket(posted_date)
        seniority = _detect_seniority(title)

        # Salary
        sal = extract_salary(salary_text) or extract_salary(body)
        sal_min = sal.get('salary_min_usd') if sal and not sal.get('skip') else None
        sal_max = sal.get('salary_max_usd') if sal and not sal.get('skip') else None
        sal_period = sal.get('salary_period') if sal and not sal.get('skip') else None
        sal_currency = sal.get('salary_currency') if sal and not sal.get('skip') else None
        sal_text_final = sal.get('salary_text', salary_text) if sal and not sal.get('skip') else salary_text

        desc_snippet = body[:500] if body else ''
        ai_signal_types = ', '.join(ai_hits[:5])

        # ── Pre-ingest validation gate ──
        pre_ingest_row = {
            'title': title, 'job_url': job_url,
            'source_job_id': source_job_id, 'company_name': company,
            'status': 'Open', 'work_mode': work_mode,
            'salary_min_usd': sal_min, 'salary_max_usd': sal_max,
        }
        validation_errors = validate_pre_ingest(pre_ingest_row)
        if validation_errors:
            log(f"  REJECT raw_id {raw_id}: {'; '.join(validation_errors)}")
            cur.execute("UPDATE raw_postings SET processed = 5 WHERE raw_id = ?", (raw_id,))
            stats['rejected'] += 1
            continue

        try:
            cur.execute("""
                INSERT OR IGNORE INTO job_postings_gold (
                    canonical_job_key, company_id, company_name, source_platform,
                    source_job_id, job_url, title, title_normalized, role_cluster,
                    seniority, posted_date, window_bucket, location_raw,
                    location_city, location_state, location_standardized,
                    country, is_us, work_mode, status,
                    ai_signal_types, ai_keywords_hit, ai_role_signature,
                    skills_extracted, has_python, has_sql,
                    salary_currency, salary_min_usd, salary_max_usd,
                    salary_period, salary_text, has_ai_in_title, title_ai_terms,
                    description_snippet, enrich_status
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, (
                cjk, company_id, company, platform,
                source_job_id, job_url, title, title_norm, title_norm,
                seniority, posted_date, wb, location,
                loc_city, loc_state, loc_std,
                'US', 1, work_mode, 'Open',
                ai_signal_types, ', '.join(ai_hits), ai_sig,
                skills_str, has_python, has_sql,
                sal_currency, sal_min, sal_max,
                sal_period, sal_text_final, has_ai, title_ai_terms_val,
                desc_snippet, 'enriched',
            ))

            if cur.rowcount > 0:
                stats['inserted'] += 1
                existing_keys.add(cjk)
                if norm_url:
                    existing_gold_urls.add(norm_url)
            else:
                stats['dedup'] += 1

            cur.execute("UPDATE raw_postings SET processed = 1 WHERE raw_id = ?", (raw_id,))

        except sqlite3.Error as e:
            log(f"  ERROR inserting raw_id {raw_id}: {e}")
            stats['error'] += 1

    conn.commit()
    conn.close()

    log(f"ingest_raw COMPLETE:")
    log(f"  Pending processed:   {len(rows)}")
    log(f"  Inserted to gold:    {stats['inserted']}")
    log(f"  Deduped:             {stats['dedup']}")
    log(f"  Excluded (role):     {stats['excluded_role']}")
    log(f"  Excluded (non-US):   {stats['non_us']}")
    log(f"  Excluded (no AI):    {stats['no_ai_signal']}")
    log(f"  Rejected (invalid):  {stats['rejected']}")
    log(f"  Errors:              {stats['error']}")

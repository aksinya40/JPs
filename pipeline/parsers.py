"""
AI Analyst Jobs — Pure Text Parsers
=====================================
All normalize/extract/strip functions. These are pure functions with
no database access — they transform text → structured data.
"""
import hashlib
import re
from datetime import datetime, date
from typing import Optional, Dict, List, Tuple
from urllib.parse import urlparse

from pipeline.constants import (
    AI_KEYWORDS,
    TITLE_AI_TERMS,
    SALARY_PATTERNS,
    NON_USD_PATTERNS,
    SKILL_PATTERNS,
    TITLE_SEGMENTS,
    ATS_SLUG_PATTERNS,
    BLOCKED_DOMAINS,
    PLATFORM_CANONICAL,
)


def normalize_text(text: str) -> str:
    """Lowercase, strip, collapse whitespace."""
    if not text:
        return ''
    return re.sub(r'\s+', ' ', text.strip().lower())


def normalize_company(name: str) -> str:
    """Normalize company name for dedup matching."""
    if not name:
        return ''
    n = name.lower().strip()
    # Remove common suffixes
    for suffix in [', inc.', ', inc', ' inc.', ' inc', ', llc', ' llc',
                   ', ltd', ' ltd', ' corp.', ' corp', ' co.', ' co']:
        n = n.replace(suffix, '')
    return re.sub(r'[^a-z0-9]', '', n)


def normalize_url(url: str) -> str:
    """Normalize URL for matching — strip tracking params, trailing slashes."""
    if not url:
        return ''
    url = url.split('?')[0].split('#')[0].rstrip('/')
    url = re.sub(r'^https?://(www\.)?', '', url.lower())
    return url


def canonical_job_key(source_platform: str, source_job_id: str,
                      job_url: str, company_id: int) -> str:
    """SHA-256 canonical key."""
    raw = f"{source_platform}|{source_job_id}|{normalize_url(job_url)}|{company_id}"
    return hashlib.sha256(raw.encode()).hexdigest()


def compute_title_ai_terms(title: str) -> Tuple[int, str]:
    """Compute has_ai_in_title (0/1) and comma-separated title_ai_terms."""
    if not title:
        return 0, ''
    terms = []
    for pattern, label in TITLE_AI_TERMS:
        if re.search(pattern, title, re.IGNORECASE):
            terms.append(label)
    has_ai = 1 if terms else 0
    return has_ai, ', '.join(terms)


def extract_salary(text: str) -> Optional[Dict]:
    """Extract salary from text. Returns dict or None."""
    if not text:
        return None
    # Skip non-USD
    if NON_USD_PATTERNS.search(text):
        return {'currency': 'non_usd', 'skip': True}

    for pat in SALARY_PATTERNS:
        m = pat.search(text)
        if m:
            min_str = m.group(1).replace(',', '')
            max_str = m.group(2).replace(',', '')
            period = m.group(3) if m.lastindex >= 3 and m.group(3) else 'annual'

            try:
                min_val = float(min_str)
                max_val = float(max_str)
            except ValueError:
                continue

            # Handle k suffix
            if min_val < 1000 and 'k' in text[m.start():m.end()].lower():
                min_val *= 1000
            if max_val < 1000 and 'k' in text[m.start():m.end()].lower():
                max_val *= 1000

            # Period conversion
            period_lower = (period or '').lower()
            if period_lower in ('hour', 'hourly'):
                min_val *= 2080
                max_val *= 2080
                sal_period = 'Hourly'
            elif period_lower in ('month', 'monthly'):
                min_val *= 12
                max_val *= 12
                sal_period = 'Annual'
            else:
                sal_period = 'Annual'

            min_val = int(min_val)
            max_val = int(max_val)

            # Sanity checks
            if min_val < 15000 or max_val > 600000:
                return None
            if max_val > 0 and min_val > 0 and max_val / min_val > 5:
                return None
            if min_val > max_val:
                min_val, max_val = max_val, min_val

            return {
                'salary_min_usd': min_val,
                'salary_max_usd': max_val,
                'salary_period': sal_period,
                'salary_currency': 'USD',
                'salary_text': m.group(0),
            }
    return None


def extract_skills(text: str) -> Tuple[str, int, int]:
    """Extract skills, has_python, has_sql from text."""
    if not text:
        return '', 0, 0
    t = text.lower()
    skills = []
    for name, pat in SKILL_PATTERNS:
        if name == 'R':
            # R needs case-sensitive match (uppercase R only)
            if re.search(pat, text):  # use original text, not lowered
                skills.append(name)
        elif re.search(pat, t, re.IGNORECASE):
            skills.append(name)
    # Validate: R alone is noise — keep only if context supports it
    if skills == ['R']:
        # Keep R if title/text strongly suggests R programming
        r_context = any(kw in t for kw in [
            'r developer', 'r programmer', 'statistical', 'biostatist',
            'rstudio', 'r studio', 'cran', 'tidyverse', 'ggplot',
            'shiny app',
        ])
        if not r_context:
            skills = []
    has_python = 1 if 'Python' in skills else 0
    has_sql = 1 if 'SQL' in skills else 0
    return ', '.join(skills), has_python, has_sql


def extract_company_from_url(url: str) -> Optional[str]:
    """Extract company name from ATS URL slug patterns."""
    if not url:
        return None
    for pattern, formatter in ATS_SLUG_PATTERNS:
        m = re.search(pattern, url)
        if m:
            name = formatter(m)
            # Clean up known edge cases
            name = re.sub(r'\d+$', '', name).strip()  # trailing numbers
            return name if name else None
    # LinkedIn: at-companyname
    m = re.search(r'linkedin\.com/jobs/view/.*at-([a-z0-9-]+)', url, re.IGNORECASE)
    if m:
        return m.group(1).replace('-', ' ').title()
    return None


def canonicalize_platform(raw: str) -> str:
    """Map raw platform name to canonical form."""
    if not raw:
        return 'Other'
    key = raw.lower().strip()
    # Direct mapping
    if key in PLATFORM_CANONICAL:
        return PLATFORM_CANONICAL[key]
    # Substring matching
    for k, v in PLATFORM_CANONICAL.items():
        if k in key:
            return v
    return raw  # Keep original if no match


def window_bucket(posted_date: str) -> str:
    """Compute window_bucket from posted_date string."""
    if not posted_date:
        return 'UNCERTAIN'
    try:
        d = datetime.strptime(posted_date[:10], '%Y-%m-%d').date()
        if d >= date(2025, 7, 1) and d <= date(2025, 12, 31):
            return 'H2_2025'
        elif d >= date(2026, 1, 1) and d <= date(2026, 3, 31):
            return 'Q1_2026'
        else:
            return 'UNCERTAIN'
    except (ValueError, TypeError):
        return 'UNCERTAIN'


def normalize_title_to_segment(raw_title: str) -> str:
    """Normalize a raw job title to a canonical segment (15-20 buckets)."""
    if not raw_title:
        return ''
    t = raw_title.lower()
    for pattern, canonical in TITLE_SEGMENTS.items():
        if re.search(pattern, t):
            return canonical
    return raw_title  # fallback: keep raw if no match


def strip_html(text: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    if not text:
        return text
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'&[a-zA-Z]+;', ' ', clean)  # HTML entities
    clean = re.sub(r'&\#\d+;', ' ', clean)  # numeric entities
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean


def is_aggregator_url(url: str) -> bool:
    """Check if a URL belongs to a job aggregator (not primary ATS)."""
    if not url:
        return False
    try:
        domain = urlparse(url).netloc.lstrip('www.')
        return domain in BLOCKED_DOMAINS
    except Exception:
        return False

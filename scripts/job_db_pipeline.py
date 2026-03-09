#!/usr/bin/env python3
"""
AI Analyst Jobs — Unified Research Pipeline
=============================================
Single script covering: schema init, DB merge, company build,
enrichment (Tiers 0-3), backfills, QA, export, approval gate,
and Phase 2 analysis (dashboard + report).

Usage:
    python scripts/job_db_pipeline.py <command> [options]

Commands:
    init_db               Create all tables + migrations
    merge_dbs             Merge Claude DB + Codex DB with dedup
    build_companies       Populate companies_200 (exactly 200)
    collect_raw           Multi-source collection via Tavily + ATS APIs
    build_gold            Filter raw_postings → job_postings_gold
    detect_ats            Detect/confirm ATS platform per company
    mine_salary_from_body Parse salary from stored descriptions (FREE, no HTTP)
    verify_and_enrich     Run enrichment Tiers 0 → 1 → 2A → 2B → 2C → 3
    backfill_title_ai     Compute has_ai_in_title + title_ai_terms
    backfill_ai_role_signature  Classify ai_role_signature for all rows
    backfill_skills       Extract skills_extracted, has_python, has_sql
    normalize_platforms   Canonicalize source_platform names
    qa_check              Run all quality gates → qa_violations
    export_review         Export CSVs to review/ + qa_report.json
    approve_db            QA gate → block if CRITICAL > 0 → approval_state
    analyze_approved      HARD BLOCKED until approved; runs Phase 2
"""

import argparse
import csv
import hashlib
import json
import os
import random
import re
import sqlite3
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

# ─── Paths ───────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_DIR = PROJECT_ROOT / "db"
DB_PATH = DB_DIR / "job_postings_gold.db"
REVIEW_DIR = PROJECT_ROOT / "review"
OUTPUT_DIR = PROJECT_ROOT / "output"

CLAUDE_DB = Path.home() / "Documents" / "Claude Code" / "ai_analyst_roles_2026" / "db" / "job_postings_gold.db"
CODEX_DB = Path.home() / "Documents" / "Codex" / "artifacts" / "ai_analyst_roles_2026" / "db" / "job_postings_gold.db"

# ─── AI Keywords (word-boundary regex, case-insensitive) ─────────────────────
AI_KEYWORDS = [
    r'\bllm\b', r'\blarge language model\b', r'\bgenerative ai\b', r'\bgenai\b',
    r'\bgen ai\b', r'\bagentic\b', r'\bai agent\b', r'\bai agents\b',
    r'\bchatgpt\b', r'\bclaude\b', r'\bgemini\b', r'\bgpt-4\b', r'\bgpt-5\b',
    r'\bfoundation model\b', r'\brag\b', r'\bretrieval augmented\b',
    r'\bprompt engineering\b', r'\bvector database\b', r'\bembedding\b',
    r'\btext-to-sql\b', r'\bai evaluation\b', r'\bai adoption\b',
    r'\bai metrics\b', r'\bai product\b', r'\bai/ml\b',
    r'\bmachine learning\b', r'\bnatural language\b', r'\bnlp\b',
    r'\bcopilot\b', r'\bai assistant\b', r'\bai workflow\b',
    r'\bintelligent automation\b', r'\bai-powered\b', r'\bai-augmented\b',
    r'\bmultimodal\b', r'\brlhf\b', r'\bfine-tuning\b', r'\bfine tuning\b',
    r'\bai safety\b', r'\bresponsible ai\b',
]

# Title AI terms: regex pattern → human-readable label
TITLE_AI_TERMS = [
    (r'\bartificial general intelligence\b', 'Artificial General Intelligence'),
    (r'\bAGI\b', 'AGI'),
    (r'\bgenerative ai\b', 'generative AI'), (r'\bgenai\b', 'GenAI'),
    (r'\bgen ai\b', 'Gen AI'), (r'\blarge language model\b', 'large language model'),
    (r'\bfoundation model\b', 'foundation model'), (r'\bresponsible ai\b', 'responsible AI'),
    (r'\bai ethics\b', 'AI ethics'), (r'\bai safety\b', 'AI safety'),
    (r'\bai alignment\b', 'AI alignment'), (r'\bconversational ai\b', 'conversational AI'),
    (r'\bdecision intelligence\b', 'decision intelligence'), (r'\bmultimodal\b', 'multimodal'),
    (r'\bai/ml\b', 'AI/ML'), (r'\bai-ml\b', 'AI-ML'), (r'\bagentic\b', 'agentic'),
    (r'\bagents\b', 'agents'), (r'\bagent\b', 'agent'),
    (r'\bchatgpt\b', 'ChatGPT'), (r'\bgpt\b', 'GPT'), (r'\bgemini\b', 'Gemini'),
    (r'\bembedding\b', 'embedding'), (r'\bvector\b', 'vector'), (r'\bprompt\b', 'prompt'),
    (r'\bfine.tun', 'fine-tuning'), (r'\brlhf\b', 'RLHF'),
    (r'\bcopilot\b', 'copilot'), (r'\bintelligent\b', 'intelligent'),
    (r'\bai\b', 'AI'), (r'\bllms?\b', 'LLM'), (r'\bnlp\b', 'NLP'),
    (r'\bml\b', 'ML'), (r'\brag\b', 'RAG'),
]

# Role cluster inclusion
ROLE_CLUSTERS_INCLUDED = {
    'Product Analyst', 'Data Analyst', 'Analytics Analyst',
    'Data Scientist', 'Applied Data Scientist', 'Product Data Scientist',
    'Business Data Scientist', 'Growth Data Scientist', 'Decision Data Scientist',
    'Experimentation Scientist', 'A/B Testing Scientist',
    'Growth Analyst', 'Revenue Analyst', 'Marketing Analyst',
    'Lifecycle Analyst', 'GTM Analyst', 'Monetization Analyst', 'Pricing Analyst',
    'Analytics Engineer', 'Decision Scientist', 'Quantitative Analyst',
    'Operations Analyst',
    'AI Analyst', 'LLM Analyst', 'Agentic Analytics Lead',
    'Generative AI Analyst', 'Decision Intelligence Analyst',
    'AI Product Analyst', 'AI/ML Insights Analyst', 'AI Evaluation Analyst',
    'AI Trust Analyst', 'Data Scientist Strategic Intelligence',
    'Quantitative Intelligence Analyst',
    'Business Operations Analyst (AI & Automation)',
}

# Role exclusion patterns (hard, even with AI signal)
ROLE_EXCLUSION_PATTERNS = [
    r'\bBI Engineer\b', r'\bML Engineer\b', r'\bMLOps Engineer\b',
    r'\bLLM Engineer\b', r'\bAI Platform Engineer\b',
    r'\bSoftware Engineer\b', r'\bInfrastructure Engineer\b',
    r'\bData Platform Engineer\b', r'\bData Engineer\b',
    r'\bProduct Manager\b', r'\bTPM\b', r'\bAPM\b', r'\bProgram Manager\b',
    r'\bResearch Scientist\b', r'\bDevOps\b',
    r'\bSecurity Engineer\b', r'\bSecurity Architect\b',
    r'\bCybersecurity Analyst\b', r'\bInfoSec\b',
    r'\bFinance Analyst\b',
]

# Platform canonical names
PLATFORM_CANONICAL = {
    'greenhouse': 'Greenhouse', 'greenhouse.io': 'Greenhouse',
    'lever': 'Lever', 'lever.co': 'Lever',
    'ashby': 'Ashby', 'ashbyhq': 'Ashby', 'ashbyhq.com': 'Ashby',
    'workday': 'Workday', 'myworkdayjobs': 'Workday',
    'smartrecruiters': 'SmartRecruiters',
    'amazon': 'Amazon Jobs', 'amazon.jobs': 'Amazon Jobs',
    'linkedin': 'LinkedIn', 'linkedin.com': 'LinkedIn',
    'google': 'Google Careers', 'google careers': 'Google Careers',
    'meta': 'Meta Careers', 'meta careers': 'Meta Careers',
    'apple': 'Apple Jobs', 'apple jobs': 'Apple Jobs',
    'netflix': 'Netflix', 'snap': 'Snap',
    'workable': 'Workable', 'bamboohr': 'BambooHR', 'rippling': 'Rippling',
}

# Salary regex patterns
SALARY_PATTERNS = [
    # "$150,000 - $200,000" or "$150k - $200k"
    re.compile(
        r'\$\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?\s*[-–—to]+\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?'
        r'(?:\s*(?:per\s+)?(year|annual|annually|hour|hourly|month|monthly))?',
        re.IGNORECASE
    ),
    # "USD 150,000 to 200,000"
    re.compile(
        r'(?:USD|US\$)\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?\s*(?:to|-|–|—)\s*(?:USD|US\$)?\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?'
        r'(?:\s*(?:per\s+)?(year|annual|annually|hour|hourly|month|monthly))?',
        re.IGNORECASE
    ),
    # "salary range: $X to $Y" / "compensation: $X - $Y"
    re.compile(
        r'(?:salary|compensation|pay)\s*(?:range)?[:\s]*\$\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?\s*[-–—to]+\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?',
        re.IGNORECASE
    ),
]

# Non-USD currency patterns (detect and skip)
NON_USD_PATTERNS = re.compile(r'(?:CAD|C\$|CA\$|GBP|£|EUR|€)', re.IGNORECASE)


# ─── Utility Functions ───────────────────────────────────────────────────────

def get_db(path: Path = None) -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode."""
    p = path or DB_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def row_to_dict(row) -> dict:
    """Convert sqlite3.Row to a plain dict with .get() support."""
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    return {k: row[k] for k in row.keys()}


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


def match_ai_keywords(text: str) -> List[str]:
    """Return list of AI keywords found in text using word-boundary regex."""
    if not text:
        return []
    text_lower = text.lower()
    hits = []
    for pattern in AI_KEYWORDS:
        if re.search(pattern, text_lower, re.IGNORECASE):
            # Extract the human-readable term
            term = pattern.replace(r'\b', '').replace('\\b', '')
            hits.append(term)
    # Remove false positives
    false_pos = _check_false_positives(text_lower, hits)
    return [h for h in hits if h not in false_pos]


def _check_false_positives(text: str, hits: List[str]) -> set:
    """Detect word-boundary false positives like 'email'→'ml'."""
    fps = set()
    if 'ml' in hits:
        # Check if 'ml' only appears inside words like 'email', 'html'
        ml_matches = list(re.finditer(r'\bml\b', text, re.IGNORECASE))
        if not ml_matches:
            fps.add('ml')
    if 'agent' in hits or 'agents' in hits:
        # Check for 'management' false positive
        if re.search(r'\bmanagement\b', text, re.IGNORECASE) and \
           not re.search(r'\bagent\b', text, re.IGNORECASE):
            fps.discard('agent')
    if 'rag' in hits:
        # Check for 'storage', 'garage' etc.
        if not re.search(r'\brag\b', text, re.IGNORECASE):
            fps.add('rag')
    return fps


def compute_title_ai_terms(title: str) -> Tuple[int, str]:
    """Compute has_ai_in_title and title_ai_terms from title."""
    if not title:
        return 0, ''
    t = title.lower()
    matched = []
    for pattern, label in TITLE_AI_TERMS:
        if re.search(pattern, t, re.IGNORECASE):
            if label not in matched:
                matched.append(label)
    has_ai = 1 if matched else 0
    return has_ai, ', '.join(matched)


def classify_ai_role_signature(title: str, description: str = '',
                               skills: str = '') -> str:
    """Classify ai_role_signature in priority order."""
    t = (title or '').lower()
    desc = (description or '').lower()
    sk = (skills or '').lower()
    combined_scope = f"{desc} {sk}"

    # Priority 1: emerging_ai_named_role
    emerging = [
        r'\bai analyst\b', r'\bllm analyst\b', r'\bagentic analytics\b',
        r'\bdecision intelligence analyst\b', r'\bai evaluation analyst\b',
        r'\bgenerative ai analyst\b',
    ]
    for pat in emerging:
        if re.search(pat, t, re.IGNORECASE):
            return 'emerging_ai_named_role'

    # Split title at comma/dash/parens for role vs team context
    role_part = re.split(r'[,\-–—(]', t)[0].strip()
    after_part = t[len(role_part):]

    ai_title_terms = [r'\bai\b', r'\bllm\b', r'\bagentic\b', r'\bgenai\b',
                      r'\bgenerative ai\b', r'\bml\b', r'\bnlp\b',
                      r'\bartificial general intelligence\b', r'\bagi\b']
    llm_genai_terms = [r'\bllm\b', r'\bgenai\b', r'\bgpt\b', r'\bgpt-4\b',
                       r'\bfoundation model\b', r'\blarge language model\b',
                       r'\bgenerative ai\b']
    agentic_terms = [r'\bagentic\b', r'\bai agent\b', r'\bai agents\b']
    ai_team_terms = [r'\bai platform\b', r'\bai team\b', r'\bfoundation ai\b']

    # Priority 2: ai_in_title
    if any(re.search(p, role_part, re.I) for p in ai_title_terms):
        return 'ai_in_title'

    # Priority 3: ai_team_or_platform_in_title
    if any(re.search(p, after_part, re.I) for p in ai_title_terms):
        return 'ai_team_or_platform_in_title'

    # Priority 4: llm_or_genai_in_scope
    if any(re.search(p, combined_scope, re.I) for p in llm_genai_terms):
        return 'llm_or_genai_in_scope'

    # Priority 5: agentic_in_scope
    if any(re.search(p, combined_scope, re.I) for p in agentic_terms):
        return 'agentic_in_scope'

    # Priority 6: ai_team_or_platform_in_scope
    if any(re.search(p, combined_scope, re.I) for p in ai_team_terms):
        return 'ai_team_or_platform_in_scope'

    # Priority 7: ai_in_description_only
    if any(re.search(p, combined_scope, re.I) for p in AI_KEYWORDS):
        return 'ai_in_description_only'

    return 'ai_in_description_only'


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
    skill_patterns = [
        ('Python', r'\bpython\b'), ('SQL', r'\bsql\b'),
        ('R', r'(?<![A-Za-z])\bR\b(?!\s*&|\w)'),
        ('Tableau', r'\btableau\b'), ('Looker', r'\blooker\b'),
        ('Power BI', r'\bpower\s*bi\b'), ('Excel', r'\bexcel\b'),
        ('dbt', r'\bdbt\b'), ('Spark', r'\bspark\b'), ('Airflow', r'\bairflow\b'),
        ('BigQuery', r'\bbigquery\b'), ('Snowflake', r'\bsnowflake\b'),
        ('Redshift', r'\bredshift\b'), ('Databricks', r'\bdatabricks\b'),
        ('Pandas', r'\bpandas\b'), ('NumPy', r'\bnumpy\b'),
        ('Scikit-learn', r'\bscikit'), ('TensorFlow', r'\btensorflow\b'),
        ('PyTorch', r'\bpytorch\b'), ('Keras', r'\bkeras\b'),
        ('Jupyter', r'\bjupyter\b'), ('Git', r'\bgit\b'),
        ('AWS', r'\baws\b'), ('GCP', r'\bgcp\b'), ('Azure', r'\bazure\b'),
        ('Docker', r'\bdocker\b'), ('Kubernetes', r'\bkubernetes\b'),
        ('Kafka', r'\bkafka\b'), ('Hadoop', r'\bhadoop\b'),
        ('Hive', r'\bhive\b'), ('Presto', r'\bpresto\b'),
        ('Mixpanel', r'\bmixpanel\b'), ('Amplitude', r'\bamplitude\b'),
        ('Segment', r'\bsegment\b'), ('Fivetran', r'\bfivetran\b'),
        ('LangChain', r'\blangchain\b'), ('LlamaIndex', r'\bllamaindex\b'),
        ('Hugging Face', r'\bhugging\s*face\b'), ('OpenAI API', r'\bopenai\b'),
        ('Statsmodels', r'\bstatsmodels\b'), ('SciPy', r'\bscipy\b'),
        ('A/B Testing', r'\ba/?b\s*test'), ('Causal Inference', r'\bcausal\s*inference\b'),
        ('Bayesian', r'\bbayesian\b'), ('NLP', r'\bnlp\b'),
        ('LLM', r'\bllm\b'), ('RAG', r'\brag\b'),
    ]
    for name, pat in skill_patterns:
        if name == 'R':
            # R needs case-sensitive match (uppercase R only)
            if re.search(pat, text):  # use original text, not lowered
                skills.append(name)
        elif re.search(pat, t, re.IGNORECASE):
            skills.append(name)
    # Validate: R alone is noise — require at least 1 other real skill
    if skills == ['R']:
        skills = []
    has_python = 1 if 'Python' in skills else 0
    has_sql = 1 if 'SQL' in skills else 0
    return ', '.join(skills), has_python, has_sql


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


def is_role_excluded(title: str) -> bool:
    """Check if a role title matches exclusion patterns."""
    if not title:
        return False
    for pat in ROLE_EXCLUSION_PATTERNS:
        if re.search(pat, title, re.IGNORECASE):
            return True
    return False


## ── Data Quality Fix Utilities ─────────────────────────────────────────────

ATS_SLUG_PATTERNS = [
    (r'boards\.greenhouse\.io/([^/]+)/', lambda m: m.group(1).replace('-', ' ').title()),
    (r'jobs\.lever\.co/([^/]+)/', lambda m: m.group(1).replace('-', ' ').title()),
    (r'jobs\.ashbyhq\.com/([^/]+)/', lambda m: m.group(1).replace('-', ' ').title()),
    (r'jobs\.([^.]+)\.com/', lambda m: m.group(1).replace('-', ' ').title()),
]


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


TITLE_SEGMENTS = {
    r'\bapplied scientist\b': 'Applied Scientist',
    r'\bstaff data scientist\b': 'Staff Data Scientist',
    r'\bsenior data scientist\b|sr\.?\s+data scientist': 'Senior Data Scientist',
    r'\bprincipal data scientist\b': 'Principal Data Scientist',
    r'\blead data scientist\b': 'Lead Data Scientist',
    r'\bdata scientist\b': 'Data Scientist',
    r'\bdata science manager\b|manager.*data science': 'Data Science Manager',
    r'\bdirector.*data|data.*director\b': 'Director, Data',
    r'\banalytics engineer\b': 'Analytics Engineer',
    r'\bsenior data analyst\b|sr\.?\s+data analyst': 'Senior Data Analyst',
    r'\bdata analyst\b': 'Data Analyst',
    r'\bproduct analyst\b': 'Product Analyst',
    r'\bbusiness analyst\b': 'Business Analyst',
    r'\boperations analyst\b|ops analyst': 'Operations Analyst',
    r'\bgrowth analyst\b': 'Growth Analyst',
    r'\bmarketing analyst\b': 'Marketing Analyst',
    r'\bquantitative analyst\b|quant analyst': 'Quantitative Analyst',
    r'\bdecision scientist\b': 'Decision Scientist',
    r'\bresearch scientist\b': 'Research Scientist',
    r'\bcompetitive intelligence analyst\b': 'Competitive Intelligence Analyst',
}


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


BLOCKED_DOMAINS = {
    'builtin.com', 'builtinnyc.com', 'builtinsf.com', 'builtinchicago.com',
    'builtinaustin.com', 'builtinboston.com', 'builtincolorado.com',
    'builtinla.com', 'builtinseattle.com',
    'theladders.com', 'themuse.com', 'towardsai.net',
    'wallstreetcareers.com', 'datasciencessjobs.com', 'technyjobs.com',
    'wellfound.com', 'angel.co',
}


def is_aggregator_url(url: str) -> bool:
    """Check if a URL belongs to a job aggregator (not primary ATS)."""
    if not url:
        return False
    try:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lstrip('www.')
        return domain in BLOCKED_DOMAINS
    except Exception:
        return False


REMOTE_PATTERNS = [r'\bremote\b', r'\bwork from home\b', r'\bwfh\b', r'\bfully remote\b']
HYBRID_PATTERNS = [r'\bhybrid\b', r'\bflexible\b', r'\b\d+\s*days.*office\b']


def resolve_work_mode(ats_work_mode: str, location_raw: str,
                      location_standardized: str) -> str:
    """Resolve work_mode from ATS field + location text."""
    # Priority 1: ATS-provided value (if meaningful)
    if ats_work_mode and ats_work_mode.lower() not in ('unknown', '', 'on-site'):
        return ats_work_mode
    # Priority 2: Infer from location text
    loc = f"{location_raw or ''} {location_standardized or ''}".lower()
    if any(re.search(p, loc) for p in REMOTE_PATTERNS):
        return 'Remote'
    if any(re.search(p, loc) for p in HYBRID_PATTERNS):
        return 'Hybrid'
    # Priority 3: if ATS explicitly said On-site AND nothing contradicts, keep it
    if ats_work_mode and ats_work_mode.lower() == 'on-site':
        return 'On-site'
    return 'On-site'


APPLIED_SCIENTIST_KEEP_KEYWORDS = [
    'analytics', 'measurement', 'insights', 'ads science',
    'experimentation', 'causal', 'decision', 'ranking',
    'recommendation', 'personalization', 'search relevance',
]
APPLIED_SCIENTIST_REMOVE_KEYWORDS = [
    'llm agent', 'code agent', 'foundation model', 'pretraining',
    'robotics', 'computer vision', 'speech', 'autonomous',
    'systems', 'infrastructure', 'compiler',
]


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: init_db
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_init_db():
    """Create all tables + ALTER TABLE migrations for existing DBs."""
    conn = get_db()
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS companies_200 (
        company_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name        TEXT NOT NULL,
        canonical_name      TEXT NOT NULL,
        tier                TEXT NOT NULL,
        sector              TEXT NOT NULL,
        hq_country          TEXT NOT NULL DEFAULT 'US',
        ats_platform        TEXT,
        ats_platform_secondary TEXT,
        career_page_url     TEXT,
        ats_board_slug      TEXT,
        in_scope            INTEGER NOT NULL DEFAULT 1,
        created_at          TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS job_postings_gold (
        gold_id             INTEGER PRIMARY KEY AUTOINCREMENT,
        canonical_job_key   TEXT NOT NULL UNIQUE,
        company_id          INTEGER REFERENCES companies_200(company_id),
        company_name        TEXT NOT NULL,
        source_platform     TEXT NOT NULL,
        source_job_id       TEXT NOT NULL,
        job_url             TEXT NOT NULL,
        url_http_status     INTEGER,
        url_checked_at      TEXT,
        title               TEXT NOT NULL,
        title_normalized    TEXT NOT NULL,
        role_cluster        TEXT NOT NULL,
        seniority           TEXT,
        posted_date         TEXT,
        date_uncertain      INTEGER NOT NULL DEFAULT 0,
        window_bucket       TEXT,
        location_raw        TEXT,
        location_city       TEXT,
        location_state      TEXT,
        location_standardized TEXT,
        country             TEXT DEFAULT 'US',
        is_us               INTEGER NOT NULL DEFAULT 0,
        work_mode           TEXT,
        status              TEXT NOT NULL DEFAULT 'Open',
        ai_signal_types     TEXT,
        ai_keywords_hit     TEXT,
        ai_role_signature   TEXT,
        skills_extracted    TEXT,
        has_python          INTEGER DEFAULT 0,
        has_sql             INTEGER DEFAULT 0,
        salary_currency     TEXT,
        salary_min_usd      INTEGER,
        salary_max_usd      INTEGER,
        salary_period       TEXT,
        salary_text         TEXT,
        has_ai_in_title     INTEGER DEFAULT 0,
        title_ai_terms      TEXT,
        description_snippet TEXT,
        enrich_status       TEXT NOT NULL DEFAULT 'pending',
        verified_date       TEXT,
        created_at          TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS raw_postings (
        raw_id              INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name        TEXT,
        source_platform     TEXT,
        source_job_id       TEXT,
        job_url             TEXT,
        title               TEXT,
        location_raw        TEXT,
        body_raw            TEXT,
        posted_date         TEXT,
        salary_text         TEXT,
        collected_at        TEXT NOT NULL DEFAULT (datetime('now')),
        processed           INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS qa_violations (
        violation_id        INTEGER PRIMARY KEY AUTOINCREMENT,
        gold_id             INTEGER,
        rule_name           TEXT NOT NULL,
        severity            TEXT NOT NULL,
        details             TEXT,
        created_at          TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS source_attempts (
        attempt_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        source_platform     TEXT,
        company_name        TEXT,
        url_attempted       TEXT,
        http_status         INTEGER,
        result              TEXT,
        attempted_at        TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS approval_state (
        id                  INTEGER PRIMARY KEY,
        approved_by_user    INTEGER NOT NULL DEFAULT 0,
        approved_at         TEXT,
        row_count_at_approval INTEGER,
        critical_violations INTEGER,
        warning_violations  INTEGER
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_job_key ON job_postings_gold(canonical_job_key);
    CREATE INDEX IF NOT EXISTS idx_gold_company ON job_postings_gold(company_id);
    CREATE INDEX IF NOT EXISTS idx_gold_status ON job_postings_gold(status);
    CREATE INDEX IF NOT EXISTS idx_gold_platform ON job_postings_gold(source_platform);
    """)

    conn.commit()
    log("init_db: All tables created/verified.")

    # Run ALTER TABLE migrations for any missing columns
    existing_cols = {r[1] for r in cur.execute("PRAGMA table_info(job_postings_gold)").fetchall()}
    migrations = {
        'has_ai_in_title': 'INTEGER DEFAULT 0',
        'title_ai_terms': 'TEXT',
        'ai_role_signature': 'TEXT',
        'description_snippet': 'TEXT',
        'has_python': 'INTEGER DEFAULT 0',
        'has_sql': 'INTEGER DEFAULT 0',
        'location_city': 'TEXT',
        'location_state': 'TEXT',
        'verified_date': 'TEXT',
        'ats_platform_secondary': 'TEXT',
    }
    for col, dtype in migrations.items():
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE job_postings_gold ADD COLUMN {col} {dtype}")
            log(f"  Migration: added {col} to job_postings_gold")

    # companies_200 migrations
    comp_cols = {r[1] for r in cur.execute("PRAGMA table_info(companies_200)").fetchall()}
    comp_migrations = {
        'ats_platform_secondary': 'TEXT',
        'ats_board_slug': 'TEXT',
    }
    for col, dtype in comp_migrations.items():
        if col not in comp_cols:
            cur.execute(f"ALTER TABLE companies_200 ADD COLUMN {col} {dtype}")
            log(f"  Migration: added {col} to companies_200")

    conn.commit()
    conn.close()
    log("init_db: Migrations complete.")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: merge_dbs
# ═══════════════════════════════════════════════════════════════════════════════

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

    # Disable FK constraints during merge — company_id references may not
    # exist in companies_200 yet, and source DBs have their own id spaces.
    conn.execute("PRAGMA foreign_keys=OFF")

    # Track URLs and company+title+date for dedup
    seen_urls = {}      # normalized_url → gold_id
    seen_combos = {}    # (norm_company, norm_title, date) → gold_id

    # ── Phase 1: Import Claude DB ──
    log("merge_dbs: Importing Claude DB...")
    cdb = sqlite3.connect(str(CLAUDE_DB))
    cdb.row_factory = sqlite3.Row
    claude_rows = [row_to_dict(r) for r in cdb.execute("SELECT * FROM job_postings_gold").fetchall()]
    log(f"  Claude DB: {len(claude_rows)} rows")

    imported_claude = 0
    for r in claude_rows:
        norm_url = normalize_url(r['job_url'])
        platform = canonicalize_platform(r['source_platform'])
        cjk = r['canonical_job_key'] or canonical_job_key(
            platform, r['source_job_id'], r['job_url'], r.get('company_id', 0) or 0
        )
        has_ai, t_ai_terms = compute_title_ai_terms(r.get('title_normalized', r.get('title', '')))
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
    codex_rows = [row_to_dict(r) for r in xdb.execute("SELECT * FROM job_postings_gold").fetchall()]
    log(f"  Codex DB: {len(codex_rows)} rows")

    imported_codex = 0
    merged_codex = 0
    for r in codex_rows:
        norm_url = normalize_url(r['job_url'])
        platform = canonicalize_platform(r.get('data_source', ''))

        # PRIMARY dedup: URL match
        if norm_url in seen_urls:
            # Merge fields: prefer Codex salary if Claude has NULL
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
        claude_companies = [row_to_dict(c) for c in cdb.execute("SELECT * FROM companies_200").fetchall()]
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

    # Re-enable FK constraints
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


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: build_companies
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_build_companies():
    """Populate companies_200 with exactly 200 US big-tech/AI companies."""
    conn = get_db()
    cur = conn.cursor()
    existing = cur.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
    if existing == 200:
        log(f"build_companies: Already have {existing} companies. Done.")
        conn.close()
        return
    if existing > 0 and existing != 200:
        log(f"build_companies: Have {existing} companies, need exactly 200.")
        log("  Will add missing companies to reach 200.")

    # The 200 companies — canonical list of US big-tech + AI companies
    # Format: (company_name, tier, sector, ats_platform, ats_board_slug)
    COMPANIES = [
        # Tier 1: FAANG+ Mega-caps
        ("Google", "Tier1", "Big Tech", "Custom", None),
        ("Meta", "Tier1", "Big Tech", "Custom", None),
        ("Amazon", "Tier1", "Big Tech", "Custom", None),
        ("Apple", "Tier1", "Big Tech", "Custom", None),
        ("Microsoft", "Tier1", "Big Tech", "Workday", None),
        ("Netflix", "Tier1", "Big Tech", "Custom", None),
        # Tier 1: AI Leaders
        ("OpenAI", "Tier1", "AI Native", "Ashby", "openai"),
        ("Anthropic", "Tier1", "AI Native", "Greenhouse", "anthropic"),
        ("NVIDIA", "Tier1", "AI/Semiconductor", "Workday", None),
        ("Tesla", "Tier1", "AI/Automotive", "Custom", None),
        # Tier 2: Major Tech
        ("Salesforce", "Tier2", "Enterprise SaaS", "Workday", None),
        ("Adobe", "Tier2", "Software", "Workday", None),
        ("Oracle", "Tier2", "Enterprise", "Custom", None),
        ("IBM", "Tier2", "Enterprise", "Workday", None),
        ("Uber", "Tier2", "Marketplace", "Greenhouse", "uber"),
        ("Lyft", "Tier2", "Marketplace", "Greenhouse", "lyft"),
        ("Airbnb", "Tier2", "Marketplace", "Greenhouse", "airbnb"),
        ("Snap", "Tier2", "Social Media", "Custom", None),
        ("Pinterest", "Tier2", "Social Media", "Greenhouse", "pinterest"),
        ("Reddit", "Tier2", "Social Media", "Greenhouse", "reddit"),
        ("Twitter/X", "Tier2", "Social Media", "Custom", None),
        ("LinkedIn", "Tier2", "Social/Professional", "Custom", None),
        ("Spotify", "Tier2", "Streaming", "Greenhouse", "spotify"),
        ("Block (Square)", "Tier2", "Fintech", "Greenhouse", "block"),
        ("Stripe", "Tier2", "Fintech", "Greenhouse", "stripe"),
        ("PayPal", "Tier2", "Fintech", "Workday", None),
        ("Intuit", "Tier2", "Fintech", "Custom", None),
        ("Coinbase", "Tier2", "Crypto/Fintech", "Greenhouse", "coinbase"),
        ("Robinhood", "Tier2", "Fintech", "Greenhouse", "robinhood"),
        ("Plaid", "Tier2", "Fintech", "Greenhouse", "plaid"),
        ("DoorDash", "Tier2", "Marketplace", "Greenhouse", "doordash"),
        ("Instacart", "Tier2", "Marketplace", "Greenhouse", "instacart"),
        ("Grubhub", "Tier2", "Marketplace", "Greenhouse", None),
        ("Shopify", "Tier2", "E-commerce", "Greenhouse", "shopify"),
        ("Wayfair", "Tier2", "E-commerce", "Greenhouse", "wayfair"),
        ("Etsy", "Tier2", "E-commerce", "Greenhouse", "etsy"),
        ("eBay", "Tier2", "E-commerce", "Custom", None),
        ("Figma", "Tier2", "Design/SaaS", "Greenhouse", "figma"),
        ("Canva", "Tier2", "Design/SaaS", "Greenhouse", "canva"),
        ("Notion", "Tier2", "Productivity", "Greenhouse", "notion"),
        ("Slack (Salesforce)", "Tier2", "Productivity", "Greenhouse", None),
        ("Zoom", "Tier2", "Communication", "Workday", None),
        ("Atlassian", "Tier2", "Software", "Custom", None),
        ("ServiceNow", "Tier2", "Enterprise SaaS", "Workday", None),
        ("Snowflake", "Tier2", "Data Infrastructure", "Greenhouse", "snowflake"),
        ("Databricks", "Tier2", "Data Infrastructure", "Greenhouse", "databricks"),
        ("Palantir", "Tier2", "Data/Analytics", "Greenhouse", "palantir"),
        ("Datadog", "Tier2", "Observability", "Greenhouse", "datadog"),
        ("Splunk (Cisco)", "Tier2", "Observability", "Custom", None),
        ("Twilio", "Tier2", "Communication", "Greenhouse", "twilio"),
        # Tier 3: AI-Native / Growth Stage
        ("Cohere", "Tier3", "AI Native", "Greenhouse", "cohere"),
        ("Mistral AI", "Tier3", "AI Native", "Greenhouse", "mistral"),
        ("Perplexity AI", "Tier3", "AI Native", "Ashby", "perplexity-ai"),
        ("Inflection AI", "Tier3", "AI Native", "Greenhouse", "inflection"),
        ("Adept AI", "Tier3", "AI Native", "Greenhouse", "adept"),
        ("Character.AI", "Tier3", "AI Native", "Greenhouse", "character"),
        ("Stability AI", "Tier3", "AI Native", "Greenhouse", "stability-ai"),
        ("Runway", "Tier3", "AI/Creative", "Greenhouse", "runwayml"),
        ("Hugging Face", "Tier3", "AI Native", "Greenhouse", "huggingface"),
        ("Scale AI", "Tier3", "AI/Data", "Greenhouse", "scaleai"),
        ("Weights & Biases", "Tier3", "AI/MLOps", "Greenhouse", "wandb"),
        ("Anyscale", "Tier3", "AI Infrastructure", "Greenhouse", "anyscale"),
        ("Mosaic ML (Databricks)", "Tier3", "AI Native", "Greenhouse", None),
        ("Jasper AI", "Tier3", "AI/Content", "Greenhouse", "jasper"),
        ("Writer", "Tier3", "AI/Content", "Greenhouse", "writer"),
        ("Copy.ai", "Tier3", "AI/Content", "Greenhouse", None),
        ("Glean", "Tier3", "AI/Enterprise Search", "Greenhouse", "glean"),
        ("Harvey AI", "Tier3", "AI/Legal", "Ashby", "harvey"),
        ("Casetext (Thomson Reuters)", "Tier3", "AI/Legal", "Greenhouse", None),
        ("Moveworks", "Tier3", "AI/Enterprise", "Greenhouse", "moveworks"),
        ("Observe.AI", "Tier3", "AI/Contact Center", "Greenhouse", "observe-ai"),
        ("Ramp", "Tier3", "Fintech/AI", "Greenhouse", "ramp"),
        ("Brex", "Tier3", "Fintech/AI", "Greenhouse", "brex"),
        ("Navan (TripActions)", "Tier3", "Fintech/AI", "Greenhouse", "navan"),
        ("Rippling", "Tier3", "HR Tech/AI", "Rippling", None),
        ("Gusto", "Tier3", "HR Tech", "Greenhouse", "gusto"),
        ("Deel", "Tier3", "HR Tech", "Ashby", "deel"),
        ("Lattice", "Tier3", "HR Tech", "Greenhouse", "lattice"),
        ("Gong", "Tier3", "AI/Sales", "Greenhouse", "gong"),
        ("Clari", "Tier3", "AI/Sales", "Greenhouse", "clari"),
        ("Highspot", "Tier3", "AI/Sales", "Greenhouse", "highspot"),
        ("ZoomInfo", "Tier3", "Data/Sales", "Greenhouse", "zoominfo"),
        ("Amplitude", "Tier3", "Product Analytics", "Greenhouse", "amplitude"),
        ("Mixpanel", "Tier3", "Product Analytics", "Greenhouse", "mixpanel"),
        ("FullStory", "Tier3", "Product Analytics", "Greenhouse", "fullstory"),
        ("Heap (Contentsquare)", "Tier3", "Product Analytics", "Greenhouse", None),
        ("Braze", "Tier3", "Marketing Tech", "Greenhouse", "braze"),
        ("Iterable", "Tier3", "Marketing Tech", "Greenhouse", "iterable"),
        ("Klaviyo", "Tier3", "Marketing Tech", "Greenhouse", "klaviyo"),
        ("HubSpot", "Tier3", "Marketing/CRM", "Greenhouse", "hubspot"),
        ("MongoDB", "Tier3", "Database", "Greenhouse", "mongodb"),
        ("Elastic", "Tier3", "Search/Analytics", "Greenhouse", "elastic"),
        ("Confluent", "Tier3", "Data Streaming", "Greenhouse", "confluent"),
        ("dbt Labs", "Tier3", "Data/Analytics", "Greenhouse", "dbtlabs"),
        ("Fivetran", "Tier3", "Data Integration", "Greenhouse", "fivetran"),
        ("Census", "Tier3", "Data/Reverse ETL", "Greenhouse", "census"),
        ("Hex", "Tier3", "Data/Analytics", "Greenhouse", "hex"),
        ("Mode Analytics", "Tier3", "Data/Analytics", "Greenhouse", None),
        ("ThoughtSpot", "Tier3", "Analytics/AI", "Greenhouse", "thoughtspot"),
        ("Sigma Computing", "Tier3", "Analytics", "Greenhouse", "sigmacomputing"),
        ("Tableau (Salesforce)", "Tier3", "Analytics", "Workday", None),
        ("Looker (Google)", "Tier3", "Analytics", "Custom", None),
        ("Vercel", "Tier3", "Developer Platform", "Ashby", "vercel"),
        ("Supabase", "Tier3", "Developer Platform", "Ashby", "supabase"),
        ("Retool", "Tier3", "Developer Platform", "Greenhouse", "retool"),
        ("Postman", "Tier3", "Developer Platform", "Greenhouse", "postman"),
        # Tier 4: Established Tech / Late-stage
        ("Cisco", "Tier4", "Networking/Enterprise", "Custom", None),
        ("VMware (Broadcom)", "Tier4", "Enterprise", "Workday", None),
        ("Dell Technologies", "Tier4", "Enterprise", "Workday", None),
        ("HP Inc", "Tier4", "Enterprise", "Workday", None),
        ("Intel", "Tier4", "Semiconductor", "Workday", None),
        ("AMD", "Tier4", "Semiconductor", "Workday", None),
        ("Qualcomm", "Tier4", "Semiconductor", "Workday", None),
        ("Broadcom", "Tier4", "Semiconductor", "Workday", None),
        ("Micron", "Tier4", "Semiconductor", "Workday", None),
        ("Applied Materials", "Tier4", "Semiconductor", "Workday", None),
        ("Workday", "Tier4", "Enterprise SaaS", "Workday", None),
        ("SAP America", "Tier4", "Enterprise", "Custom", None),
        ("Palo Alto Networks", "Tier4", "Cybersecurity", "Workday", None),
        ("CrowdStrike", "Tier4", "Cybersecurity", "Workday", None),
        ("Fortinet", "Tier4", "Cybersecurity", "Workday", None),
        ("Okta", "Tier4", "Identity/Security", "Greenhouse", "okta"),
        ("Cloudflare", "Tier4", "Infrastructure", "Greenhouse", "cloudflare"),
        ("Akamai", "Tier4", "Infrastructure", "Workday", None),
        ("Fastly", "Tier4", "Infrastructure", "Greenhouse", "fastly"),
        ("DigitalOcean", "Tier4", "Cloud", "Greenhouse", "digitalocean"),
        ("HashiCorp (IBM)", "Tier4", "Infrastructure", "Greenhouse", None),
        ("Elastic", "Tier4", "Search", "Greenhouse", "elastic"),
        ("New Relic", "Tier4", "Observability", "Custom", None),
        ("PagerDuty", "Tier4", "Operations", "Greenhouse", "pagerduty"),
        ("Dynatrace", "Tier4", "Observability", "SmartRecruiters", None),
        ("Docusign", "Tier4", "Software", "Greenhouse", "docusign"),
        ("Dropbox", "Tier4", "Storage", "Greenhouse", "dropbox"),
        ("Box", "Tier4", "Storage", "Greenhouse", "box"),
        ("Asana", "Tier4", "Productivity", "Greenhouse", "asana"),
        ("Monday.com", "Tier4", "Productivity", "Greenhouse", "mondaycom"),
        ("Smartsheet", "Tier4", "Productivity", "SmartRecruiters", None),
        ("Zendesk", "Tier4", "CX", "Greenhouse", "zendesk"),
        ("Freshworks", "Tier4", "CX", "Greenhouse", "freshworks"),
        ("Sprinklr", "Tier4", "CX", "SmartRecruiters", None),
        ("Toast", "Tier4", "Restaurant Tech", "Greenhouse", "toast"),
        ("Square (Block)", "Tier4", "Fintech", "Greenhouse", None),
        ("Affirm", "Tier4", "Fintech", "Greenhouse", "affirm"),
        ("Marqeta", "Tier4", "Fintech", "Greenhouse", "marqeta"),
        ("SoFi", "Tier4", "Fintech", "Greenhouse", "sofi"),
        ("Chime", "Tier4", "Fintech", "Greenhouse", "chime"),
        ("Bill.com", "Tier4", "Fintech", "Greenhouse", "billcom"),
        ("Carta", "Tier4", "Fintech", "Greenhouse", "carta"),
        ("Wealthsimple", "Tier4", "Fintech", "Greenhouse", "wealthsimple"),
        ("Lemonade", "Tier4", "Insurtech/AI", "Greenhouse", "lemonade"),
        ("Root Insurance", "Tier4", "Insurtech/AI", "Greenhouse", "root-insurance"),
        ("Oscar Health", "Tier4", "Healthtech/AI", "Greenhouse", "oscar-health"),
        ("Tempus AI", "Tier4", "Healthtech/AI", "Greenhouse", "tempus"),
        ("Veeva Systems", "Tier4", "Healthtech", "Workday", None),
        ("Doximity", "Tier4", "Healthtech", "Greenhouse", "doximity"),
        ("Ro", "Tier4", "Healthtech", "Greenhouse", "ro"),
        ("Hims & Hers", "Tier4", "Healthtech", "Greenhouse", "hims"),
        ("Duolingo", "Tier4", "EdTech/AI", "Greenhouse", "duolingo"),
        ("Coursera", "Tier4", "EdTech", "Lever", "coursera"),
        ("Chegg", "Tier4", "EdTech", "Greenhouse", "chegg"),
        ("Quizlet", "Tier4", "EdTech/AI", "Greenhouse", "quizlet"),
        ("GitLab", "Tier4", "Developer Platform", "Greenhouse", "gitlab"),
        ("GitHub (Microsoft)", "Tier4", "Developer Platform", "Greenhouse", "github"),
        ("JFrog", "Tier4", "Developer Platform", "Greenhouse", "jfrog"),
        ("Snyk", "Tier4", "Developer Security", "Greenhouse", "snyk"),
        ("Sentry", "Tier4", "Developer Tools", "Greenhouse", "sentry"),
        ("LaunchDarkly", "Tier4", "Developer Tools", "Greenhouse", "launchdarkly"),
        ("Grafana Labs", "Tier4", "Observability", "Greenhouse", "grafanalabs"),
        ("Cockroach Labs", "Tier4", "Database", "Greenhouse", "cockroach-labs"),
        ("SingleStore", "Tier4", "Database", "Greenhouse", "singlestore"),
        ("Pinecone", "Tier4", "Vector Database/AI", "Ashby", "pinecone"),
        ("Weaviate", "Tier4", "Vector Database/AI", "Greenhouse", "weaviate"),
        ("Qdrant", "Tier4", "Vector Database/AI", "Greenhouse", None),
        ("LangChain", "Tier4", "AI Framework", "Greenhouse", None),
        ("Replit", "Tier4", "AI/Developer", "Ashby", "replit"),
        ("Cursor", "Tier4", "AI/Developer", "Ashby", "anysphere"),
        ("Codeium", "Tier4", "AI/Developer", "Greenhouse", None),
        ("Magic", "Tier4", "AI/Developer", "Greenhouse", None),
        ("Together AI", "Tier4", "AI Infrastructure", "Greenhouse", "togetherai"),
        ("Groq", "Tier4", "AI/Hardware", "Greenhouse", "groq"),
        ("Cerebras", "Tier4", "AI/Hardware", "Greenhouse", "cerebras"),
        ("SambaNova", "Tier4", "AI/Hardware", "Greenhouse", "sambanova"),
        ("d-Matrix", "Tier4", "AI/Hardware", "Greenhouse", None),
        ("Airtable", "Tier4", "No-Code/SaaS", "Greenhouse", "airtable"),
        ("Zapier", "Tier4", "Automation", "Greenhouse", "zapier"),
        ("UiPath", "Tier4", "Automation/AI", "Workday", None),
        ("C3.ai", "Tier4", "Enterprise AI", "Greenhouse", "c3-ai"),
        ("DataRobot", "Tier4", "AI/AutoML", "Greenhouse", "datarobot"),
        ("H2O.ai", "Tier4", "AI/AutoML", "Greenhouse", "h2o-ai"),
        ("Alteryx", "Tier4", "Analytics", "Custom", None),
        ("Sisense", "Tier4", "Analytics", "Greenhouse", "sisense"),
        ("Domo", "Tier4", "Analytics", "Workday", None),
        ("Firecrawl", "Tier4", "AI/Data", "Ashby", "firecrawl"),
        ("Anthropic", "Tier1", "AI Native", "Ashby", "anthropic"),
    ]

    # Deduplicate by canonical_name
    seen_canonical = set()
    for c in cur.execute("SELECT canonical_name FROM companies_200").fetchall():
        seen_canonical.add(c[0].lower())

    added = 0
    for name, tier, sector, ats, slug in COMPANIES:
        cn = name.lower().strip()
        if cn in seen_canonical:
            continue
        cur.execute("""
            INSERT INTO companies_200 (company_name, canonical_name, tier, sector,
                                       hq_country, ats_platform, ats_board_slug)
            VALUES (?, ?, ?, ?, 'US', ?, ?)
        """, (name, name, tier, sector, ats, slug))
        seen_canonical.add(cn)
        added += 1

    conn.commit()
    total = cur.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
    conn.close()
    log(f"build_companies: Added {added} companies. Total: {total}")
    if total != 200:
        log(f"  WARNING: Expected 200 companies, got {total}. Adjust list manually.")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: normalize_platforms
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_normalize_platforms():
    """Canonicalize source_platform names."""
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT DISTINCT source_platform FROM job_postings_gold"
    ).fetchall()
    updated = 0
    for r in rows:
        raw = r[0]
        canonical = canonicalize_platform(raw)
        if canonical != raw:
            cur.execute(
                "UPDATE job_postings_gold SET source_platform = ? WHERE source_platform = ?",
                (canonical, raw)
            )
            updated += cur.rowcount
            log(f"  {raw} → {canonical} ({cur.rowcount} rows)")
    conn.commit()
    conn.close()
    log(f"normalize_platforms: Updated {updated} rows.")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: mine_salary_from_body
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_mine_salary_from_body():
    """Parse salary regex from stored body_raw/description_snippet. FREE, no HTTP."""
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT gold_id, description_snippet FROM job_postings_gold
        WHERE salary_min_usd IS NULL AND description_snippet IS NOT NULL
              AND length(description_snippet) > 20
    """).fetchall()
    log(f"mine_salary_from_body: Scanning {len(rows)} rows...")
    mined = 0
    for r in rows:
        result = extract_salary(r['description_snippet'])
        if result and not result.get('skip'):
            cur.execute("""
                UPDATE job_postings_gold
                SET salary_min_usd = ?, salary_max_usd = ?,
                    salary_currency = ?, salary_period = ?, salary_text = ?
                WHERE gold_id = ?
            """, (result['salary_min_usd'], result['salary_max_usd'],
                  result['salary_currency'], result['salary_period'],
                  result['salary_text'], r['gold_id']))
            mined += 1
        elif result and result.get('skip'):
            # Non-USD detected
            cur.execute("""
                INSERT INTO qa_violations (gold_id, rule_name, severity, details)
                VALUES (?, 'non_usd_salary', 'WARNING', 'Non-USD currency detected in body')
            """, (r['gold_id'],))
    conn.commit()
    conn.close()
    log(f"mine_salary_from_body: Mined salary for {mined} rows.")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: backfill_title_ai
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_backfill_title_ai():
    """Compute has_ai_in_title + title_ai_terms for all rows."""
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute("SELECT gold_id, title FROM job_postings_gold").fetchall()
    updated = 0
    for r in rows:
        has_ai, terms = compute_title_ai_terms(r['title'])
        cur.execute("""
            UPDATE job_postings_gold
            SET has_ai_in_title = ?, title_ai_terms = ?
            WHERE gold_id = ?
        """, (has_ai, terms, r['gold_id']))
        updated += 1
    conn.commit()
    conn.close()
    log(f"backfill_title_ai: Updated {updated} rows.")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: backfill_ai_role_signature
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_backfill_ai_role_signature():
    """Classify ai_role_signature for all rows."""
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT gold_id, title, description_snippet, skills_extracted FROM job_postings_gold"
    ).fetchall()
    updated = 0
    for r in rows:
        sig = classify_ai_role_signature(
            r['title'], r['description_snippet'], r['skills_extracted']
        )
        cur.execute(
            "UPDATE job_postings_gold SET ai_role_signature = ? WHERE gold_id = ?",
            (sig, r['gold_id'])
        )
        updated += 1
    conn.commit()
    conn.close()
    log(f"backfill_ai_role_signature: Updated {updated} rows.")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: backfill_skills
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_backfill_skills():
    """Extract skills_extracted, has_python, has_sql from description_snippet."""
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT gold_id, description_snippet, skills_extracted FROM job_postings_gold"
    ).fetchall()
    updated = 0
    for r in rows:
        text = r['description_snippet'] or ''
        # Also use existing skills if present
        existing = r['skills_extracted'] or ''
        combined = f"{text} {existing}"
        skills, has_py, has_sql = extract_skills(combined)
        cur.execute("""
            UPDATE job_postings_gold
            SET skills_extracted = ?, has_python = ?, has_sql = ?
            WHERE gold_id = ?
        """, (skills or existing, has_py, has_sql, r['gold_id']))
        updated += 1
    conn.commit()
    conn.close()
    log(f"backfill_skills: Updated {updated} rows.")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: verify_and_enrich
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_verify_and_enrich():
    """Run all enrichment tiers in order."""
    import urllib.request
    import urllib.error

    conn = get_db()
    cur = conn.cursor()

    # PRE-STEP: mine salary from body (free)
    log("verify_and_enrich: PRE-STEP mine_salary_from_body")
    cmd_mine_salary_from_body()

    def _get_pending(platform_filter=None):
        """Get rows needing enrichment."""
        q = "SELECT * FROM job_postings_gold WHERE enrich_status IN ('pending','failed')"
        if platform_filter:
            q += f" AND (source_platform = '{platform_filter}' OR job_url LIKE '%{platform_filter}%')"
        return cur.execute(q).fetchall()

    def _http_get_json(url, headers=None, timeout=10):
        """Simple HTTP GET returning parsed JSON."""
        hdrs = headers or {}
        hdrs.setdefault('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)')
        hdrs.setdefault('Accept', 'application/json')
        req = urllib.request.Request(url, headers=hdrs)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode()), resp.status
        except urllib.error.HTTPError as e:
            return None, e.code
        except Exception:
            return None, 0

    def _http_get_html(url, timeout=10):
        """Simple HTTP GET returning HTML text."""
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode('utf-8', errors='replace'), resp.status
        except urllib.error.HTTPError as e:
            return None, e.code
        except Exception:
            return None, 0

    def _update_enriched(gid, updates: dict, status='api_enriched'):
        """Apply enrichment updates to a row."""
        updates['enrich_status'] = status
        updates['url_checked_at'] = datetime.now().isoformat()
        set_clause = ', '.join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [gid]
        cur.execute(f"UPDATE job_postings_gold SET {set_clause} WHERE gold_id = ?", vals)

    # ── TIER 1A: Greenhouse ──
    log("verify_and_enrich: TIER 1A — Greenhouse")
    gh_rows = _get_pending('greenhouse')
    enriched_gh = 0
    for r in gh_rows:
        url = r['job_url']
        # Parse slug and job_id
        m = re.search(r'greenhouse\.io/([^/]+)/jobs/(\d+)', url)
        if not m:
            m = re.search(r'boards\.greenhouse\.io/([^/]+)/jobs/(\d+)', url)
        if not m:
            # Try from URL with gh_jid param
            m2 = re.search(r'gh_jid=(\d+)', url)
            slug_m = re.search(r'greenhouse\.io/([^/]+)', url)
            if m2 and slug_m:
                slug, jid = slug_m.group(1), m2.group(1)
            else:
                continue
        else:
            slug, jid = m.group(1), m.group(2)

        api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{jid}?pay_transparency=true"
        data, status = _http_get_json(api_url)
        time.sleep(0.5)

        if status == 200 and data:
            updates = {'url_http_status': 200}
            loc = data.get('location', {})
            if isinstance(loc, dict) and loc.get('name'):
                updates['location_standardized'] = loc['name']
            # Pay transparency
            pay = data.get('pay_input_ranges', [])
            if pay and isinstance(pay, list) and len(pay) > 0:
                p = pay[0]
                if p.get('min_cents') and p.get('max_cents'):
                    min_usd = p['min_cents'] / 100
                    max_usd = p['max_cents'] / 100
                    curr = p.get('currency_type', 'USD')
                    if curr == 'USD' and 15000 <= min_usd <= 600000:
                        updates['salary_min_usd'] = int(min_usd)
                        updates['salary_max_usd'] = int(max_usd)
                        updates['salary_currency'] = 'USD'
                        updates['salary_period'] = 'Annual'
            # Work mode from metadata
            meta = data.get('metadata', [])
            for md in (meta or []):
                if md.get('name') == 'Location Type' and md.get('value'):
                    updates['work_mode'] = md['value']
            # Check if closed
            content = json.dumps(data).lower()
            if 'this job has been filled' in content or not data.get('title'):
                updates['status'] = 'Closed'

            _update_enriched(r['gold_id'], updates)
            enriched_gh += 1
        elif status in (404, 410):
            _update_enriched(r['gold_id'], {'url_http_status': status, 'status': 'Closed'})
    conn.commit()
    log(f"  Greenhouse: enriched {enriched_gh}/{len(gh_rows)}")

    # ── TIER 1B: Ashby ──
    log("verify_and_enrich: TIER 1B — Ashby")
    ashby_rows = _get_pending('ashby')
    enriched_ashby = 0
    for r in ashby_rows:
        url = r['job_url'].rstrip('/').replace('/application', '')
        m = re.search(r'ashbyhq\.com/([^/]+)/([^/]+)', url)
        if not m:
            continue
        slug = m.group(1)
        api_url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
        data, status = _http_get_json(api_url)
        time.sleep(0.5)

        if status == 200 and data:
            # Find matching job by UUID
            job_uuid = m.group(2)
            jobs = data.get('jobs', [])
            matched = None
            for j in jobs:
                if j.get('id') == job_uuid or job_uuid in str(j.get('id', '')):
                    matched = j
                    break
            if matched:
                updates = {'url_http_status': 200}
                if matched.get('location'):
                    updates['location_standardized'] = matched['location']
                wt = matched.get('workplaceType', '')
                if wt:
                    wt_map = {'Remote': 'Remote', 'Hybrid': 'Hybrid', 'OnSite': 'On-site'}
                    updates['work_mode'] = wt_map.get(wt, wt)
                comp = matched.get('compensationTierSummary', '')
                if comp:
                    sal = extract_salary(comp)
                    if sal and not sal.get('skip'):
                        updates.update({k: v for k, v in sal.items()})
                _update_enriched(r['gold_id'], updates)
                enriched_ashby += 1
    conn.commit()
    log(f"  Ashby: enriched {enriched_ashby}/{len(ashby_rows)}")

    # ── TIER 1C: Lever ──
    log("verify_and_enrich: TIER 1C — Lever")
    lever_rows = _get_pending('lever')
    enriched_lever = 0
    for r in lever_rows:
        m = re.search(r'lever\.co/([^/]+)/([a-f0-9-]+)', r['job_url'])
        if not m:
            continue
        slug, pid = m.group(1), m.group(2)
        api_url = f"https://api.lever.co/v0/postings/{slug}/{pid}?mode=json"
        data, status = _http_get_json(api_url)
        time.sleep(0.5)

        if status == 200 and data:
            updates = {'url_http_status': 200}
            cats = data.get('categories', {})
            if cats.get('location'):
                updates['location_raw'] = cats['location']
            wt = data.get('workplaceType', '')
            if wt:
                updates['work_mode'] = wt.capitalize()
            sal = data.get('salaryRange', {})
            if sal and sal.get('min') and sal.get('max'):
                currency = sal.get('currency', 'USD')
                if currency == 'USD':
                    interval = sal.get('interval', 'per-year')
                    min_v = sal['min']
                    max_v = sal['max']
                    if 'month' in interval:
                        min_v *= 12; max_v *= 12
                    elif 'hour' in interval:
                        min_v *= 2080; max_v *= 2080
                    if 15000 <= min_v <= 600000:
                        updates['salary_min_usd'] = int(min_v)
                        updates['salary_max_usd'] = int(max_v)
                        updates['salary_currency'] = 'USD'
                        updates['salary_period'] = 'Annual'
            created = data.get('createdAt')
            if created:
                updates['posted_date'] = datetime.fromtimestamp(created / 1000).strftime('%Y-%m-%d')
            _update_enriched(r['gold_id'], updates)
            enriched_lever += 1
        elif status in (404, 410):
            _update_enriched(r['gold_id'], {'url_http_status': status, 'status': 'Closed'})
    conn.commit()
    log(f"  Lever: enriched {enriched_lever}/{len(lever_rows)}")

    # ── TIER 2A: JSON-LD from job page ──
    log("verify_and_enrich: TIER 2A — JSON-LD extraction")
    pending = cur.execute("""
        SELECT * FROM job_postings_gold
        WHERE enrich_status IN ('pending', 'failed') AND job_url IS NOT NULL
    """).fetchall()
    enriched_ld = 0
    for r in pending[:100]:  # Batch limit to avoid timeout
        html, status = _http_get_html(r['job_url'])
        time.sleep(1.5)
        if status == 200 and html:
            updates = {'url_http_status': 200}
            # Parse JSON-LD
            ld_matches = re.findall(
                r'<script\s+type=["\']application/ld\+json["\']\s*>(.*?)</script>',
                html, re.DOTALL | re.IGNORECASE
            )
            for ld_text in ld_matches:
                try:
                    ld = json.loads(ld_text)
                    if isinstance(ld, list):
                        ld = next((x for x in ld if x.get('@type') == 'JobPosting'), None)
                    if not ld or ld.get('@type') != 'JobPosting':
                        continue
                    # Location
                    jl = ld.get('jobLocation', {})
                    if isinstance(jl, dict):
                        addr = jl.get('address', {})
                        if isinstance(addr, dict):
                            if addr.get('addressLocality'):
                                updates['location_city'] = addr['addressLocality']
                            if addr.get('addressRegion'):
                                updates['location_state'] = addr['addressRegion']
                            if addr.get('addressCountry'):
                                country_val = addr['addressCountry']
                                if isinstance(country_val, dict):
                                    country_val = country_val.get('name', '')
                                updates['country'] = country_val
                    # Remote
                    if ld.get('jobLocationType') == 'TELECOMMUTE':
                        updates['work_mode'] = 'Remote'
                    # Date
                    if ld.get('datePosted'):
                        updates['posted_date'] = str(ld['datePosted'])[:10]
                    # Salary
                    bs = ld.get('baseSalary', {})
                    if isinstance(bs, dict):
                        val = bs.get('value', {})
                        if isinstance(val, dict) and val.get('minValue') and val.get('maxValue'):
                            curr = bs.get('currency', 'USD')
                            if curr == 'USD':
                                min_v = float(val['minValue'])
                                max_v = float(val['maxValue'])
                                unit = val.get('unitText', 'YEAR')
                                if 'HOUR' in unit.upper():
                                    min_v *= 2080; max_v *= 2080
                                if 15000 <= min_v <= 600000:
                                    updates['salary_min_usd'] = int(min_v)
                                    updates['salary_max_usd'] = int(max_v)
                                    updates['salary_currency'] = 'USD'
                                    updates['salary_period'] = 'Annual'
                    # Description snippet
                    desc = ld.get('description', '')
                    if desc and not r['description_snippet']:
                        # Strip HTML tags
                        clean = re.sub(r'<[^>]+>', ' ', desc)
                        clean = re.sub(r'\s+', ' ', clean).strip()[:500]
                        updates['description_snippet'] = clean
                    break
                except (json.JSONDecodeError, KeyError):
                    continue

            if len(updates) > 1:
                _update_enriched(r['gold_id'], updates)
                enriched_ld += 1
            else:
                _update_enriched(r['gold_id'], {'url_http_status': status}, 'pending')
        elif status in (404, 410):
            _update_enriched(r['gold_id'], {'url_http_status': status, 'status': 'Closed'}, 'failed')
    conn.commit()
    log(f"  JSON-LD: enriched {enriched_ld}/{len(pending[:100])}")

    # ── TIER 2B: Salary from description regex (already done in mine_salary_from_body) ──
    log("verify_and_enrich: TIER 2B — salary regex (already done in pre-step)")

    # ── Final summary ──
    stats = cur.execute("""
        SELECT enrich_status, COUNT(*) as cnt
        FROM job_postings_gold GROUP BY enrich_status
    """).fetchall()
    log("verify_and_enrich COMPLETE. Status distribution:")
    for s in stats:
        log(f"  {s['enrich_status']}: {s['cnt']}")

    sal_count = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE salary_min_usd IS NOT NULL AND is_us=1"
    ).fetchone()[0]
    total = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]
    log(f"  Salary coverage: {sal_count}/{total} ({100*sal_count//max(total,1)}%)")

    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: qa_check
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_qa_check():
    """Run all quality gates → populate qa_violations table → print summary."""
    conn = get_db()
    cur = conn.cursor()

    # Clear previous violations
    cur.execute("DELETE FROM qa_violations")

    critical = 0
    warning = 0

    def _add_violation(gold_id, rule, severity, details=''):
        nonlocal critical, warning
        cur.execute("""
            INSERT INTO qa_violations (gold_id, rule_name, severity, details)
            VALUES (?, ?, ?, ?)
        """, (gold_id, rule, severity, details))
        if severity == 'CRITICAL':
            critical += 1
        else:
            warning += 1

    # ── CRITICAL checks ──

    # row_count_below_300
    active_count = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]
    if active_count < 300:
        _add_violation(None, 'row_count_below_300', 'CRITICAL',
                       f'Active US rows: {active_count} (need 300+)')

    # companies_200_count_wrong
    comp_count = cur.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
    if comp_count != 200:
        _add_violation(None, 'companies_200_count_wrong', 'CRITICAL',
                       f'companies_200 has {comp_count} rows (need exactly 200)')

    # duplicate_job_key
    dupes = cur.execute("""
        SELECT canonical_job_key, COUNT(*) as cnt
        FROM job_postings_gold GROUP BY canonical_job_key HAVING cnt > 1
    """).fetchall()
    for d in dupes:
        _add_violation(None, 'duplicate_job_key', 'CRITICAL',
                       f'Key {d["canonical_job_key"][:16]}... appears {d["cnt"]} times')

    # Per-row checks
    rows = cur.execute("SELECT * FROM job_postings_gold WHERE status = 'Open'").fetchall()
    for r in rows:
        gid = r['gold_id']

        # required_field_null
        for field in ['company_name', 'source_platform', 'source_job_id',
                      'job_url', 'title', 'title_normalized', 'role_cluster',
                      'canonical_job_key']:
            if not r[field]:
                _add_violation(gid, 'required_field_null', 'CRITICAL',
                               f'{field} is NULL')

        # not_us
        if not r['is_us'] and r['status'] == 'Open':
            _add_violation(gid, 'not_us', 'CRITICAL',
                           f'is_us=0 on active row')

        # url_not_reachable
        if r['url_http_status'] and r['url_http_status'] not in (200, 301, 302):
            _add_violation(gid, 'url_not_reachable', 'CRITICAL',
                           f'HTTP {r["url_http_status"]}')

        # no_ai_signal
        kw = r['ai_keywords_hit']
        if not kw or kw in ('', '[]', 'null'):
            _add_violation(gid, 'no_ai_signal', 'CRITICAL',
                           'ai_keywords_hit empty')

        # role_excluded
        if is_role_excluded(r['title']):
            _add_violation(gid, 'role_excluded', 'CRITICAL',
                           f'Title matches exclusion: {r["title"]}')

        # unknown_company (new) — WARNING for isolated cases, escalated later
        if r['company_name'] == 'Unknown':
            _add_violation(gid, 'unknown_company', 'WARNING',
                           f'company_name is Unknown, URL: {r["job_url"][:60]}')

        # html_in_snippet (new)
        snippet = r['description_snippet'] or ''
        if '<' in snippet and '>' in snippet:
            _add_violation(gid, 'html_in_snippet', 'WARNING',
                           'description_snippet contains HTML tags')

        # aggregator_url (new)
        if is_aggregator_url(r['job_url']):
            _add_violation(gid, 'aggregator_url', 'CRITICAL',
                           f'Aggregator URL: {r["job_url"][:80]}')

        # work_mode_contradiction (new)
        loc_text = f"{r['location_raw'] or ''} {r['location_standardized'] or ''}".lower()
        if r['work_mode'] == 'On-site' and any(re.search(p, loc_text) for p in REMOTE_PATTERNS):
            _add_violation(gid, 'work_mode_contradiction', 'WARNING',
                           f'work_mode=On-site but location has Remote')

        # date_out_of_window
        pd = r['posted_date']
        if pd and not r['date_uncertain']:
            try:
                d = datetime.strptime(pd[:10], '%Y-%m-%d').date()
                if d < date(2025, 7, 1) or d > date(2026, 3, 31):
                    _add_violation(gid, 'date_out_of_window', 'CRITICAL',
                                   f'posted_date={pd}')
            except ValueError:
                pass

        # ── WARNING checks ──

        # salary sanity
        if r['salary_min_usd']:
            if r['salary_min_usd'] < 15000 or (r['salary_max_usd'] and r['salary_max_usd'] > 600000):
                _add_violation(gid, 'salary_insane', 'WARNING',
                               f'${r["salary_min_usd"]}-${r["salary_max_usd"]}')
            if r['salary_max_usd'] and r['salary_min_usd'] > 0:
                if r['salary_max_usd'] / r['salary_min_usd'] > 5:
                    _add_violation(gid, 'salary_insane', 'WARNING',
                                   f'max/min ratio > 5')

        # missing_description
        ds = r['description_snippet']
        if not ds or len(ds) < 50:
            _add_violation(gid, 'missing_description', 'WARNING',
                           'description_snippet NULL or < 50 chars')

    # unknown_company_high_count — CRITICAL if 5+ unknown companies
    unknown_count = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE company_name='Unknown' AND status='Open'"
    ).fetchone()[0]
    if unknown_count >= 5:
        _add_violation(None, 'unknown_company_high_count', 'CRITICAL',
                       f'{unknown_count} rows with company_name=Unknown')

    # date_uncertain_high_ratio
    uncertain = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE date_uncertain=1 AND status='Open'"
    ).fetchone()[0]
    if active_count > 0 and uncertain / active_count > 0.3:
        _add_violation(None, 'date_uncertain_high_ratio', 'WARNING',
                       f'{uncertain}/{active_count} = {100*uncertain//active_count}%')

    conn.commit()
    conn.close()

    log(f"qa_check COMPLETE: {critical} CRITICAL, {warning} WARNING")
    return critical, warning


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: export_review
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_export_review():
    """Export all CSVs to review/ + qa_report.json + random_spot_check_30.csv."""
    REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_db()
    cur = conn.cursor()

    # Export job_postings_gold.csv
    rows = cur.execute("SELECT * FROM job_postings_gold ORDER BY gold_id").fetchall()
    _export_csv(rows, REVIEW_DIR / "job_postings_gold.csv")
    log(f"  Exported {len(rows)} rows to job_postings_gold.csv")

    # Export companies_200.csv
    companies = cur.execute("SELECT * FROM companies_200 ORDER BY company_id").fetchall()
    _export_csv(companies, REVIEW_DIR / "companies_200.csv")
    log(f"  Exported {len(companies)} rows to companies_200.csv")

    # Export qa_violations.csv
    violations = cur.execute("SELECT * FROM qa_violations ORDER BY violation_id").fetchall()
    _export_csv(violations, REVIEW_DIR / "qa_violations.csv")
    log(f"  Exported {len(violations)} violations to qa_violations.csv")

    # Export source_attempts.csv
    attempts = cur.execute("SELECT * FROM source_attempts ORDER BY attempt_id").fetchall()
    _export_csv(attempts, REVIEW_DIR / "source_attempts.csv")

    # Random spot check
    active_rows = cur.execute(
        "SELECT gold_id, company_name, title, job_url, source_platform, salary_min_usd, salary_max_usd "
        "FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchall()
    sample_size = min(30, len(active_rows))
    spot_check = random.sample(active_rows, sample_size) if active_rows else []
    _export_csv(spot_check, REVIEW_DIR / "random_spot_check_30.csv")
    log(f"  Exported {sample_size} rows to random_spot_check_30.csv")

    # QA Report JSON
    report = _build_qa_report(cur)
    with open(REVIEW_DIR / "qa_report.json", 'w') as f:
        json.dump(report, f, indent=2, default=str)
    log(f"  Exported qa_report.json")

    conn.close()
    log("export_review COMPLETE.")


def _export_csv(rows, path):
    """Export sqlite3.Row list to CSV."""
    if not rows:
        with open(path, 'w') as f:
            f.write('')
        return
    keys = rows[0].keys()
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow(dict(r))


def _build_qa_report(cur) -> dict:
    """Build the comprehensive qa_report.json."""
    report = {}
    report['generated_at'] = datetime.now().isoformat()

    # Row counts
    report['total_gold_rows'] = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    report['active_us_rows'] = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]
    report['excluded_non_us'] = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE status='Excluded-NonUS'"
    ).fetchone()[0]
    report['closed_rows'] = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE status='Closed'"
    ).fetchone()[0]

    active = report['active_us_rows'] or 1
    sal = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE salary_min_usd IS NOT NULL AND is_us=1 AND status='Open'"
    ).fetchone()[0]
    report['pct_with_salary'] = round(100 * sal / active, 1)

    ai_title = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE has_ai_in_title=1 AND is_us=1 AND status='Open'"
    ).fetchone()[0]
    report['pct_with_ai_in_title'] = round(100 * ai_title / active, 1)

    uncertain = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE date_uncertain=1 AND is_us=1 AND status='Open'"
    ).fetchone()[0]
    report['pct_date_uncertain'] = round(100 * uncertain / active, 1)

    # Enrich status distribution
    report['enrich_status_distribution'] = {}
    for r in cur.execute(
        "SELECT enrich_status, COUNT(*) as cnt FROM job_postings_gold GROUP BY enrich_status"
    ).fetchall():
        report['enrich_status_distribution'][r['enrich_status']] = r['cnt']

    # Top role clusters
    report['top_role_clusters'] = {}
    for r in cur.execute("""
        SELECT role_cluster, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' GROUP BY role_cluster ORDER BY cnt DESC LIMIT 10
    """).fetchall():
        report['top_role_clusters'][r['role_cluster']] = r['cnt']

    # Top companies
    report['top_companies'] = {}
    for r in cur.execute("""
        SELECT company_name, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' GROUP BY company_name ORDER BY cnt DESC LIMIT 20
    """).fetchall():
        report['top_companies'][r['company_name']] = r['cnt']

    # Top AI keywords
    report['top_ai_keywords'] = _count_csv_field(cur, 'ai_keywords_hit', 25)
    report['top_title_ai_terms'] = _count_csv_field(cur, 'title_ai_terms', 15)
    report['top_ai_role_signatures'] = {}
    for r in cur.execute("""
        SELECT ai_role_signature, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' AND ai_role_signature IS NOT NULL
        GROUP BY ai_role_signature ORDER BY cnt DESC
    """).fetchall():
        report['top_ai_role_signatures'][r['ai_role_signature']] = r['cnt']

    # Violations
    report['critical_violation_count'] = cur.execute(
        "SELECT COUNT(*) FROM qa_violations WHERE severity='CRITICAL'"
    ).fetchone()[0]
    report['warning_violation_count'] = cur.execute(
        "SELECT COUNT(*) FROM qa_violations WHERE severity='WARNING'"
    ).fetchone()[0]

    # Companies represented
    report['companies_represented'] = cur.execute("""
        SELECT COUNT(DISTINCT company_name) FROM job_postings_gold
        WHERE is_us=1 AND status='Open'
    """).fetchone()[0]

    # Source platform distribution
    report['source_platform_distribution'] = {}
    for r in cur.execute("""
        SELECT source_platform, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' GROUP BY source_platform ORDER BY cnt DESC
    """).fetchall():
        report['source_platform_distribution'][r['source_platform']] = r['cnt']

    # Work mode distribution
    report['work_mode_distribution'] = {}
    for r in cur.execute("""
        SELECT work_mode, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' GROUP BY work_mode ORDER BY cnt DESC
    """).fetchall():
        report['work_mode_distribution'][r['work_mode'] or 'Unknown'] = r['cnt']

    # Window bucket distribution
    report['window_bucket_distribution'] = {}
    for r in cur.execute("""
        SELECT window_bucket, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' GROUP BY window_bucket ORDER BY cnt DESC
    """).fetchall():
        report['window_bucket_distribution'][r['window_bucket'] or 'UNCERTAIN'] = r['cnt']

    # Approval state
    approval = cur.execute("SELECT * FROM approval_state ORDER BY id DESC LIMIT 1").fetchone()
    if approval:
        report['approval_state'] = {
            'approved': bool(approval['approved_by_user']),
            'approved_at': approval['approved_at'],
            'row_count_at_approval': approval['row_count_at_approval'],
        }
    else:
        report['approval_state'] = {'approved': False}

    return report


def _count_csv_field(cur, field: str, top_n: int) -> dict:
    """Count frequency of comma-separated values in a field."""
    counts = {}
    rows = cur.execute(f"""
        SELECT {field} FROM job_postings_gold
        WHERE is_us=1 AND status='Open' AND {field} IS NOT NULL AND {field} != ''
    """).fetchall()
    for r in rows:
        val = r[0]
        if not val:
            continue
        # Handle JSON arrays
        if val.startswith('['):
            try:
                items = json.loads(val)
            except json.JSONDecodeError:
                items = [v.strip() for v in val.split(',')]
        else:
            items = [v.strip() for v in val.split(',')]
        for item in items:
            item = item.strip().strip('"').strip("'")
            if item:
                counts[item] = counts.get(item, 0) + 1
    # Sort and return top N
    sorted_items = sorted(counts.items(), key=lambda x: -x[1])[:top_n]
    return dict(sorted_items)


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: approve_db
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_approve_db():
    """Inline qa_check → block if CRITICAL > 0 → insert approval_state row."""
    critical, warning = cmd_qa_check()

    if critical > 0:
        log(f"approve_db: BLOCKED — {critical} CRITICAL violations found.")
        log("  Fix all CRITICAL violations before approving.")
        log("  Run: python scripts/job_db_pipeline.py export_review")
        log("  Then review review/qa_violations.csv")
        return False

    conn = get_db()
    cur = conn.cursor()
    active = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]

    cur.execute("DELETE FROM approval_state")
    cur.execute("""
        INSERT INTO approval_state (id, approved_by_user, approved_at,
                                    row_count_at_approval, critical_violations,
                                    warning_violations)
        VALUES (1, 1, ?, ?, 0, ?)
    """, (datetime.now().isoformat(), active, warning))
    conn.commit()
    conn.close()

    log(f"approve_db: APPROVED with {active} active rows, {warning} warnings.")
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: analyze_approved (Phase 2)
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_analyze_approved():
    """HARD BLOCKED until approval_state.approved_by_user=1; runs full Phase 2."""
    conn = get_db()
    cur = conn.cursor()

    approval = cur.execute(
        "SELECT approved_by_user FROM approval_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not approval or not approval['approved_by_user']:
        log("analyze_approved: HARD BLOCKED — DB not approved.")
        log("  Run: python scripts/job_db_pipeline.py approve_db")
        conn.close()
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build QA report data
    report = _build_qa_report(cur)

    # ── Generate Dashboard HTML ──
    log("analyze_approved: Generating dashboard...")
    _generate_dashboard(cur, report)

    # ── Generate Markdown Report ──
    log("analyze_approved: Generating markdown report...")
    _generate_markdown_report(cur, report)

    conn.close()
    log("analyze_approved COMPLETE.")
    log(f"  Dashboard: {OUTPUT_DIR / 'dashboard.html'}")
    log(f"  Report:    {OUTPUT_DIR / 'AI_Analyst_Roles_Research_2026.md'}")


def _generate_dashboard(cur, report: dict):
    """Generate Plotly interactive dashboard HTML."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
        import plotly.io as pio
    except ImportError:
        log("  WARNING: plotly not installed. Run: pip install plotly")
        log("  Skipping dashboard generation.")
        return

    figs = []

    # 1. Role cluster distribution
    rc = report.get('top_role_clusters', {})
    if rc:
        fig1 = go.Figure(go.Bar(x=list(rc.values()), y=list(rc.keys()),
                                orientation='h', marker_color='#4C78A8'))
        fig1.update_layout(title='Role Cluster Distribution', xaxis_title='Count',
                          height=500, margin=dict(l=200))
        figs.append(fig1)

    # 2. Work mode distribution
    wm = report.get('work_mode_distribution', {})
    if wm:
        fig2 = go.Figure(go.Bar(x=list(wm.keys()), y=list(wm.values()),
                                marker_color='#72B7B2'))
        fig2.update_layout(title='Work Mode Distribution', yaxis_title='Count')
        figs.append(fig2)

    # 3. Posting volume by time bucket
    wb = report.get('window_bucket_distribution', {})
    if wb:
        fig3 = go.Figure(go.Bar(x=list(wb.keys()), y=list(wb.values()),
                                marker_color='#F58518'))
        fig3.update_layout(title='Posting Volume by Time Bucket', yaxis_title='Count')
        figs.append(fig3)

    # 4. Top 20 companies
    tc = report.get('top_companies', {})
    if tc:
        fig4 = go.Figure(go.Bar(x=list(tc.values()), y=list(tc.keys()),
                                orientation='h', marker_color='#E45756'))
        fig4.update_layout(title='Top 20 Companies by Posting Count',
                          xaxis_title='Count', height=600, margin=dict(l=200))
        figs.append(fig4)

    # 5. AI keyword frequency
    ak = report.get('top_ai_keywords', {})
    if ak:
        fig5 = go.Figure(go.Bar(x=list(ak.values()), y=list(ak.keys()),
                                orientation='h', marker_color='#54A24B'))
        fig5.update_layout(title='Top AI Keywords from Descriptions',
                          xaxis_title='Frequency', height=700, margin=dict(l=200))
        figs.append(fig5)

    # 6. Title AI terms
    tt = report.get('top_title_ai_terms', {})
    if tt:
        fig6 = go.Figure(go.Bar(x=list(tt.values()), y=list(tt.keys()),
                                orientation='h', marker_color='#B279A2'))
        fig6.update_layout(title='Title AI Terms Frequency',
                          xaxis_title='Frequency', height=500, margin=dict(l=150))
        figs.append(fig6)

    # 7. Salary ranges box plot
    salary_data = cur.execute("""
        SELECT role_cluster, salary_min_usd, salary_max_usd
        FROM job_postings_gold
        WHERE salary_min_usd IS NOT NULL AND is_us=1 AND status='Open'
    """).fetchall()
    if salary_data:
        fig7 = go.Figure()
        clusters_sal = {}
        for r in salary_data:
            cl = r['role_cluster']
            if cl not in clusters_sal:
                clusters_sal[cl] = []
            clusters_sal[cl].extend([r['salary_min_usd'], r['salary_max_usd']])
        for cl, vals in sorted(clusters_sal.items()):
            fig7.add_trace(go.Box(y=vals, name=cl))
        fig7.update_layout(title='Salary Ranges by Role Cluster ($USD)',
                          yaxis_title='Annual Salary (USD)', height=500)
        figs.append(fig7)

    # 8. Top skills
    skills_data = _count_csv_field(cur, 'skills_extracted', 25)
    if skills_data:
        fig8 = go.Figure(go.Bar(x=list(skills_data.values()),
                                y=list(skills_data.keys()),
                                orientation='h', marker_color='#FF9DA6'))
        fig8.update_layout(title='Top 25 Skills', xaxis_title='Frequency',
                          height=700, margin=dict(l=150))
        figs.append(fig8)

    # 9. Source platform distribution
    sp = report.get('source_platform_distribution', {})
    if sp:
        fig9 = go.Figure(go.Bar(x=list(sp.keys()), y=list(sp.values()),
                                marker_color='#9D755D'))
        fig9.update_layout(title='Source Platform Distribution', yaxis_title='Count')
        figs.append(fig9)

    # 10. ai_role_signature donut
    ars = report.get('top_ai_role_signatures', {})
    if ars:
        fig10 = go.Figure(go.Pie(labels=list(ars.keys()), values=list(ars.values()),
                                 hole=0.4))
        fig10.update_layout(title='AI Role Signature Distribution')
        figs.append(fig10)

    # Combine into single HTML
    html_parts = [f"""<!DOCTYPE html>
<html><head><title>AI Analyst Jobs Dashboard 2026</title>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 20px; background: #f8f9fa; }}
.header {{ background: #1a1a2e; color: white; padding: 30px; border-radius: 12px; margin-bottom: 30px; }}
.stats {{ display: flex; gap: 20px; flex-wrap: wrap; margin-top: 15px; }}
.stat {{ background: rgba(255,255,255,0.1); padding: 15px 25px; border-radius: 8px; }}
.stat-val {{ font-size: 28px; font-weight: bold; }}
.stat-label {{ font-size: 12px; opacity: 0.8; }}
.chart {{ background: white; border-radius: 12px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
</style></head><body>
<div class="header">
<h1>AI Analyst Roles — US Job Market Research 2026</h1>
<div class="stats">
<div class="stat"><div class="stat-val">{report['active_us_rows']}</div><div class="stat-label">Active US Postings</div></div>
<div class="stat"><div class="stat-val">{report['companies_represented']}</div><div class="stat-label">Companies</div></div>
<div class="stat"><div class="stat-val">{report['pct_with_salary']}%</div><div class="stat-label">With Salary</div></div>
<div class="stat"><div class="stat-val">{report['pct_with_ai_in_title']}%</div><div class="stat-label">AI in Title</div></div>
</div></div>"""]

    for i, fig in enumerate(figs):
        div_id = f"chart_{i}"
        fig_json = pio.to_json(fig)
        html_parts.append(f"""
<div class="chart"><div id="{div_id}"></div></div>
<script>Plotly.newPlot('{div_id}', {fig_json}.data, {fig_json}.layout, {{responsive: true}});</script>
""")

    html_parts.append("</body></html>")

    with open(OUTPUT_DIR / "dashboard.html", 'w') as f:
        f.write('\n'.join(html_parts))
    log(f"  Dashboard written: {OUTPUT_DIR / 'dashboard.html'}")


def _generate_markdown_report(cur, report: dict):
    """Generate the 10-section markdown research report."""
    sections = []

    # 1. Executive Summary
    sections.append(f"""# AI Analyst Roles — US Job Market Research 2026

## 1. Executive Summary

- **{report['active_us_rows']} active US job postings** across {report['companies_represented']} companies
- **{report['pct_with_salary']}% salary coverage** with data from ATS APIs and pay transparency disclosures
- **{report['pct_with_ai_in_title']}% of postings** have AI/LLM terms directly in the job title
- Top role clusters: {', '.join(list(report.get('top_role_clusters', {}).keys())[:5])}
- Sources: {', '.join(f"{k}({v})" for k, v in list(report.get('source_platform_distribution', {}).items())[:5])}
""")

    # 2. Dataset Overview
    sections.append(f"""## 2. Dataset Overview

| Metric | Value |
|--------|-------|
| Total gold rows | {report['total_gold_rows']} |
| Active US rows | {report['active_us_rows']} |
| Excluded (non-US) | {report['excluded_non_us']} |
| Closed rows | {report['closed_rows']} |
| With salary | {report['pct_with_salary']}% |
| AI in title | {report['pct_with_ai_in_title']}% |
| Date uncertain | {report['pct_date_uncertain']}% |
| Critical violations | {report['critical_violation_count']} |
| Warning violations | {report['warning_violation_count']} |
""")

    # 3-10 abbreviated for now — structure is in place
    rc = report.get('top_role_clusters', {})
    sections.append(f"""## 3. Role Family Landscape

| Role Cluster | Count |
|-------------|-------|
""" + '\n'.join(f"| {k} | {v} |" for k, v in rc.items()))

    ars = report.get('top_ai_role_signatures', {})
    sections.append(f"""## 4. AI/LLM Signal Analysis

| AI Role Signature | Count |
|------------------|-------|
""" + '\n'.join(f"| {k} | {v} |" for k, v in ars.items()))

    sections.append("""## 5. Compensation Benchmarks

See dashboard charts for interactive salary range analysis by cluster and tier.
""")

    wm = report.get('work_mode_distribution', {})
    sections.append(f"""## 6. Work Model Distribution

| Work Mode | Count |
|-----------|-------|
""" + '\n'.join(f"| {k} | {v} |" for k, v in wm.items()))

    tc = report.get('top_companies', {})
    sections.append(f"""## 7. Top Employers & Tier Analysis

| Company | Postings |
|---------|----------|
""" + '\n'.join(f"| {k} | {v} |" for k, v in list(tc.items())[:20]))

    sections.append("""## 8. Skills Landscape

See dashboard for interactive top-25 skills chart.
""")

    sections.append("""## 9. Emerging AI Title Patterns

See AI Role Signature analysis in Section 4 for emerging title patterns.
""")

    es = report.get('enrich_status_distribution', {})
    sections.append(f"""## 10. Methodology & Coverage Gaps

| Enrich Status | Count |
|--------------|-------|
""" + '\n'.join(f"| {k} | {v} |" for k, v in es.items()) + f"""

**Pipeline**: Two-source merge (Claude DB + Codex DB) → dedup → ATS API enrichment → JSON-LD → salary regex → QA gates → approval
**Date window**: 2025-07-01 to 2026-03-31
**Company scope**: 200 US big-tech & AI companies
""")

    md = '\n\n'.join(sections)
    with open(OUTPUT_DIR / "AI_Analyst_Roles_Research_2026.md", 'w') as f:
        f.write(md)
    log(f"  Report written: {OUTPUT_DIR / 'AI_Analyst_Roles_Research_2026.md'}")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: fix_data_quality
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_fix_data_quality():
    """Run all 11 data quality fixes in correct dependency order.

    Steps:
    1. DELETE non-US rows (is_us=0)
    2. DELETE aggregator URLs (builtin, theladders, etc.)
    3. Fix company_name='Unknown' from URL slug
    4. Strip HTML from description_snippet
    5. Re-extract skills_extracted with fixed regex
    6. Build title_normalized taxonomy (15-segment)
    7. Fix work_mode / location contradictions
    8. Fix salary_text corruption
    9. Re-compute posted_date date_uncertain flag (no ATS re-call)
    10. Classify Applied Scientist keep/remove
    11. Add in_target_list column
    """
    conn = get_db()
    cur = conn.cursor()

    # ── Step 1: DELETE non-US rows ──
    log("Step 1: Deleting non-US rows (is_us=0)...")
    non_us = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us = 0"
    ).fetchone()[0]
    cur.execute("DELETE FROM job_postings_gold WHERE is_us = 0")
    conn.commit()
    log(f"  Deleted {non_us} non-US rows")

    # ── Step 2: DELETE aggregator URLs ──
    log("Step 2: Deleting aggregator URLs...")
    agg_conditions = ' OR '.join(
        f"job_url LIKE '%{d}%'" for d in [
            'builtin.com', 'builtinnyc.com', 'builtinsf.com',
            'builtinchicago.com', 'builtinaustin.com', 'builtinboston.com',
            'builtincolorado.com', 'builtinla.com', 'builtinseattle.com',
            'theladders.com', 'themuse.com', 'towardsai.net',
            'wallstreetcareers.com', 'datasciencessjobs.com', 'technyjobs.com',
            'wellfound.com', 'angel.co',
        ]
    )
    before = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    cur.execute(f"DELETE FROM job_postings_gold WHERE {agg_conditions}")
    after = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    conn.commit()
    log(f"  Deleted {before - after} aggregator rows (was {before}, now {after})")

    # ── Step 3: Fix company_name='Unknown' from URL slug ──
    log("Step 3: Fixing company_name='Unknown' from URL slugs...")
    unknown_rows = cur.execute(
        "SELECT gold_id, job_url FROM job_postings_gold WHERE company_name = 'Unknown'"
    ).fetchall()
    fixed_companies = 0
    for r in unknown_rows:
        gid, url = r[0], r[1]
        name = extract_company_from_url(url)
        if name and name.lower() != 'unknown':
            cur.execute(
                "UPDATE job_postings_gold SET company_name = ? WHERE gold_id = ?",
                (name, gid)
            )
            fixed_companies += 1
    conn.commit()
    log(f"  Fixed {fixed_companies}/{len(unknown_rows)} Unknown company names")

    # ── Step 4: Strip HTML from description_snippet ──
    log("Step 4: Stripping HTML from description_snippet...")
    html_rows = cur.execute(
        "SELECT gold_id, description_snippet FROM job_postings_gold "
        "WHERE description_snippet LIKE '%<%' AND description_snippet IS NOT NULL"
    ).fetchall()
    for r in html_rows:
        gid, snippet = r[0], r[1]
        cleaned = strip_html(snippet)
        if len(cleaned) > 500:
            cleaned = cleaned[:500]
        cur.execute(
            "UPDATE job_postings_gold SET description_snippet = ? WHERE gold_id = ?",
            (cleaned, gid)
        )
    conn.commit()
    log(f"  Cleaned HTML from {len(html_rows)} description_snippets")

    # ── Step 5: Re-extract skills with fixed regex ──
    log("Step 5: Re-extracting skills with fixed R regex...")
    all_rows = cur.execute(
        "SELECT gold_id, description_snippet, title FROM job_postings_gold"
    ).fetchall()
    skills_fixed = 0
    for r in all_rows:
        gid = r[0]
        text = f"{r[2] or ''} {r[1] or ''}"
        new_skills, hp, hs = extract_skills(text)
        cur.execute(
            "UPDATE job_postings_gold SET skills_extracted=?, has_python=?, has_sql=? "
            "WHERE gold_id=?",
            (new_skills, hp, hs, gid)
        )
        skills_fixed += 1
    conn.commit()
    log(f"  Re-extracted skills for {skills_fixed} rows")

    # ── Step 6: Build title_normalized taxonomy ──
    log("Step 6: Normalizing title_normalized with taxonomy...")
    title_rows = cur.execute(
        "SELECT gold_id, title FROM job_postings_gold"
    ).fetchall()
    title_fixed = 0
    for r in title_rows:
        gid, title = r[0], r[1]
        norm = normalize_title_to_segment(title)
        if norm != title:
            title_fixed += 1
        cur.execute(
            "UPDATE job_postings_gold SET title_normalized = ? WHERE gold_id = ?",
            (norm, gid)
        )
    conn.commit()
    distinct = cur.execute(
        "SELECT COUNT(DISTINCT title_normalized) FROM job_postings_gold"
    ).fetchone()[0]
    log(f"  Normalized {title_fixed} titles → {distinct} distinct segments")

    # ── Step 7: Fix work_mode / location contradictions ──
    log("Step 7: Fixing work_mode / location contradictions...")
    wm_rows = cur.execute(
        "SELECT gold_id, work_mode, location_raw, location_standardized "
        "FROM job_postings_gold"
    ).fetchall()
    wm_fixed = 0
    for r in wm_rows:
        gid = r[0]
        old_wm = r[1] or 'Unknown'
        new_wm = resolve_work_mode(old_wm, r[2], r[3])
        if new_wm != old_wm:
            cur.execute(
                "UPDATE job_postings_gold SET work_mode = ? WHERE gold_id = ?",
                (new_wm, gid)
            )
            wm_fixed += 1
    conn.commit()
    log(f"  Fixed {wm_fixed} work_mode contradictions")

    # ── Step 8: Fix salary_text corruption ──
    log("Step 8: Fixing salary_text corruption...")
    corrupt = cur.execute("""
        SELECT gold_id, salary_text, salary_min_usd, salary_max_usd
        FROM job_postings_gold
        WHERE salary_text IS NOT NULL
          AND salary_max_usd IS NOT NULL
          AND LENGTH(salary_text) > 0
          AND salary_text LIKE '%1'
          AND CAST(salary_max_usd AS TEXT) != ''
    """).fetchall()
    sal_fixed = 0
    for r in corrupt:
        gid, st, smin, smax = r[0], r[1], r[2], r[3]
        if st and smax and st.endswith('1') and str(int(smax)) + '1' in st.replace(',', '').replace(' ', ''):
            # Likely corruption: salary_text has has_ai_in_title=1 appended
            clean_st = st[:-1]  # remove trailing "1"
            # Reformat nicely
            if smin and smax:
                clean_st = f"${int(smin):,} - ${int(smax):,}"
            cur.execute(
                "UPDATE job_postings_gold SET salary_text = ? WHERE gold_id = ?",
                (clean_st, gid)
            )
            sal_fixed += 1
    conn.commit()
    log(f"  Fixed {sal_fixed} corrupted salary_text values")

    # ── Step 9: Re-check date_uncertain for rows with enriched posted_date ──
    log("Step 9: Rechecking date_uncertain flags...")
    # Only clear date_uncertain for rows that have been successfully enriched
    # (enrich_status != 'pending'), since those dates came from ATS APIs
    date_rows = cur.execute(
        "SELECT gold_id, posted_date, date_uncertain, enrich_status FROM job_postings_gold "
        "WHERE posted_date IS NOT NULL AND posted_date != '' AND date_uncertain = 1"
    ).fetchall()
    date_fixed = 0
    for r in date_rows:
        gid, pd, du, es = r[0], r[1], r[2], r[3]
        # Only mark certain if enrichment confirmed the date
        if es and es not in ('pending', 'failed', 'skipped'):
            try:
                datetime.strptime(pd[:10], '%Y-%m-%d')
                cur.execute(
                    "UPDATE job_postings_gold SET date_uncertain = 0 WHERE gold_id = ?",
                    (gid,)
                )
                date_fixed += 1
            except (ValueError, TypeError):
                pass
    conn.commit()
    log(f"  Cleared date_uncertain for {date_fixed} enriched rows with valid dates")

    # ── Step 10: Classify Applied Scientist scope ──
    log("Step 10: Classifying Applied Scientist scope...")
    as_rows = cur.execute(
        "SELECT gold_id, title, description_snippet, ai_keywords_hit "
        "FROM job_postings_gold WHERE title LIKE '%Applied Scientist%'"
    ).fetchall()
    kept = 0
    removed = 0
    for r in as_rows:
        gid, title, desc, kw = r[0], r[1], r[2] or '', r[3] or ''
        full_text = f"{title} {desc} {kw}".lower()
        # Check remove keywords first (higher priority)
        should_remove = any(k in full_text for k in APPLIED_SCIENTIST_REMOVE_KEYWORDS)
        should_keep = any(k in full_text for k in APPLIED_SCIENTIST_KEEP_KEYWORDS)
        if should_remove and not should_keep:
            # Mark as excluded via status
            cur.execute(
                "UPDATE job_postings_gold SET role_cluster = 'Applied Scientist (Excluded)', "
                "status = 'Excluded' WHERE gold_id = ?", (gid,)
            )
            removed += 1
        else:
            # Update role_cluster to Applied Scientist (distinct from Data Scientist)
            cur.execute(
                "UPDATE job_postings_gold SET role_cluster = 'Applied Scientist' "
                "WHERE gold_id = ?", (gid,)
            )
            kept += 1
    conn.commit()
    log(f"  Applied Scientist: kept {kept}, excluded {removed}")

    # ── Step 11: Add in_target_list column ──
    log("Step 11: Adding in_target_list column...")
    existing_cols = {r[1] for r in cur.execute(
        "PRAGMA table_info(job_postings_gold)"
    ).fetchall()}
    if 'in_target_list' not in existing_cols:
        cur.execute(
            "ALTER TABLE job_postings_gold ADD COLUMN in_target_list INTEGER DEFAULT 0"
        )
    cur.execute("UPDATE job_postings_gold SET in_target_list = 1 WHERE company_id > 0")
    # Also try matching by company_name against companies_200
    cur.execute("""
        UPDATE job_postings_gold
        SET in_target_list = 1
        WHERE company_name IN (SELECT company_name FROM companies_200)
          OR company_name IN (SELECT canonical_name FROM companies_200)
    """)
    conn.commit()
    in_target = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE in_target_list = 1"
    ).fetchone()[0]
    total = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    log(f"  in_target_list: {in_target}/{total} rows")

    # ── Final summary ──
    total = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    active = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]
    distinct_titles = cur.execute(
        "SELECT COUNT(DISTINCT title_normalized) FROM job_postings_gold"
    ).fetchone()[0]
    unknowns = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE company_name='Unknown'"
    ).fetchone()[0]
    html_remaining = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE description_snippet LIKE '%<%'"
    ).fetchone()[0]
    r_only = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE skills_extracted='R'"
    ).fetchone()[0]

    conn.close()

    log(f"fix_data_quality COMPLETE:")
    log(f"  Total rows:           {total}")
    log(f"  Active US:            {active}")
    log(f"  Distinct titles:      {distinct_titles}")
    log(f"  Remaining Unknown:    {unknowns}")
    log(f"  Remaining HTML:       {html_remaining}")
    log(f"  R-only skills:        {r_only}")
    log(f"  In target list:       {in_target}/{total}")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: fix_title_normalization  (7-step title AI signal cleanup)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Type A: AI identity base function lookup ──
_TYPE_A_FUNCTION_MAP = [
    (r'\banalyst\b', 'AI Analyst'),
    (r'\bresearch(?:er| scientist)\b', 'AI Researcher'),
    (r'\bengineer\b', 'AI Engineer'),
    (r'\bscientist\b', 'AI Scientist'),     # catches Applied Scientist etc.
    (r'\bstrategist\b', 'AI Strategist'),
    (r'\bmanager\b', 'AI Manager'),
]

# ── Type B: AI team context patterns to strip (ordered, longest first) ──
_AI_TEAM_STRIP = [
    r',?\s*\(Contract\),?\s*Artificial General Intelligence',
    r',?\s*Artificial General Intelligence',
    r',?\s*AGI',
    r'\s*-\s*Agentic AI\b[^,]*',
    r',?\s*Agentic AI\b[^,]*',
    r',?\s*GenAI\b[^,]*',
    r',?\s*Generative AI\b[^,]*',
    r',?\s*Generative Intelligence\b[^,]*',
    r',?\s*LLM\b[^,]*',
    r',?\s*Ads & GenAI\b[^,]*',
    r',?\s*AI Products\b[^,]*',
    r',?\s*AI Platform\b[^,]*',
    r'\s*-\s*AI\b[^,]*',
    r',?\s*Applied AI\b[^,]*',
    r',?\s*AWS Applied AI\b[^,]*',
    r',?\s*Foundation AI\b[^,]*',
    r',?\s*AI Core\b[^,]*',
    r',?\s*AI Data\b[^,]*',
    r',?\s*AI Engineering\b[^,]*',
    r',?\s*AI\s*$',
]

# ── Seniority / qualifier patterns to strip ──
_SENIORITY_STRIP = re.compile(
    r'\b(?:Senior|Sr\.?|Lead|Principal|Staff|Junior|Jr\.?|Associate)\s+',
    re.IGNORECASE
)
_LEVEL_STRIP = re.compile(r'\s+(?:I{1,3}|[123])\s*$')
_EMPLOYMENT_STRIP = re.compile(r'\s*\((?:Contract|Temp|Part-time|Remote)\)\s*', re.I)

# ── Prefix junk from LinkedIn / job board titles ──
_PREFIX_JUNK = re.compile(
    r'^(?:\[\d{4}\]\s*|'                          # [2026]
    r'(?:Oracle|Scale AI|Job Application for)\s+hiring\s+|'
    r'Job Application for\s+|'
    r'.*?\bhiring\s+)',                            # "X hiring Y in Z - LinkedIn"
    re.IGNORECASE
)
_SUFFIX_JUNK = re.compile(
    r'\s*(?:-\s*LinkedIn|in\s+United States.*$|in\s+San\s+.*$|\s*at\s+\w+$)',
    re.IGNORECASE
)

# ── Out-of-scope role patterns ──
OUT_OF_SCOPE_TITLES = {
    'Benefits and Leave Analyst', 'People Analyst', 'Sr. HRIS Analyst',
    'Compliance Lead Analyst', 'Lead Compliance Analyst', 'GRC Senior Analyst',
    'Corporate Accounting Analyst', 'NetSuite System Analyst',
    'Content Integrity Analyst', 'Protective Intelligence Analyst',
    'Field Enablement Reporting Analyst', 'Tech Stack Analyst - GTM Tools',
    'Certified Financial Planner, AI Analyst',
    'Senior Financial Analyst, Cloud Infrastructure',
    'Senior Financial Analyst, GTM', 'Sr. Analyst, GTM Strategic Finance',
    'Finance Expert - Macro Research Analyst',
    'Finance Reporting & Analytics Manager',
    'Senior Customer Success Quality Analyst',
    'Senior Sales Quality Analyst', 'Senior Sales Performance Analyst',
    'Technical Support Analyst - Generative AI',
}

# ── Extended TITLE_SEGMENTS for verbatim-copy normalization ──
TITLE_SEGMENTS_EXTENDED = {
    # Seniority-specific (ordered: specific before generic)
    r'\bhead of data science\b': 'Head of Data Science',
    r'\bstaff data science engineer\b': 'Staff Data Science Engineer',
    r'\bdata science engineer\b': 'Data Science Engineer',
    r'\bdata science intern\b': 'Data Science Intern',
    r'\bapplied data science\b': 'Applied Data Scientist',
    r'\bmachine learning analyst\b': 'Machine Learning Analyst',
    r'\b(?:senior )?business intelligence analyst\b': 'Business Intelligence Analyst',
    r'\bsenior analyst\b': 'Senior Analyst',
    r'\bsenior risk analyst\b': 'Risk Analyst',
    r'\bstaff risk analyst\b': 'Risk Analyst',
    r'\b(?:lead )?risk analyst\b': 'Risk Analyst',
    r'\bfraud.*analyst\b': 'Risk Analyst',
    r'\b(?:senior )?reporting analyst\b': 'Reporting Analyst',
    r'\b(?:senior )?revops.*analyst\b': 'RevOps Analyst',
    r'\bstrateg(?:y|ic).*analyst\b': 'Strategy Analyst',
    r'\b(?:senior )?manager.*decision intelligence\b': 'Decision Intelligence Manager',
    r'\bsenior manager.*data science\b': 'Data Science Manager',
    r'\bproduct data scientist\b': 'Product Data Scientist',
    r'\binternal audit.*analyst\b': 'Audit Analyst',
    r'\benablement.*analyst\b': 'Enablement Analyst',
    r'\bintelligence analyst\b': 'Intelligence Analyst',
    r'\b(?:software dev|sde)\b.*data science': 'Data Science Engineer',
    r'\brobotic': 'Robotics Data Scientist',
    r'\bdata science internship\b': 'Data Science Intern',
    r'\bproduct data science manager\b': 'Data Science Manager',
    r'\bproduct data scientist\b': 'Product Data Scientist',
}


def _clean_title_base(raw: str) -> str:
    """Strip LinkedIn/job-board junk, seniority, levels, and employment type."""
    s = _PREFIX_JUNK.sub('', raw).strip()
    s = _SUFFIX_JUNK.sub('', s).strip()
    s = _EMPLOYMENT_STRIP.sub(' ', s).strip()
    s = _SENIORITY_STRIP.sub('', s).strip()
    s = _LEVEL_STRIP.sub('', s).strip()
    return s


def _normalize_type_a(raw_title: str) -> str:
    """Type A (ai_in_title): strip all qualifiers → 'AI [function]'."""
    cleaned = _clean_title_base(raw_title)
    t = cleaned.lower()
    for pat, ai_func in _TYPE_A_FUNCTION_MAP:
        if re.search(pat, t):
            return ai_func
    return f"AI {cleaned}"  # fallback: keep whatever is left


def _normalize_type_b(raw_title: str) -> str:
    """Type B (ai_team_or_platform_in_title): strip AI context → '[Role] (AI Team)'."""
    # Strip AI team context
    s = raw_title
    for pat in _AI_TEAM_STRIP:
        s = re.sub(pat, '', s, flags=re.IGNORECASE).strip()

    # Clean LinkedIn/job-board junk
    s = _clean_title_base(s)

    # Remove trailing commas, dashes, colons from stripping
    s = re.sub(r'[,\-–—:]\s*$', '', s).strip()

    if not s:
        s = raw_title  # safety fallback

    # Normalize via TITLE_SEGMENTS
    t = s.lower()
    for pattern, canonical in TITLE_SEGMENTS.items():
        if re.search(pattern, t):
            return f"{canonical} (AI Team)"

    # Also check extended segments
    for pattern, canonical in TITLE_SEGMENTS_EXTENDED.items():
        if re.search(pattern, t):
            return f"{canonical} (AI Team)"

    return f"{s} (AI Team)"


def _normalize_verbatim(raw_title: str) -> str:
    """Normalize a verbatim-copy title (title_normalized == title)."""
    s = _clean_title_base(raw_title)

    # Remove department context suffixes (non-AI): ", Team Name" / "- Group"
    s = re.sub(r',\s*(?:Helix|ARR|GTM|Cloud Infrastructure|PXT)\b.*$', '', s, flags=re.I).strip()
    s = re.sub(r'\s*-\s*(?:US Remote|Data & Product|Global Forecasting)\b.*$', '', s, flags=re.I).strip()
    s = re.sub(r',\s*(?:Quantitative Modeling|Customer Metrics)\b.*$', '', s, flags=re.I).strip()
    s = re.sub(r',\s*(?:Siri Runtime|Egregious Harms|Trust and Safety)\b.*$', '', s, flags=re.I).strip()
    s = re.sub(r'\s*-\s*(?:Internal Audit & Risk|United States)\b.*$', '', s, flags=re.I).strip()

    # Strip year prefix without brackets (e.g. "2026 Data Science Internship")
    s = re.sub(r'^\d{4}\s+', '', s).strip()

    # Re-run level strip after dept-context removal (level may now be at end)
    s = _LEVEL_STRIP.sub('', s).strip()

    # Normalize plurals to singular
    s = re.sub(r'\b(Scientist|Manager|Analyst|Engineer)s\b', r'\1', s)

    # Remove trailing commas, dashes
    s = re.sub(r'[,\-–—:]\s*$', '', s).strip()

    if not s:
        return raw_title

    # Match against standard TITLE_SEGMENTS first
    t = s.lower()
    for pattern, canonical in TITLE_SEGMENTS.items():
        if re.search(pattern, t):
            return canonical

    # Then extended
    for pattern, canonical in TITLE_SEGMENTS_EXTENDED.items():
        if re.search(pattern, t):
            return canonical

    return s  # cleaned but unmatched


def cmd_fix_title_normalization():
    """7-step title normalization and AI signal fix.

    Steps:
    1. Fix has_ai_in_title for AGI/Artificial General Intelligence
    2. Fix ai_role_signature for AGI rows
    3. Recompute ai_signal_types from source fields
    4. Type A: normalize AI identity → "AI [function]"
    5. Type B: normalize AI team → "[Role] (AI Team)"
    6. Fix remaining verbatim copies
    7. Flag out-of-scope roles
    """
    conn = get_db()
    cur = conn.cursor()

    # ── Step 1: Fix has_ai_in_title for AGI rows ──
    log("Step 1: Fixing has_ai_in_title for AGI/Artificial General Intelligence...")
    before_count = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE has_ai_in_title = 1"
    ).fetchone()[0]

    agi_rows = cur.execute("""
        SELECT gold_id, title, has_ai_in_title, title_ai_terms
        FROM job_postings_gold
        WHERE (title LIKE '%Artificial General Intelligence%'
               OR title LIKE '% AGI %' OR title LIKE '% AGI' OR title LIKE '%,AGI%'
               OR title LIKE '%, AGI%')
          AND has_ai_in_title = 0
    """).fetchall()

    step1_fixed = 0
    for r in agi_rows:
        gid, title, _, existing_terms = r[0], r[1], r[2], r[3] or ''
        # Determine which term matched
        new_term = 'Artificial General Intelligence' if 'artificial general intelligence' in title.lower() else 'AGI'
        terms = f"{existing_terms}, {new_term}" if existing_terms else new_term
        cur.execute(
            "UPDATE job_postings_gold SET has_ai_in_title = 1, title_ai_terms = ? WHERE gold_id = ?",
            (terms, gid)
        )
        step1_fixed += 1
    conn.commit()

    after_count = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE has_ai_in_title = 1"
    ).fetchone()[0]
    log(f"  Fixed {step1_fixed} rows (has_ai_in_title: {before_count} → {after_count})")

    # ── Step 2: Fix ai_role_signature for AGI rows ──
    log("Step 2: Fixing ai_role_signature for AGI rows...")
    step2_fixed = cur.execute("""
        UPDATE job_postings_gold
        SET ai_role_signature = 'ai_team_or_platform_in_title'
        WHERE (title LIKE '%Artificial General Intelligence%'
               OR title LIKE '% AGI %' OR title LIKE '% AGI' OR title LIKE '%,AGI%'
               OR title LIKE '%, AGI%')
          AND ai_role_signature != 'ai_team_or_platform_in_title'
    """).rowcount
    conn.commit()
    log(f"  Fixed {step2_fixed} rows → ai_team_or_platform_in_title")

    # ── Step 3: Recompute ai_signal_types from source fields ──
    log("Step 3: Recomputing ai_signal_types from source fields...")
    all_rows = cur.execute("""
        SELECT gold_id, has_ai_in_title, description_snippet, ai_keywords_hit
        FROM job_postings_gold
    """).fetchall()

    step3_fixed = 0
    for r in all_rows:
        gid = r[0]
        signals = []
        # title signal
        if r[1]:  # has_ai_in_title
            signals.append('title')
        # description signal
        desc = r[2] or ''
        kw_hit = r[3] or ''
        desc_text = f"{desc} {kw_hit}".lower()
        if any(re.search(p, desc_text, re.I) for p in AI_KEYWORDS):
            signals.append('description')
        new_types = '|'.join(signals) if signals else ''
        cur.execute(
            "UPDATE job_postings_gold SET ai_signal_types = ? WHERE gold_id = ?",
            (new_types, gid)
        )
        step3_fixed += 1
    conn.commit()

    # Validate: no rows with has_ai_in_title=1 but missing 'title' in ai_signal_types
    inconsistent = cur.execute("""
        SELECT COUNT(*) FROM job_postings_gold
        WHERE has_ai_in_title = 1 AND (ai_signal_types IS NULL OR ai_signal_types NOT LIKE '%title%')
    """).fetchone()[0]
    log(f"  Recomputed {step3_fixed} rows. Validation: {inconsistent} inconsistent (should be 0)")

    # ── Step 4: Type A — AI identity → "AI [function]" ──
    log("Step 4: Normalizing Type A (ai_in_title) → 'AI [function]'...")
    type_a = cur.execute("""
        SELECT gold_id, title, title_normalized
        FROM job_postings_gold
        WHERE ai_role_signature = 'ai_in_title' AND status = 'Open'
    """).fetchall()

    step4_fixed = 0
    for r in type_a:
        gid, raw, old_norm = r[0], r[1], r[2]
        new_norm = _normalize_type_a(raw)
        if new_norm != old_norm:
            step4_fixed += 1
        cur.execute(
            "UPDATE job_postings_gold SET title_normalized = ? WHERE gold_id = ?",
            (new_norm, gid)
        )
    conn.commit()
    log(f"  Processed {len(type_a)} Type A rows, changed {step4_fixed}")

    # Also handle emerging_ai_named_role (these are also AI identity)
    emerging = cur.execute("""
        SELECT gold_id, title, title_normalized
        FROM job_postings_gold
        WHERE ai_role_signature = 'emerging_ai_named_role' AND status = 'Open'
    """).fetchall()
    for r in emerging:
        gid, raw, old_norm = r[0], r[1], r[2]
        new_norm = _normalize_type_a(raw)
        if new_norm != old_norm:
            step4_fixed += 1
            cur.execute(
                "UPDATE job_postings_gold SET title_normalized = ? WHERE gold_id = ?",
                (new_norm, gid)
            )
    conn.commit()
    log(f"  + {len(emerging)} emerging_ai_named_role rows (total changes: {step4_fixed})")

    # ── Step 5: Type B — AI team → "[Role] (AI Team)" ──
    log("Step 5: Normalizing Type B (ai_team_or_platform_in_title) → '[Role] (AI Team)'...")
    type_b = cur.execute("""
        SELECT gold_id, title, title_normalized
        FROM job_postings_gold
        WHERE ai_role_signature = 'ai_team_or_platform_in_title' AND status != 'Excluded'
    """).fetchall()

    step5_fixed = 0
    for r in type_b:
        gid, raw, old_norm = r[0], r[1], r[2]
        new_norm = _normalize_type_b(raw)
        if new_norm != old_norm:
            step5_fixed += 1
        cur.execute(
            "UPDATE job_postings_gold SET title_normalized = ? WHERE gold_id = ?",
            (new_norm, gid)
        )
    conn.commit()

    # Validate: all Type B should end with (AI Team)
    bad_b = cur.execute("""
        SELECT COUNT(*) FROM job_postings_gold
        WHERE ai_role_signature = 'ai_team_or_platform_in_title'
          AND status != 'Excluded'
          AND title_normalized NOT LIKE '%(AI Team)'
    """).fetchone()[0]
    log(f"  Processed {len(type_b)} Type B rows, changed {step5_fixed}. Validation: {bad_b} without (AI Team) suffix")

    # ── Step 6: Fix remaining verbatim copies (title_normalized == title) ──
    log("Step 6: Fixing remaining verbatim copies...")
    verbatim = cur.execute("""
        SELECT gold_id, title, title_normalized, ai_role_signature
        FROM job_postings_gold
        WHERE title_normalized = title AND status = 'Open'
    """).fetchall()

    step6_fixed = 0
    for r in verbatim:
        gid, raw, old_norm, sig = r[0], r[1], r[2], r[3]
        # Skip if already handled by Type A/B
        if sig in ('ai_in_title', 'ai_team_or_platform_in_title', 'emerging_ai_named_role'):
            continue
        new_norm = _normalize_verbatim(raw)
        if new_norm != old_norm:
            step6_fixed += 1
            cur.execute(
                "UPDATE job_postings_gold SET title_normalized = ? WHERE gold_id = ?",
                (new_norm, gid)
            )
    conn.commit()

    still_verbatim = cur.execute("""
        SELECT COUNT(*) FROM job_postings_gold
        WHERE title_normalized = title AND status = 'Open'
    """).fetchone()[0]
    log(f"  Fixed {step6_fixed} verbatim copies. Still verbatim: {still_verbatim}")

    # ── Step 7: Flag out-of-scope roles ──
    log("Step 7: Flagging out-of-scope roles...")
    step7_flagged = 0
    for oos_title in OUT_OF_SCOPE_TITLES:
        cnt = cur.execute("""
            UPDATE job_postings_gold SET status = 'Excluded'
            WHERE title = ? AND status = 'Open'
        """, (oos_title,)).rowcount
        if cnt:
            log(f"    Excluded: {oos_title} ({cnt} rows)")
            step7_flagged += cnt
    conn.commit()

    # Also flag by pattern
    oos_patterns = [
        (r'\bfinance\b.*\banalyst\b', 'Financial Analyst'),
        (r'\bfinancial analyst\b', 'Financial Analyst'),
        (r'\bhris\b', 'HRIS Analyst'),
        (r'\bnetsuite\b', 'ERP Analyst'),
        (r'\bsap\b', 'ERP Analyst'),
        (r'\berp\b', 'ERP Analyst'),
        (r'\baccounting analyst\b', 'Accounting Analyst'),
        (r'\bbenefit.*analyst\b', 'Benefits Analyst'),
        (r'\bpeople analyst\b', 'People Analyst'),
    ]
    for pat, label in oos_patterns:
        matching = cur.execute("""
            SELECT gold_id, title FROM job_postings_gold
            WHERE status = 'Open'
        """).fetchall()
        for m in matching:
            if re.search(pat, m[1], re.IGNORECASE):
                cur.execute(
                    "UPDATE job_postings_gold SET status = 'Excluded' WHERE gold_id = ?",
                    (m[0],)
                )
                step7_flagged += 1
    conn.commit()
    log(f"  Flagged {step7_flagged} out-of-scope rows as Excluded")

    # ── Final summary ──
    total = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    active = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]
    excluded = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE status='Excluded'"
    ).fetchone()[0]
    distinct_titles = cur.execute(
        "SELECT COUNT(DISTINCT title_normalized) FROM job_postings_gold WHERE status='Open'"
    ).fetchone()[0]
    type_a_count = cur.execute("""
        SELECT COUNT(*) FROM job_postings_gold
        WHERE title_normalized LIKE 'AI %' AND status='Open'
    """).fetchone()[0]
    type_b_count = cur.execute("""
        SELECT COUNT(*) FROM job_postings_gold
        WHERE title_normalized LIKE '%(AI Team)' AND status='Open'
    """).fetchone()[0]
    still_verbatim = cur.execute("""
        SELECT COUNT(*) FROM job_postings_gold
        WHERE title_normalized = title AND status='Open'
    """).fetchone()[0]

    conn.close()

    log(f"fix_title_normalization COMPLETE:")
    log(f"  Total rows:           {total}")
    log(f"  Active US:            {active}")
    log(f"  Excluded:             {excluded}")
    log(f"  Distinct titles:      {distinct_titles}")
    log(f"  Type A (AI X):        {type_a_count}")
    log(f"  Type B (X AI Team):   {type_b_count}")
    log(f"  Still verbatim:       {still_verbatim}")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

COMMANDS = {
    'init_db': cmd_init_db,
    'merge_dbs': cmd_merge_dbs,
    'build_companies': cmd_build_companies,
    'normalize_platforms': cmd_normalize_platforms,
    'mine_salary_from_body': cmd_mine_salary_from_body,
    'verify_and_enrich': cmd_verify_and_enrich,
    'backfill_title_ai': cmd_backfill_title_ai,
    'backfill_ai_role_signature': cmd_backfill_ai_role_signature,
    'backfill_skills': cmd_backfill_skills,
    'qa_check': cmd_qa_check,
    'export_review': cmd_export_review,
    'approve_db': cmd_approve_db,
    'analyze_approved': cmd_analyze_approved,
    'fix_data_quality': cmd_fix_data_quality,
    'fix_title_normalization': cmd_fix_title_normalization,
}


def main():
    parser = argparse.ArgumentParser(
        description='AI Analyst Jobs — Unified Research Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='\n'.join(f'  {k:30s}' for k in COMMANDS.keys())
    )
    parser.add_argument('command', choices=COMMANDS.keys(),
                        help='Pipeline command to run')
    args = parser.parse_args()

    log(f"Running: {args.command}")
    COMMANDS[args.command]()
    log(f"Done: {args.command}")


if __name__ == '__main__':
    main()

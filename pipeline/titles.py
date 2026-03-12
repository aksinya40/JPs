"""
AI Analyst Jobs — Title Normalization
=======================================
Title cleaning, normalization (Type A / B / Verbatim),
out-of-scope flagging, and the cmd_fix_title_normalization command.
"""
import re
from typing import List, Dict

from pipeline.constants import (
    AI_KEYWORDS,
    TITLE_SEGMENTS,
)
from pipeline.db import get_db, log


# ═══════════════════════════════════════════════════════════════════════════════
# TITLE NORMALIZATION CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

# ── Type A: AI identity base function lookup ──
TYPE_A_FUNCTION_MAP = [
    (r'\banalyst\b', 'AI Analyst'),
    (r'\bresearch(?:er| scientist)\b', 'AI Researcher'),
    (r'\bengineer\b', 'AI Engineer'),
    (r'\bscientist\b', 'AI Scientist'),     # catches Applied Scientist etc.
    (r'\bstrategist\b', 'AI Strategist'),
    (r'\bmanager\b', 'AI Manager'),
]

# ── Type B: AI team context patterns to strip (ordered, longest first) ──
AI_TEAM_STRIP = [
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
SENIORITY_STRIP = re.compile(
    r'\b(?:Senior|Sr\.?|Lead|Principal|Staff|Junior|Jr\.?|Associate)\s+',
    re.IGNORECASE
)
LEVEL_STRIP = re.compile(r'\s+(?:I{1,3}|[123])\s*$')
EMPLOYMENT_STRIP = re.compile(r'\s*\((?:Contract|Temp|Part-time|Remote)\)\s*', re.I)

# ── Prefix junk from LinkedIn / job board titles ──
PREFIX_JUNK = re.compile(
    r'^(?:\[\d{4}\]\s*|'                          # [2026]
    r'(?:Oracle|Scale AI|Job Application for)\s+hiring\s+|'
    r'Job Application for\s+|'
    r'.*?\bhiring\s+)',                            # "X hiring Y in Z - LinkedIn"
    re.IGNORECASE
)
SUFFIX_JUNK = re.compile(
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
}


# ═══════════════════════════════════════════════════════════════════════════════
# TITLE NORMALIZATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _clean_title_base(raw: str) -> str:
    """Strip LinkedIn/job-board junk, seniority, levels, and employment type."""
    s = PREFIX_JUNK.sub('', raw).strip()
    s = SUFFIX_JUNK.sub('', s).strip()
    s = EMPLOYMENT_STRIP.sub(' ', s).strip()
    s = SENIORITY_STRIP.sub('', s).strip()
    s = LEVEL_STRIP.sub('', s).strip()
    return s


def _normalize_type_a(raw_title: str) -> str:
    """Type A (ai_in_title): strip all qualifiers -> 'AI [function]'."""
    cleaned = _clean_title_base(raw_title)
    t = cleaned.lower()
    for pat, ai_func in TYPE_A_FUNCTION_MAP:
        if re.search(pat, t):
            return ai_func
    return f"AI {cleaned}"  # fallback: keep whatever is left


def _normalize_type_b(raw_title: str) -> str:
    """Type B (ai_team_or_platform_in_title): strip AI context -> '[Role] (AI Team)'."""
    # Strip AI team context
    s = raw_title
    for pat in AI_TEAM_STRIP:
        s = re.sub(pat, '', s, flags=re.IGNORECASE).strip()

    # Clean LinkedIn/job-board junk
    s = _clean_title_base(s)

    # Remove trailing commas, dashes, colons from stripping
    s = re.sub(r'[,\-\u2013\u2014:]\s*$', '', s).strip()

    if not s:
        s = raw_title  # safety fallback

    # Normalize via TITLE_SEGMENTS first
    t = s.lower()
    for pattern, canonical in TITLE_SEGMENTS.items():
        if re.search(pattern, t):
            return f"{canonical} (AI Team)"

    # Then extended segments
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
    s = LEVEL_STRIP.sub('', s).strip()

    # Normalize plurals to singular
    s = re.sub(r'\b(Scientist|Manager|Analyst|Engineer)s\b', r'\1', s)

    # Remove trailing commas, dashes
    s = re.sub(r'[,\-\u2013\u2014:]\s*$', '', s).strip()

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


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: fix_title_normalization
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_fix_title_normalization():
    """7-step title normalization and AI signal fix.

    Steps:
    1. Fix has_ai_in_title for AGI/Artificial General Intelligence
    2. Fix ai_role_signature for AGI rows
    3. Recompute ai_signal_types from source fields
    4. Type A: normalize AI identity -> "AI [function]"
    5. Type B: normalize AI team -> "[Role] (AI Team)"
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
    log(f"  Fixed {step1_fixed} rows (has_ai_in_title: {before_count} -> {after_count})")

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
    log(f"  Fixed {step2_fixed} rows -> ai_team_or_platform_in_title")

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
        if r[1]:  # has_ai_in_title
            signals.append('title')
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

    inconsistent = cur.execute("""
        SELECT COUNT(*) FROM job_postings_gold
        WHERE has_ai_in_title = 1 AND (ai_signal_types IS NULL OR ai_signal_types NOT LIKE '%title%')
    """).fetchone()[0]
    log(f"  Recomputed {step3_fixed} rows. Validation: {inconsistent} inconsistent (should be 0)")

    # ── Step 4: Type A -- AI identity -> "AI [function]" ──
    log("Step 4: Normalizing Type A (ai_in_title) -> 'AI [function]'...")
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

    # ── Step 5: Type B -- AI team -> "[Role] (AI Team)" ──
    log("Step 5: Normalizing Type B (ai_team_or_platform_in_title) -> '[Role] (AI Team)'...")
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

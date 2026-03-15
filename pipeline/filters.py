"""
AI Analyst Jobs — Classification & Filtering
===============================================
Functions that classify, filter, or categorize job data:
AI keyword matching, role exclusion, work mode resolution,
AI role signature classification.
"""
from __future__ import annotations

import re

from pipeline.constants import (
    AI_KEYWORDS,
    ROLE_EXCLUSION_PATTERNS,
    REMOTE_PATTERNS,
    HYBRID_PATTERNS,
    EMERGING_AI_PATTERNS,
    AI_TITLE_TERMS,
    LLM_GENAI_TERMS,
    AGENTIC_TERMS,
    AI_TEAM_TERMS,
)


def _check_false_positives(text: str, hits: list[str]) -> set:
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


def match_ai_keywords(text: str) -> list[str]:
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


def is_role_excluded(title: str) -> bool:
    """Check if a role title matches exclusion patterns."""
    if not title:
        return False
    for pat in ROLE_EXCLUSION_PATTERNS:
        if re.search(pat, title, re.IGNORECASE):
            return True
    return False


def classify_ai_role_signature(title: str, description: str = '',
                               skills: str = '') -> str:
    """Classify ai_role_signature in priority order."""
    t = (title or '').lower()
    desc = (description or '').lower()
    sk = (skills or '').lower()
    combined_scope = f"{desc} {sk}"

    # Priority 1: emerging_ai_named_role
    for pat in EMERGING_AI_PATTERNS:
        if re.search(pat, t, re.IGNORECASE):
            return 'emerging_ai_named_role'

    # Split title at comma/dash/parens for role vs team context
    role_part = re.split(r'[,\-–—(]', t)[0].strip()
    after_part = t[len(role_part):]

    # Priority 2: ai_in_title
    if any(re.search(p, role_part, re.I) for p in AI_TITLE_TERMS):
        return 'ai_in_title'

    # Priority 3: ai_team_or_platform_in_title
    if any(re.search(p, after_part, re.I) for p in AI_TITLE_TERMS):
        return 'ai_team_or_platform_in_title'

    # Priority 4: llm_or_genai_in_scope
    if any(re.search(p, combined_scope, re.I) for p in LLM_GENAI_TERMS):
        return 'llm_or_genai_in_scope'

    # Priority 5: agentic_in_scope
    if any(re.search(p, combined_scope, re.I) for p in AGENTIC_TERMS):
        return 'agentic_in_scope'

    # Priority 6: ai_team_or_platform_in_scope
    if any(re.search(p, combined_scope, re.I) for p in AI_TEAM_TERMS):
        return 'ai_team_or_platform_in_scope'

    # Priority 7: ai_in_description_only
    if any(re.search(p, combined_scope, re.I) for p in AI_KEYWORDS):
        return 'ai_in_description_only'

    return 'ai_in_description_only'


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

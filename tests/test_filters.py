"""Tests for pipeline.filters — classification & filtering functions."""
import pytest
from pipeline.filters import (
    _check_false_positives,
    match_ai_keywords,
    is_role_excluded,
    classify_ai_role_signature,
    resolve_work_mode,
)


# ═══════════════════════════════════════════════════════════════════════════════
# _check_false_positives
# ═══════════════════════════════════════════════════════════════════════════════

class TestCheckFalsePositives:
    def test_ml_inside_email(self):
        """'ml' inside 'email' should be flagged as false positive."""
        fps = _check_false_positives("send us an email", ['ml'])
        assert 'ml' in fps

    def test_ml_standalone_not_false_positive(self):
        """Standalone 'ml' should NOT be flagged."""
        fps = _check_false_positives("experience with ml models", ['ml'])
        assert 'ml' not in fps

    def test_ml_inside_html(self):
        """'ml' inside 'html' should be flagged."""
        fps = _check_false_positives("html and css skills", ['ml'])
        assert 'ml' in fps

    def test_rag_inside_garage(self):
        """'rag' inside 'garage' (no standalone) should be flagged."""
        fps = _check_false_positives("park in the garage", ['rag'])
        assert 'rag' in fps

    def test_rag_standalone_not_false_positive(self):
        """Standalone 'rag' should NOT be flagged."""
        fps = _check_false_positives("implement rag pipeline for retrieval", ['rag'])
        assert 'rag' not in fps

    def test_no_hits(self):
        fps = _check_false_positives("some text", [])
        assert len(fps) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# match_ai_keywords
# ═══════════════════════════════════════════════════════════════════════════════

class TestMatchAiKeywords:
    def test_finds_llm(self):
        hits = match_ai_keywords("Building LLM applications for enterprise")
        assert any('llm' in h.lower() for h in hits)

    def test_finds_machine_learning(self):
        hits = match_ai_keywords("Machine learning engineer with Python")
        assert len(hits) > 0

    def test_finds_generative_ai(self):
        hits = match_ai_keywords("Generative AI platform development")
        assert len(hits) > 0

    def test_empty_text(self):
        assert match_ai_keywords("") == []
        assert match_ai_keywords(None) == []

    def test_no_ai_keywords(self):
        hits = match_ai_keywords("Accounting specialist for quarterly reports")
        assert len(hits) == 0

    def test_email_not_ml(self):
        """'email' should NOT trigger 'ml' keyword."""
        hits = match_ai_keywords("Send your resume via email to apply")
        ml_hits = [h for h in hits if 'ml' in h.lower() and h.lower() != 'html']
        assert len(ml_hits) == 0

    def test_multiple_keywords(self):
        hits = match_ai_keywords(
            "Work with LLM and NLP models to build RAG systems"
        )
        assert len(hits) >= 2  # Should find llm, nlp, rag at minimum

    def test_case_insensitive(self):
        hits1 = match_ai_keywords("LLM applications")
        hits2 = match_ai_keywords("llm applications")
        assert len(hits1) == len(hits2)


# ═══════════════════════════════════════════════════════════════════════════════
# is_role_excluded
# ═══════════════════════════════════════════════════════════════════════════════

class TestIsRoleExcluded:
    def test_bi_engineer_excluded(self):
        assert is_role_excluded("BI Engineer") is True

    def test_ml_engineer_excluded(self):
        assert is_role_excluded("ML Engineer") is True

    def test_mlops_excluded(self):
        assert is_role_excluded("MLOps Engineer") is True

    def test_data_analyst_not_excluded(self):
        assert is_role_excluded("Data Analyst") is False

    def test_ai_analyst_not_excluded(self):
        assert is_role_excluded("AI Data Analyst") is False

    def test_empty(self):
        assert is_role_excluded("") is False
        assert is_role_excluded(None) is False


# ═══════════════════════════════════════════════════════════════════════════════
# classify_ai_role_signature
# ═══════════════════════════════════════════════════════════════════════════════

class TestClassifyAiRoleSignature:
    def test_emerging_ai_named_role(self):
        """AI Analyst, LLM Analyst etc. should be emerging_ai_named_role."""
        result = classify_ai_role_signature("AI Analyst")
        assert result == 'emerging_ai_named_role'

    def test_emerging_llm_analyst(self):
        result = classify_ai_role_signature("LLM Analyst")
        assert result == 'emerging_ai_named_role'

    def test_emerging_genai_analyst(self):
        result = classify_ai_role_signature("Generative AI Analyst")
        assert result == 'emerging_ai_named_role'

    def test_ai_in_title(self):
        """Direct AI term in the role part of title."""
        result = classify_ai_role_signature("AI Data Analyst")
        assert result == 'ai_in_title'

    def test_ai_team_in_title(self):
        """AI term after comma/dash in title = team context."""
        result = classify_ai_role_signature("Data Analyst - AI Platform Team")
        assert result in ('ai_in_title', 'ai_team_or_platform_in_title')

    def test_llm_in_scope(self):
        """LLM mentioned in description but not title."""
        result = classify_ai_role_signature(
            "Data Analyst",
            description="Work with LLM applications and fine-tuning"
        )
        assert result == 'llm_or_genai_in_scope'

    def test_agentic_in_scope(self):
        """Agentic terms in description — needs literal 'agentic' or 'ai agent'."""
        result = classify_ai_role_signature(
            "Data Analyst",
            description="Build agentic workflows for data processing"
        )
        assert result == 'agentic_in_scope'

    def test_ai_agent_in_scope(self):
        result = classify_ai_role_signature(
            "Data Analyst",
            description="Deploy AI agents for automated reporting"
        )
        assert result == 'agentic_in_scope'

    def test_ai_in_description_only(self):
        """Generic AI keyword in description only."""
        result = classify_ai_role_signature(
            "Data Analyst",
            description="Support machine learning team with data prep"
        )
        assert result in ('ai_in_description_only', 'ai_team_or_platform_in_scope')

    def test_fallback(self):
        """No AI signals at all — should return ai_in_description_only."""
        result = classify_ai_role_signature(
            "Data Analyst",
            description="Excel and SQL reporting"
        )
        assert result == 'ai_in_description_only'

    def test_priority_order(self):
        """emerging_ai_named_role should beat ai_in_title when both match."""
        result = classify_ai_role_signature(
            "AI Analyst",
            description="Build LLM-based agentic systems"
        )
        # AI Analyst matches emerging_ai_named_role (Priority 1), which wins over ai_in_title
        assert result == 'emerging_ai_named_role'

    def test_empty_title(self):
        result = classify_ai_role_signature("")
        assert result == 'ai_in_description_only'


# ═══════════════════════════════════════════════════════════════════════════════
# resolve_work_mode
# ═══════════════════════════════════════════════════════════════════════════════

class TestResolveWorkMode:
    def test_ats_remote(self):
        """ATS value 'Remote' should be used directly."""
        assert resolve_work_mode("Remote", "", "") == "Remote"

    def test_ats_hybrid(self):
        assert resolve_work_mode("Hybrid", "", "") == "Hybrid"

    def test_ats_unknown_falls_through(self):
        """'unknown' ATS value should fall through to location check."""
        result = resolve_work_mode("unknown", "Remote, USA", "")
        assert result == "Remote"

    def test_location_remote(self):
        assert resolve_work_mode("", "Remote", "") == "Remote"

    def test_location_hybrid(self):
        assert resolve_work_mode("", "Hybrid - San Francisco", "") == "Hybrid"

    def test_ats_onsite_kept(self):
        """Explicit 'On-site' from ATS should be preserved."""
        assert resolve_work_mode("On-site", "San Francisco, CA", "") == "On-site"

    def test_default_onsite(self):
        """No signals → default to On-site."""
        assert resolve_work_mode("", "San Francisco, CA", "") == "On-site"

    def test_location_standardized_remote(self):
        """Remote signal in standardized location."""
        assert resolve_work_mode("", "", "Remote") == "Remote"

    def test_ats_empty_string_falls_through(self):
        """Empty ATS value should fall through."""
        result = resolve_work_mode("", "Remote position", "")
        assert result == "Remote"

    def test_remote_in_combined_location(self):
        """Remote pattern found across location_raw + location_standardized."""
        result = resolve_work_mode("", "New York, NY", "Remote OK")
        assert result == "Remote"

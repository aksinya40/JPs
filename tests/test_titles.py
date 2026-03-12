"""Tests for pipeline.titles — title normalization functions."""
import pytest
import re
from pipeline.titles import (
    _clean_title_base,
    _normalize_type_a,
    _normalize_type_b,
    _normalize_verbatim,
    TYPE_A_FUNCTION_MAP,
    AI_TEAM_STRIP,
    SENIORITY_STRIP,
    LEVEL_STRIP,
    EMPLOYMENT_STRIP,
    PREFIX_JUNK,
    SUFFIX_JUNK,
    OUT_OF_SCOPE_TITLES,
    TITLE_SEGMENTS_EXTENDED,
)


# ═════════════════════════════════════════════════════════════════════════════
# _clean_title_base
# ═════════════════════════════════════════════════════════════════════════════

class TestCleanTitleBase:
    def test_strips_linkedin_suffix(self):
        """Should strip '- LinkedIn' suffix."""
        result = _clean_title_base("Data Analyst - LinkedIn")
        assert "LinkedIn" not in result
        assert "Data Analyst" in result

    def test_strips_contract_employment_type(self):
        """Should strip (Contract) employment marker."""
        result = _clean_title_base("Data Analyst (Contract)")
        assert "Contract" not in result
        assert "Data Analyst" in result

    def test_strips_seniority_prefix(self):
        """Should strip Sr., Jr., etc. for normalization."""
        result = _clean_title_base("Sr. Data Scientist")
        assert "Data Scientist" in result
        assert "Sr." not in result

    def test_strips_level_suffix(self):
        """Should strip level indicators (I, II, III) from end."""
        result = _clean_title_base("Data Analyst III")
        assert "III" not in result

    def test_preserves_core_title(self):
        """Core title words should survive cleaning."""
        result = _clean_title_base("Data Analyst")
        assert "Data Analyst" in result

    def test_empty_input(self):
        result = _clean_title_base("")
        assert result == ""


# ═════════════════════════════════════════════════════════════════════════════
# _normalize_type_a (AI-titled roles: "AI [function]")
# ═════════════════════════════════════════════════════════════════════════════

class TestNormalizeTypeA:
    def test_analyst_maps_to_ai_analyst(self):
        """Any title with 'analyst' → 'AI Analyst'."""
        result = _normalize_type_a("AI Data Analyst")
        assert result == "AI Analyst"

    def test_machine_learning_analyst(self):
        """Machine Learning Data Analyst → 'AI Analyst' (analyst match)."""
        result = _normalize_type_a("Machine Learning Data Analyst")
        assert result == "AI Analyst"

    def test_scientist_maps_to_ai_scientist(self):
        """Scientist → 'AI Scientist'."""
        result = _normalize_type_a("AI Data Scientist")
        assert result == "AI Scientist"

    def test_engineer_maps_to_ai_engineer(self):
        """Engineer → 'AI Engineer'."""
        result = _normalize_type_a("AI ML Engineer")
        assert result == "AI Engineer"

    def test_researcher_maps_to_ai_researcher(self):
        """Researcher → 'AI Researcher'."""
        result = _normalize_type_a("AI Research Scientist")
        assert result == "AI Researcher"

    def test_unknown_function_fallback(self):
        """Title not in TYPE_A map → 'AI <cleaned>'."""
        result = _normalize_type_a("AI Evangelist")
        assert result.startswith("AI ")
        assert len(result) > 3

    def test_strips_seniority(self):
        """Senior AI Data Analyst → seniority stripped before mapping."""
        result = _normalize_type_a("Senior AI Data Analyst")
        assert result == "AI Analyst"


# ═════════════════════════════════════════════════════════════════════════════
# _normalize_type_b (AI team/platform: "X (AI Team)")
# ═════════════════════════════════════════════════════════════════════════════

class TestNormalizeTypeB:
    def test_data_analyst_ai_team(self):
        """Data Analyst, AI Team → 'Data Analyst (AI Team)'."""
        result = _normalize_type_b("Data Analyst, AI Team")
        assert result == "Data Analyst (AI Team)"

    def test_adds_ai_team_suffix(self):
        """Should always end with ' (AI Team)'."""
        result = _normalize_type_b("Product Analyst, GenAI Division")
        assert result.endswith("(AI Team)")

    def test_strips_ai_context_from_role(self):
        """The AI context should not appear in the role prefix."""
        result = _normalize_type_b("Data Analyst, Generative AI Platform")
        assert "(AI Team)" in result
        assert "Generative AI" not in result.split("(AI Team)")[0]

    def test_agi_context_stripped(self):
        """AGI context stripped from title."""
        result = _normalize_type_b("Data Scientist, Artificial General Intelligence")
        assert "(AI Team)" in result


# ═════════════════════════════════════════════════════════════════════════════
# _normalize_verbatim (fallback cleaning)
# ═════════════════════════════════════════════════════════════════════════════

class TestNormalizeVerbatim:
    def test_removes_level(self):
        """Should strip level indicators like III."""
        result = _normalize_verbatim("Data Analyst III")
        assert "III" not in result

    def test_preserves_core(self):
        result = _normalize_verbatim("Data Analyst")
        assert "Data Analyst" in result

    def test_cleans_trailing_dash(self):
        """Should not end with a dash or comma."""
        result = _normalize_verbatim("Data Analyst -")
        assert not result.rstrip().endswith("-")

    def test_strips_department_context(self):
        """Should strip department suffixes like ', GTM'."""
        result = _normalize_verbatim("Financial Analyst, GTM Strategic Finance")
        assert "GTM" not in result


# ═════════════════════════════════════════════════════════════════════════════
# Constants sanity checks
# ═════════════════════════════════════════════════════════════════════════════

class TestTitleConstants:
    def test_type_a_function_map_has_entries(self):
        """TYPE_A_FUNCTION_MAP should have reasonable number of entries."""
        assert len(TYPE_A_FUNCTION_MAP) >= 5

    def test_out_of_scope_has_entries(self):
        """OUT_OF_SCOPE_TITLES should have entries for exclusion."""
        assert len(OUT_OF_SCOPE_TITLES) >= 3

    def test_title_segments_extended_has_entries(self):
        """TITLE_SEGMENTS_EXTENDED should have 15+ segments."""
        assert len(TITLE_SEGMENTS_EXTENDED) >= 15

    def test_seniority_strip_is_compiled_regex(self):
        """SENIORITY_STRIP should be a compiled regex pattern."""
        assert hasattr(SENIORITY_STRIP, 'pattern')
        assert 'Senior' in SENIORITY_STRIP.pattern

    def test_ai_team_strip_patterns_compile(self):
        """All AI team patterns should compile as regex."""
        for pat in AI_TEAM_STRIP:
            re.compile(pat, re.IGNORECASE)


# ═════════════════════════════════════════════════════════════════════════════
# Regression tests for known edge cases
# ═════════════════════════════════════════════════════════════════════════════

class TestTitleRegressions:
    def test_ai_analyst_not_stripped_to_empty(self):
        """AI Analyst should not be cleaned to an empty string."""
        result = _clean_title_base("AI Analyst")
        assert len(result.strip()) > 0

    def test_genai_title_preserved(self):
        """GenAI in title should survive cleaning."""
        result = _clean_title_base("GenAI Data Analyst")
        # Should still have substance
        assert "Analyst" in result or "GenAI" in result

    def test_applied_scientist_preserved(self):
        """Applied Scientist should survive cleaning."""
        result = _clean_title_base("Applied Scientist")
        assert "Applied Scientist" in result

    def test_staff_level_preserved_in_base(self):
        """Staff-level titles should still have substance."""
        result = _clean_title_base("Staff Data Scientist")
        assert "Data Scientist" in result

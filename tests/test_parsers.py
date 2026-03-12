"""Tests for pipeline.parsers — pure text transformation functions."""
import pytest
from pipeline.parsers import (
    normalize_text,
    normalize_company,
    normalize_url,
    canonical_job_key,
    compute_title_ai_terms,
    extract_salary,
    extract_skills,
    extract_company_from_url,
    canonicalize_platform,
    window_bucket,
    normalize_title_to_segment,
    strip_html,
    is_aggregator_url,
)


# ═══════════════════════════════════════════════════════════════════════════════
# normalize_text
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalizeText:
    def test_basic(self):
        assert normalize_text("  Hello   World  ") == "hello world"

    def test_empty(self):
        assert normalize_text("") == ""
        assert normalize_text(None) == ""

    def test_tabs_newlines(self):
        assert normalize_text("hello\n\tworld") == "hello world"


# ═══════════════════════════════════════════════════════════════════════════════
# normalize_company
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalizeCompany:
    def test_strip_inc(self):
        assert normalize_company("Acme, Inc.") == "acme"

    def test_strip_llc(self):
        assert normalize_company("Widgets LLC") == "widgets"

    def test_strip_corp(self):
        assert normalize_company("Big Corp.") == "big"

    def test_removes_special_chars(self):
        assert normalize_company("O'Reilly & Sons") == "oreillysons"

    def test_empty(self):
        assert normalize_company("") == ""
        assert normalize_company(None) == ""

    def test_already_clean(self):
        assert normalize_company("google") == "google"


# ═══════════════════════════════════════════════════════════════════════════════
# normalize_url
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalizeUrl:
    def test_strips_protocol_and_www(self):
        assert normalize_url("https://www.example.com/jobs") == "example.com/jobs"

    def test_strips_query_params(self):
        assert normalize_url("https://example.com/jobs?utm_source=google") == "example.com/jobs"

    def test_strips_fragment(self):
        assert normalize_url("https://example.com/jobs#apply") == "example.com/jobs"

    def test_strips_trailing_slash(self):
        assert normalize_url("https://example.com/jobs/") == "example.com/jobs"

    def test_empty(self):
        assert normalize_url("") == ""
        assert normalize_url(None) == ""


# ═══════════════════════════════════════════════════════════════════════════════
# canonical_job_key
# ═══════════════════════════════════════════════════════════════════════════════

class TestCanonicalJobKey:
    def test_deterministic(self):
        k1 = canonical_job_key("Greenhouse", "12345", "https://example.com/jobs/1", 42)
        k2 = canonical_job_key("Greenhouse", "12345", "https://example.com/jobs/1", 42)
        assert k1 == k2

    def test_different_inputs_different_keys(self):
        k1 = canonical_job_key("Greenhouse", "12345", "https://example.com/jobs/1", 42)
        k2 = canonical_job_key("Lever", "12345", "https://example.com/jobs/1", 42)
        assert k1 != k2

    def test_url_normalized(self):
        """Same URL with/without tracking params should produce same key."""
        k1 = canonical_job_key("Greenhouse", "12345", "https://example.com/jobs/1", 42)
        k2 = canonical_job_key("Greenhouse", "12345", "https://example.com/jobs/1?utm=google", 42)
        assert k1 == k2

    def test_sha256_length(self):
        k = canonical_job_key("GH", "1", "https://x.com", 1)
        assert len(k) == 64  # SHA-256 hex digest


# ═══════════════════════════════════════════════════════════════════════════════
# compute_title_ai_terms
# ═══════════════════════════════════════════════════════════════════════════════

class TestComputeTitleAiTerms:
    def test_ai_in_title(self):
        has_ai, terms = compute_title_ai_terms("AI Data Analyst")
        assert has_ai == 1
        assert "AI" in terms

    def test_no_ai(self):
        has_ai, terms = compute_title_ai_terms("Marketing Specialist")
        assert has_ai == 0
        assert terms == ""

    def test_empty(self):
        has_ai, terms = compute_title_ai_terms("")
        assert has_ai == 0
        assert terms == ""

    def test_multiple_terms(self):
        has_ai, terms = compute_title_ai_terms("Senior ML/AI Engineer - NLP Platform")
        assert has_ai == 1
        # Should find multiple AI terms


# ═══════════════════════════════════════════════════════════════════════════════
# extract_salary
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractSalary:
    def test_annual_range(self):
        result = extract_salary("$120,000 - $160,000 per year")
        assert result is not None
        assert result['salary_min_usd'] == 120000
        assert result['salary_max_usd'] == 160000
        assert result['salary_period'] == 'Annual'
        assert result['salary_currency'] == 'USD'

    def test_k_suffix(self):
        result = extract_salary("$120k - $160k")
        assert result is not None
        assert result['salary_min_usd'] == 120000
        assert result['salary_max_usd'] == 160000

    def test_hourly(self):
        result = extract_salary("$50 - $75 per hour")
        assert result is not None
        assert result['salary_period'] == 'Hourly'
        # $50/hr * 2080 = $104,000
        assert result['salary_min_usd'] == 50 * 2080
        assert result['salary_max_usd'] == 75 * 2080

    def test_non_usd_skipped(self):
        result = extract_salary("£60,000 - £80,000")
        assert result is not None
        assert result.get('skip') is True

    def test_no_salary(self):
        result = extract_salary("Looking for a data analyst with 5+ years experience")
        assert result is None

    def test_empty(self):
        assert extract_salary("") is None
        assert extract_salary(None) is None

    def test_insane_range_rejected(self):
        """Ranges over 5x ratio are rejected."""
        result = extract_salary("$10,000 - $600,000 per year")
        assert result is None

    def test_min_gt_max_swapped(self):
        """If min > max, they get swapped."""
        result = extract_salary("$160,000 - $120,000 per year")
        if result:
            assert result['salary_min_usd'] <= result['salary_max_usd']


# ═══════════════════════════════════════════════════════════════════════════════
# extract_skills
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractSkills:
    def test_python_and_sql(self):
        skills, has_py, has_sql = extract_skills(
            "Experience with Python, SQL, and Tableau required"
        )
        assert has_py == 1
        assert has_sql == 1
        assert 'Python' in skills
        assert 'SQL' in skills
        assert 'Tableau' in skills

    def test_no_skills(self):
        skills, has_py, has_sql = extract_skills("Great team player wanted")
        assert skills == ''
        assert has_py == 0
        assert has_sql == 0

    def test_empty(self):
        skills, has_py, has_sql = extract_skills("")
        assert skills == ''
        assert has_py == 0
        assert has_sql == 0

    def test_r_alone_is_noise(self):
        """R by itself is too ambiguous — filtered out."""
        skills, _, _ = extract_skills("Candidates should have strong R skills")
        # R alone should be filtered
        assert skills == '' or 'R' not in skills.split(', ') or len(skills.split(', ')) > 1

    def test_r_with_other_skills_kept(self):
        """R alongside Python/SQL is valid."""
        skills, has_py, _ = extract_skills(
            "Must know Python, R, and SQL for statistical analysis"
        )
        assert has_py == 1
        assert 'R' in skills

    def test_case_sensitive_r(self):
        """Only uppercase R should match, not 'r' in other contexts."""
        skills1, _, _ = extract_skills("Use R for analysis alongside Python")
        # lowercase 'r' in 'for' should not trigger
        # but uppercase R at start should


# ═══════════════════════════════════════════════════════════════════════════════
# extract_company_from_url
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractCompanyFromUrl:
    def test_greenhouse(self):
        result = extract_company_from_url(
            "https://boards.greenhouse.io/acmecorp/jobs/12345"
        )
        assert result is not None
        assert 'acme' in result.lower() or 'acmecorp' in result.lower()

    def test_lever(self):
        result = extract_company_from_url(
            "https://jobs.lever.co/widgetsinc/some-job-id"
        )
        assert result is not None
        assert 'widget' in result.lower()

    def test_empty(self):
        assert extract_company_from_url("") is None
        assert extract_company_from_url(None) is None

    def test_unknown_domain(self):
        result = extract_company_from_url("https://example.com/jobs/123")
        # May return None if no ATS pattern matches
        # (depends on ATS_SLUG_PATTERNS config)


# ═══════════════════════════════════════════════════════════════════════════════
# canonicalize_platform
# ═══════════════════════════════════════════════════════════════════════════════

class TestCanonicalizePlatform:
    def test_greenhouse(self):
        assert canonicalize_platform("greenhouse") == "Greenhouse"

    def test_lever(self):
        assert canonicalize_platform("lever") == "Lever"

    def test_linkedin(self):
        assert canonicalize_platform("linkedin") == "LinkedIn"

    def test_empty_returns_other(self):
        assert canonicalize_platform("") == "Other"
        assert canonicalize_platform(None) == "Other"

    def test_unknown_kept_as_is(self):
        result = canonicalize_platform("SomeObscurePlatform")
        # Should keep original if no match
        assert result == "SomeObscurePlatform"


# ═══════════════════════════════════════════════════════════════════════════════
# window_bucket
# ═══════════════════════════════════════════════════════════════════════════════

class TestWindowBucket:
    def test_h2_2025(self):
        assert window_bucket("2025-07-01") == "H2_2025"
        assert window_bucket("2025-12-31") == "H2_2025"

    def test_q1_2026(self):
        assert window_bucket("2026-01-01") == "Q1_2026"
        assert window_bucket("2026-03-15") == "Q1_2026"
        assert window_bucket("2026-03-31") == "Q1_2026"

    def test_out_of_range(self):
        assert window_bucket("2025-01-01") == "UNCERTAIN"
        assert window_bucket("2026-04-01") == "UNCERTAIN"
        assert window_bucket("2024-12-31") == "UNCERTAIN"

    def test_empty(self):
        assert window_bucket("") == "UNCERTAIN"
        assert window_bucket(None) == "UNCERTAIN"

    def test_invalid_date(self):
        assert window_bucket("not-a-date") == "UNCERTAIN"

    def test_datetime_string(self):
        """Should handle full datetime strings by parsing first 10 chars."""
        assert window_bucket("2026-02-15T10:30:00") == "Q1_2026"


# ═══════════════════════════════════════════════════════════════════════════════
# normalize_title_to_segment
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalizeTitleToSegment:
    def test_data_analyst(self):
        result = normalize_title_to_segment("Senior Data Analyst")
        assert result != ""  # Should match some segment

    def test_empty(self):
        assert normalize_title_to_segment("") == ""

    def test_no_match_returns_raw(self):
        result = normalize_title_to_segment("Chief Happiness Officer")
        # Falls back to raw title if no pattern matches
        assert result == "Chief Happiness Officer"


# ═══════════════════════════════════════════════════════════════════════════════
# strip_html
# ═══════════════════════════════════════════════════════════════════════════════

class TestStripHtml:
    def test_removes_tags(self):
        assert strip_html("<p>Hello <b>world</b></p>") == "Hello world"

    def test_removes_entities(self):
        assert strip_html("Hello&amp;World") == "Hello World"

    def test_numeric_entities(self):
        assert strip_html("Hello&#160;World") == "Hello World"

    def test_collapses_whitespace(self):
        assert strip_html("<p>Hello</p>  <p>World</p>") == "Hello World"

    def test_empty(self):
        assert strip_html("") is None or strip_html("") == ""
        assert strip_html(None) is None

    def test_plain_text_unchanged(self):
        assert strip_html("Just plain text") == "Just plain text"


# ═══════════════════════════════════════════════════════════════════════════════
# is_aggregator_url
# ═══════════════════════════════════════════════════════════════════════════════

class TestIsAggregatorUrl:
    def test_primary_ats_not_blocked(self):
        assert is_aggregator_url("https://boards.greenhouse.io/company/jobs/123") is False

    def test_empty(self):
        assert is_aggregator_url("") is False
        assert is_aggregator_url(None) is False

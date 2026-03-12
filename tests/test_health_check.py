"""Tests for pipeline.qa — cmd_health_check() per-column validation."""
import json
import re
import sqlite3
import pytest
from pipeline.qa import cmd_health_check
from pipeline.db import cmd_init_db
from pipeline.ingest import validate_pre_ingest


@pytest.fixture
def hc_db(tmp_path, monkeypatch):
    """Create a temp DB with schema for health check tests."""
    db_path = tmp_path / "test_hc.db"
    import pipeline.db as db_mod
    original_db_path = db_mod.DB_PATH
    original_review = db_mod.REVIEW_DIR
    db_mod.DB_PATH = db_path
    db_mod.REVIEW_DIR = tmp_path / "review"
    db_mod.REVIEW_DIR.mkdir(exist_ok=True)

    # Also monkeypatch REVIEW_DIR in qa module
    import pipeline.qa as qa_mod
    qa_mod.REVIEW_DIR = db_mod.REVIEW_DIR

    cmd_init_db()

    # Insert companies_200 (exactly 200)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    for i in range(200):
        conn.execute("""
            INSERT INTO companies_200 (company_name, canonical_name, tier, sector)
            VALUES (?, ?, 'Tier1', 'Tech')
        """, (f"Company_{i}", f"company_{i}"))
    conn.commit()
    conn.close()

    yield db_path

    db_mod.DB_PATH = original_db_path
    db_mod.REVIEW_DIR = original_review


def _insert_valid_row(db_path, gold_id=1, **overrides):
    """Insert a valid gold row, optionally overriding fields."""
    defaults = {
        'gold_id': gold_id,
        'company_name': 'TestCorp',
        'company_id': 1,
        'source_platform': 'Greenhouse',
        'source_job_id': f'job_{gold_id}',
        'job_url': f'https://boards.greenhouse.io/test/jobs/{gold_id}',
        'title': 'AI Data Analyst',
        'title_normalized': 'Data Analyst',  # different from title (not verbatim)
        'role_cluster': 'Data Analyst',
        'canonical_job_key': f'key_{gold_id}',
        'description_snippet': 'Looking for an AI analyst with Python and machine learning experience. '
                               'Must have strong analytical skills and experience with data tools.',
        'ai_keywords_hit': '["machine learning"]',
        'posted_date': '2026-01-15',
        'date_uncertain': 0,
        'is_us': 1,
        'status': 'Open',
        'work_mode': 'On-site',
        'location_raw': 'San Francisco, CA',
        'location_standardized': 'San Francisco, CA',
        'url_http_status': 200,
        'salary_min_usd': 120000,
        'salary_max_usd': 160000,
        'skills_extracted': 'Python,SQL,machine learning',
        'has_python': 1,
        'has_sql': 1,
        'has_ai_in_title': 1,
        'title_ai_terms': 'AI',
        'ai_role_signature': 'AI-Titled Analyst',
        'seniority': 'Mid',
        'enrich_status': 'enriched',
    }
    defaults.update(overrides)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cols = ', '.join(defaults.keys())
    placeholders = ', '.join(['?'] * len(defaults))
    conn.execute(f"INSERT INTO job_postings_gold ({cols}) VALUES ({placeholders})",
                 list(defaults.values()))
    conn.commit()
    conn.close()


def _bulk_insert(db_path, count=310):
    """Insert count valid rows for aggregate threshold tests."""
    for i in range(count):
        _insert_valid_row(db_path, gold_id=i+1,
                          canonical_job_key=f'key_{i+1}',
                          source_job_id=f'job_{i+1}')


# ═════════════════════════════════════════════════════════════════════════════
# Health check basics
# ═════════════════════════════════════════════════════════════════════════════

class TestHealthCheckBasics:
    def test_perfect_score(self, hc_db):
        """310 valid rows should produce 100% or near-100% score."""
        _bulk_insert(hc_db, 310)
        score, crit, warn = cmd_health_check()
        assert score >= 99.0
        assert crit == 0

    def test_returns_tuple_of_three(self, hc_db):
        """cmd_health_check returns (score, critical_count, warning_count)."""
        _bulk_insert(hc_db, 310)
        result = cmd_health_check()
        assert len(result) == 3
        score, crit, warn = result
        assert isinstance(score, float)
        assert isinstance(crit, int)
        assert isinstance(warn, int)

    def test_saves_json_report(self, hc_db, tmp_path):
        """Health check should save a JSON report to review dir."""
        _bulk_insert(hc_db, 310)
        cmd_health_check()
        review_dir = tmp_path / "review"
        reports = list(review_dir.glob("health_*.json"))
        assert len(reports) >= 1
        with open(reports[0]) as f:
            data = json.load(f)
        assert 'health_score' in data
        assert 'critical_count' in data
        assert 'warning_count' in data
        assert 'coverage' in data
        assert 'violations' in data

    def test_empty_db_low_score(self, hc_db):
        """Empty DB should trigger row_count_below_300."""
        score, crit, warn = cmd_health_check()
        assert crit >= 1  # row_count_below_300


# ═════════════════════════════════════════════════════════════════════════════
# Required field checks
# ═════════════════════════════════════════════════════════════════════════════

class TestHealthCheckRequired:
    def test_empty_title_is_critical(self, hc_db):
        """Empty title on Open row should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9998, title='',
                          canonical_job_key='key_9998',
                          source_job_id='job_9998')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_empty_company_is_critical(self, hc_db):
        """Empty company_name on Open row should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9997, company_name='',
                          canonical_job_key='key_9997',
                          source_job_id='job_9997')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_empty_job_url_is_critical(self, hc_db):
        """Empty job_url on Open row should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9996, job_url='',
                          canonical_job_key='key_9996',
                          source_job_id='job_9996')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_empty_canonical_key_is_critical(self, hc_db):
        """Empty canonical_job_key should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9995, canonical_job_key='',
                          source_job_id='job_9995')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_is_us_zero_is_critical(self, hc_db):
        """is_us=0 on Open row should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9994, is_us=0,
                          canonical_job_key='key_9994',
                          source_job_id='job_9994')
        score, crit, warn = cmd_health_check()
        assert crit >= 1


# ═════════════════════════════════════════════════════════════════════════════
# Enum checks
# ═════════════════════════════════════════════════════════════════════════════

class TestHealthCheckEnums:
    def test_invalid_platform_is_critical(self, hc_db):
        """Unknown source_platform should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9993,
                          source_platform='FakeATS',
                          canonical_job_key='key_9993',
                          source_job_id='job_9993')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_invalid_work_mode_is_critical(self, hc_db):
        """Invalid work_mode should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9992,
                          work_mode='Telecommute',
                          canonical_job_key='key_9992',
                          source_job_id='job_9992')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_valid_work_modes_ok(self, hc_db):
        """All valid work_modes should not trigger violations."""
        _bulk_insert(hc_db, 310)
        for i, wm in enumerate(['On-site', 'Remote', 'Hybrid', 'Unknown']):
            _insert_valid_row(hc_db, gold_id=8000+i,
                              work_mode=wm,
                              canonical_job_key=f'key_wm_{i}',
                              source_job_id=f'job_wm_{i}')
        score, crit, warn = cmd_health_check()
        assert crit == 0

    def test_api_enriched_is_valid(self, hc_db):
        """'api_enriched' enrich_status should be accepted."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=8010,
                          enrich_status='api_enriched',
                          canonical_job_key='key_enrich_api',
                          source_job_id='job_enrich_api')
        score, crit, warn = cmd_health_check()
        assert crit == 0

    def test_custom_platform_accepted(self, hc_db):
        """Custom platform names like 'Oracle Careers' should be valid."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=8011,
                          source_platform='Oracle Careers',
                          canonical_job_key='key_oracle',
                          source_job_id='job_oracle')
        score, crit, warn = cmd_health_check()
        assert crit == 0

    def test_iso_created_at_accepted(self, hc_db):
        """ISO created_at with T-separator and microseconds should be valid."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=8012,
                          canonical_job_key='key_iso_ca',
                          source_job_id='job_iso_ca')
        # Update created_at to ISO T-format (can't set via _insert_valid_row easily)
        conn = sqlite3.connect(str(hc_db))
        conn.execute(
            "UPDATE job_postings_gold SET created_at='2026-03-04T13:35:48.207736' "
            "WHERE gold_id=8012"
        )
        conn.commit()
        conn.close()
        score, crit, warn = cmd_health_check()
        assert crit == 0


# ═════════════════════════════════════════════════════════════════════════════
# Numeric range checks
# ═════════════════════════════════════════════════════════════════════════════

class TestHealthCheckNumeric:
    def test_404_is_critical(self, hc_db):
        """HTTP 404 should be CRITICAL (dead URL)."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9991,
                          url_http_status=404,
                          canonical_job_key='key_9991',
                          source_job_id='job_9991')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_410_is_critical(self, hc_db):
        """HTTP 410 should be CRITICAL (permanently gone)."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9990,
                          url_http_status=410,
                          canonical_job_key='key_9990',
                          source_job_id='job_9990')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_salary_min_too_low(self, hc_db):
        """salary_min_usd below 15000 should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9989,
                          salary_min_usd=5000, salary_max_usd=10000,
                          canonical_job_key='key_9989',
                          source_job_id='job_9989')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_salary_max_too_high(self, hc_db):
        """salary_max_usd above 600000 should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9988,
                          salary_min_usd=200000, salary_max_usd=700000,
                          canonical_job_key='key_9988',
                          source_job_id='job_9988')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_salary_ratio_extreme(self, hc_db):
        """max/min ratio > 5 should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9987,
                          salary_min_usd=20000, salary_max_usd=150000,
                          canonical_job_key='key_9987',
                          source_job_id='job_9987')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_valid_salary_ok(self, hc_db):
        """Valid salary range should not trigger violations."""
        _bulk_insert(hc_db, 310)
        score, crit, warn = cmd_health_check()
        assert crit == 0


# ═════════════════════════════════════════════════════════════════════════════
# Format checks
# ═════════════════════════════════════════════════════════════════════════════

class TestHealthCheckFormat:
    def test_old_date_is_critical(self, hc_db):
        """Date before 2025-07-01 should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9986,
                          posted_date='2024-01-01',
                          canonical_job_key='key_9986',
                          source_job_id='job_9986')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_future_date_is_critical(self, hc_db):
        """Date after 2026-03-31 should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9985,
                          posted_date='2027-01-01',
                          canonical_job_key='key_9985',
                          source_job_id='job_9985')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_invalid_date_format_is_critical(self, hc_db):
        """Non-parseable date should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9984,
                          posted_date='not-a-date',
                          canonical_job_key='key_9984',
                          source_job_id='job_9984')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_valid_date_ok(self, hc_db):
        """Valid date in window should not trigger violations."""
        _bulk_insert(hc_db, 310)
        score, crit, warn = cmd_health_check()
        assert crit == 0

    def test_invalid_url_format(self, hc_db):
        """URL not starting with http should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9983,
                          job_url='ftp://weird.url/job',
                          canonical_job_key='key_9983',
                          source_job_id='job_9983')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_ai_keywords_csv_accepted(self, hc_db):
        """Plain CSV ai_keywords_hit (e.g. 'machine learning, ai') is valid."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9982,
                          ai_keywords_hit='machine learning, ai',
                          canonical_job_key='key_9982',
                          source_job_id='job_9982')
        score, crit, warn = cmd_health_check()
        # CSV string should NOT generate a format violation
        assert crit == 0


# ═════════════════════════════════════════════════════════════════════════════
# Cross-column consistency
# ═════════════════════════════════════════════════════════════════════════════

class TestHealthCheckCrossColumn:
    def test_work_mode_contradiction(self, hc_db):
        """On-site work_mode + remote in location should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9981,
                          work_mode='On-site',
                          location_raw='Remote, US',
                          canonical_job_key='key_9981',
                          source_job_id='job_9981')
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_salary_inverted(self, hc_db):
        """salary_min > salary_max should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9980,
                          salary_min_usd=200000,
                          salary_max_usd=100000,
                          canonical_job_key='key_9980',
                          source_job_id='job_9980')
        score, crit, warn = cmd_health_check()
        assert crit >= 1


# ═════════════════════════════════════════════════════════════════════════════
# Warning checks (optional fields)
# ═════════════════════════════════════════════════════════════════════════════

class TestHealthCheckWarnings:
    def test_short_description_is_warning(self, hc_db):
        """Short description_snippet should be WARNING."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9979,
                          description_snippet='Short',
                          canonical_job_key='key_9979',
                          source_job_id='job_9979')
        score, crit, warn = cmd_health_check()
        assert warn >= 1

    def test_missing_skills_is_warning(self, hc_db):
        """Empty skills_extracted should be WARNING."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9978,
                          skills_extracted='',
                          canonical_job_key='key_9978',
                          source_job_id='job_9978')
        score, crit, warn = cmd_health_check()
        assert warn >= 1

    def test_missing_posted_date_is_warning(self, hc_db):
        """NULL posted_date should be WARNING."""
        _bulk_insert(hc_db, 310)
        _insert_valid_row(hc_db, gold_id=9977,
                          posted_date=None,
                          canonical_job_key='key_9977',
                          source_job_id='job_9977')
        score, crit, warn = cmd_health_check()
        assert warn >= 1


# ═════════════════════════════════════════════════════════════════════════════
# Aggregate threshold checks
# ═════════════════════════════════════════════════════════════════════════════

class TestHealthCheckAggregates:
    def test_below_300_is_critical(self, hc_db):
        """Less than 300 rows should trigger row_count_below_300."""
        _bulk_insert(hc_db, 50)
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_companies_200_wrong_count(self, hc_db):
        """If companies_200 != 200 rows, CRITICAL."""
        # Delete some companies
        conn = sqlite3.connect(str(hc_db))
        conn.execute("DELETE FROM companies_200 WHERE company_id > 150")
        conn.commit()
        conn.close()
        _bulk_insert(hc_db, 310)
        score, crit, warn = cmd_health_check()
        assert crit >= 1

    def test_duplicate_key_detected(self, hc_db):
        """Duplicate canonical_job_key should be CRITICAL."""
        _bulk_insert(hc_db, 310)
        # Recreate table without UNIQUE constraint
        conn = sqlite3.connect(str(hc_db))
        create_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='job_postings_gold'"
        ).fetchone()[0]
        create_sql_no_unique = re.sub(
            r'(canonical_job_key\s+TEXT\s+NOT\s+NULL)\s+UNIQUE',
            r'\1', create_sql, flags=re.IGNORECASE)
        conn.execute("CREATE TABLE gold_backup AS SELECT * FROM job_postings_gold")
        conn.execute("DROP TABLE job_postings_gold")
        conn.execute(create_sql_no_unique)
        conn.execute("INSERT INTO job_postings_gold SELECT * FROM gold_backup")
        conn.execute("DROP TABLE gold_backup")
        # Insert a duplicate
        conn.execute("""
            INSERT INTO job_postings_gold
            (canonical_job_key, company_name, company_id, source_platform,
             source_job_id, job_url, title, title_normalized, role_cluster,
             description_snippet, ai_keywords_hit, posted_date, date_uncertain,
             is_us, status, work_mode, location_raw, location_standardized,
             url_http_status, salary_min_usd, salary_max_usd,
             skills_extracted, has_python, has_sql, enrich_status)
            VALUES ('key_1', 'TestCorp', 1, 'Greenhouse', 'job_dup',
             'https://boards.greenhouse.io/test/jobs/dup', 'AI Data Analyst',
             'Data Analyst', 'Data Analyst',
             'Looking for an AI analyst with Python and machine learning experience. Must have strong analytical skills.',
             '["machine learning"]', '2026-01-15', 0, 1, 'Open', 'On-site',
             'San Francisco, CA', 'San Francisco, CA', 200, 120000, 160000,
             'Python,SQL', 1, 1, 'enriched')
        """)
        conn.commit()
        conn.close()
        score, crit, warn = cmd_health_check()
        assert crit >= 1


# ═════════════════════════════════════════════════════════════════════════════
# Pre-ingest validation
# ═════════════════════════════════════════════════════════════════════════════

class TestValidatePreIngest:
    def test_valid_row_passes(self):
        """Valid row dict should return empty error list."""
        row = {
            'title': 'AI Analyst',
            'job_url': 'https://example.com/job/1',
            'source_job_id': '12345',
            'company_name': 'TestCorp',
            'status': 'Open',
            'work_mode': 'Remote',
        }
        errors = validate_pre_ingest(row)
        assert errors == []

    def test_missing_title_fails(self):
        """Missing title should return error."""
        row = {
            'title': '',
            'job_url': 'https://example.com/job/1',
            'source_job_id': '12345',
            'company_name': 'TestCorp',
        }
        errors = validate_pre_ingest(row)
        assert any('title' in e for e in errors)

    def test_missing_company_fails(self):
        """Missing company_name should return error."""
        row = {
            'title': 'AI Analyst',
            'job_url': 'https://example.com/job/1',
            'source_job_id': '12345',
            'company_name': '',
        }
        errors = validate_pre_ingest(row)
        assert any('company_name' in e for e in errors)

    def test_missing_url_fails(self):
        """Missing job_url should return error."""
        row = {
            'title': 'AI Analyst',
            'job_url': '',
            'source_job_id': '12345',
            'company_name': 'TestCorp',
        }
        errors = validate_pre_ingest(row)
        assert any('job_url' in e for e in errors)

    def test_invalid_url_format(self):
        """URL not starting with http should fail."""
        row = {
            'title': 'AI Analyst',
            'job_url': 'ftp://invalid/url',
            'source_job_id': '12345',
            'company_name': 'TestCorp',
        }
        errors = validate_pre_ingest(row)
        assert any('url' in e.lower() for e in errors)

    def test_low_salary_fails(self):
        """salary_min_usd below 10000 should fail."""
        row = {
            'title': 'AI Analyst',
            'job_url': 'https://example.com/job/1',
            'source_job_id': '12345',
            'company_name': 'TestCorp',
            'salary_min_usd': 500,
        }
        errors = validate_pre_ingest(row)
        assert any('salary' in e for e in errors)

    def test_inverted_salary_fails(self):
        """salary_min > salary_max should fail."""
        row = {
            'title': 'AI Analyst',
            'job_url': 'https://example.com/job/1',
            'source_job_id': '12345',
            'company_name': 'TestCorp',
            'salary_min_usd': 200000,
            'salary_max_usd': 100000,
        }
        errors = validate_pre_ingest(row)
        assert any('salary' in e for e in errors)

    def test_invalid_status_fails(self):
        """Invalid status enum should fail."""
        row = {
            'title': 'AI Analyst',
            'job_url': 'https://example.com/job/1',
            'source_job_id': '12345',
            'company_name': 'TestCorp',
            'status': 'Maybe',
        }
        errors = validate_pre_ingest(row)
        assert any('status' in e for e in errors)

    def test_invalid_work_mode_fails(self):
        """Invalid work_mode enum should fail."""
        row = {
            'title': 'AI Analyst',
            'job_url': 'https://example.com/job/1',
            'source_job_id': '12345',
            'company_name': 'TestCorp',
            'work_mode': 'Telecommute',
        }
        errors = validate_pre_ingest(row)
        assert any('work_mode' in e for e in errors)

    def test_valid_enums_pass(self):
        """Valid status and work_mode should pass."""
        row = {
            'title': 'AI Analyst',
            'job_url': 'https://example.com/job/1',
            'source_job_id': '12345',
            'company_name': 'TestCorp',
            'status': 'Closed',
            'work_mode': 'Hybrid',
        }
        errors = validate_pre_ingest(row)
        assert errors == []

    def test_null_optional_fields_pass(self):
        """NULL optional fields should not cause errors."""
        row = {
            'title': 'AI Analyst',
            'job_url': 'https://example.com/job/1',
            'source_job_id': '12345',
            'company_name': 'TestCorp',
            'salary_min_usd': None,
            'salary_max_usd': None,
            'status': None,
            'work_mode': None,
        }
        errors = validate_pre_ingest(row)
        assert errors == []

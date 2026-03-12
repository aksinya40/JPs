"""Tests for pipeline.qa — QA rule checks."""
import re
import sqlite3
import pytest
from pipeline.qa import cmd_qa_check
from pipeline.db import cmd_init_db


@pytest.fixture
def qa_db(tmp_path, monkeypatch):
    """Create an in-memory-like temp DB with schema for QA tests."""
    db_path = tmp_path / "test_qa.db"
    # Monkey-patch get_db to use our temp DB
    import pipeline.db as db_mod
    original_db_path = db_mod.DB_PATH
    db_mod.DB_PATH = db_path

    # Initialize schema
    cmd_init_db()

    # Insert companies_200 (need exactly 200 for QA)
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

    # Restore original
    db_mod.DB_PATH = original_db_path


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
        'title_normalized': 'AI Data Analyst',
        'role_cluster': 'Data Analyst',
        'canonical_job_key': f'key_{gold_id}',
        'description_snippet': 'Looking for an AI analyst with Python and machine learning experience.',
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


# ═════════════════════════════════════════════════════════════════════════════
# Row count checks
# ═════════════════════════════════════════════════════════════════════════════

class TestQaRowCount:
    def test_below_300_is_critical(self, qa_db):
        """Less than 300 active rows should trigger CRITICAL."""
        # Insert only 50 rows
        for i in range(50):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        crit, warn = cmd_qa_check()
        assert crit >= 1  # row_count_below_300

    def test_above_300_no_row_count_critical(self, qa_db):
        """300+ active rows should not trigger row_count_below_300."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        crit, warn = cmd_qa_check()
        # Should have 0 CRITICAL (assuming all rows are valid)
        assert crit == 0


# ═════════════════════════════════════════════════════════════════════════════
# Duplicate job key
# ═════════════════════════════════════════════════════════════════════════════

class TestQaDuplicateKey:
    def test_schema_prevents_duplicate_key(self, qa_db):
        """Schema UNIQUE constraint should prevent duplicate canonical_job_key."""
        _insert_valid_row(qa_db, gold_id=1,
                          canonical_job_key='key_1',
                          source_job_id='job_1')
        with pytest.raises(sqlite3.IntegrityError):
            _insert_valid_row(qa_db, gold_id=2,
                              canonical_job_key='key_1',
                              source_job_id='job_2')

    def test_duplicate_key_detected_by_qa(self, qa_db):
        """QA check detects duplicates if they exist (e.g. from migration)."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        # Simulate a duplicate by recreating the table without UNIQUE constraint.
        # We dynamically get the schema so it works regardless of migration columns.
        conn = sqlite3.connect(str(qa_db))
        # Get the original CREATE TABLE SQL from sqlite_master
        create_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='job_postings_gold'"
        ).fetchone()[0]
        # Remove UNIQUE from canonical_job_key column definition
        create_sql_no_unique = re.sub(
            r'(canonical_job_key\s+TEXT\s+NOT\s+NULL)\s+UNIQUE',
            r'\1',
            create_sql,
            flags=re.IGNORECASE,
        )
        # Backup, drop, recreate without UNIQUE, restore
        conn.execute("CREATE TABLE gold_backup AS SELECT * FROM job_postings_gold")
        conn.execute("DROP TABLE job_postings_gold")
        conn.execute(create_sql_no_unique)
        conn.execute("INSERT INTO job_postings_gold SELECT * FROM gold_backup")
        conn.execute("DROP TABLE gold_backup")
        # Now insert a genuine duplicate key
        conn.execute("""
            INSERT INTO job_postings_gold
            (canonical_job_key, company_name, company_id, source_platform,
             source_job_id, job_url, title, title_normalized, role_cluster,
             description_snippet, ai_keywords_hit, posted_date, date_uncertain,
             is_us, status, work_mode, location_raw, location_standardized,
             url_http_status, salary_min_usd, salary_max_usd)
            VALUES ('key_1', 'TestCorp', 1, 'Greenhouse', 'job_dup',
             'https://boards.greenhouse.io/test/jobs/dup', 'AI Data Analyst',
             'AI Data Analyst', 'Data Analyst',
             'Looking for an AI analyst with Python and machine learning experience.',
             '["machine learning"]', '2026-01-15', 0, 1, 'Open', 'On-site',
             'San Francisco, CA', 'San Francisco, CA', 200, 120000, 160000)
        """)
        conn.commit()
        conn.close()
        crit, warn = cmd_qa_check()
        assert crit >= 1


# ═════════════════════════════════════════════════════════════════════════════
# Required field null
# ═════════════════════════════════════════════════════════════════════════════

class TestQaRequiredFields:
    def test_empty_title_is_critical(self, qa_db):
        """Empty title on Open row should be CRITICAL."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        # One with empty title (schema has NOT NULL, so use empty string)
        _insert_valid_row(qa_db, gold_id=9998,
                          title='',
                          canonical_job_key='key_9998',
                          source_job_id='job_9998')
        crit, warn = cmd_qa_check()
        assert crit >= 1

    def test_empty_company_is_critical(self, qa_db):
        """Empty company_name on Open row should be CRITICAL."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        # Schema has NOT NULL, so use empty string instead of None
        _insert_valid_row(qa_db, gold_id=9997,
                          company_name='',
                          canonical_job_key='key_9997',
                          source_job_id='job_9997')
        crit, warn = cmd_qa_check()
        assert crit >= 1


# ═════════════════════════════════════════════════════════════════════════════
# URL not reachable
# ═════════════════════════════════════════════════════════════════════════════

class TestQaUrlStatus:
    def test_404_is_critical(self, qa_db):
        """HTTP 404 should be CRITICAL."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        _insert_valid_row(qa_db, gold_id=9996,
                          url_http_status=404,
                          canonical_job_key='key_9996',
                          source_job_id='job_9996')
        crit, warn = cmd_qa_check()
        assert crit >= 1

    def test_403_is_warning(self, qa_db):
        """HTTP 403 should be WARNING (transient)."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        _insert_valid_row(qa_db, gold_id=9995,
                          url_http_status=403,
                          canonical_job_key='key_9995',
                          source_job_id='job_9995')
        crit, warn = cmd_qa_check()
        assert warn >= 1

    def test_200_is_ok(self, qa_db):
        """HTTP 200 should not trigger any violation."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        crit, warn = cmd_qa_check()
        assert crit == 0


# ═════════════════════════════════════════════════════════════════════════════
# No AI signal
# ═════════════════════════════════════════════════════════════════════════════

class TestQaAiSignal:
    def test_empty_ai_keywords_is_critical(self, qa_db):
        """Empty ai_keywords_hit on Open row should be CRITICAL."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        _insert_valid_row(qa_db, gold_id=9994,
                          ai_keywords_hit='',
                          canonical_job_key='key_9994',
                          source_job_id='job_9994')
        crit, warn = cmd_qa_check()
        assert crit >= 1


# ═════════════════════════════════════════════════════════════════════════════
# Date out of window
# ═════════════════════════════════════════════════════════════════════════════

class TestQaDateWindow:
    def test_old_date_is_critical(self, qa_db):
        """Date before 2025-07-01 should be CRITICAL."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        _insert_valid_row(qa_db, gold_id=9993,
                          posted_date='2024-01-01',
                          canonical_job_key='key_9993',
                          source_job_id='job_9993')
        crit, warn = cmd_qa_check()
        assert crit >= 1

    def test_valid_date_is_ok(self, qa_db):
        """Date within window should not trigger violation."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        crit, warn = cmd_qa_check()
        assert crit == 0


# ═════════════════════════════════════════════════════════════════════════════
# Warning checks
# ═════════════════════════════════════════════════════════════════════════════

class TestQaWarnings:
    def test_unknown_company_is_warning(self, qa_db):
        """company_name='Unknown' should trigger WARNING."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        _insert_valid_row(qa_db, gold_id=9992,
                          company_name='Unknown',
                          canonical_job_key='key_9992',
                          source_job_id='job_9992')
        crit, warn = cmd_qa_check()
        assert warn >= 1

    def test_html_in_snippet_is_warning(self, qa_db):
        """HTML tags in description_snippet should trigger WARNING."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        _insert_valid_row(qa_db, gold_id=9991,
                          description_snippet='<p>Looking for <b>analyst</b></p>',
                          canonical_job_key='key_9991',
                          source_job_id='job_9991')
        crit, warn = cmd_qa_check()
        assert warn >= 1

    def test_insane_salary_is_warning(self, qa_db):
        """Salary min < 15000 should trigger WARNING."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        _insert_valid_row(qa_db, gold_id=9990,
                          salary_min_usd=5000,
                          salary_max_usd=10000,
                          canonical_job_key='key_9990',
                          source_job_id='job_9990')
        crit, warn = cmd_qa_check()
        assert warn >= 1

    def test_missing_description_is_warning(self, qa_db):
        """Short or NULL description_snippet should trigger WARNING."""
        for i in range(310):
            _insert_valid_row(qa_db, gold_id=i+1,
                              canonical_job_key=f'key_{i+1}',
                              source_job_id=f'job_{i+1}')
        _insert_valid_row(qa_db, gold_id=9989,
                          description_snippet='Short',
                          canonical_job_key='key_9989',
                          source_job_id='job_9989')
        crit, warn = cmd_qa_check()
        assert warn >= 1

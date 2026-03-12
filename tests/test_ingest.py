"""Tests for pipeline.ingest — location parsing, seniority detection, pre-ingest gate."""
import sqlite3
from pathlib import Path

import pytest
from pipeline.ingest import _parse_location, _detect_seniority, cmd_ingest_raw


# ═════════════════════════════════════════════════════════════════════════════
# _parse_location tests
# ═════════════════════════════════════════════════════════════════════════════

class TestParseLocationUS:
    """US location parsing — should return is_us=True."""

    def test_city_state_abbreviation(self):
        """'San Francisco, CA' → US, city, California."""
        is_us, city, state, std, wm = _parse_location("San Francisco, CA")
        assert is_us is True
        assert city == "San Francisco"
        assert state == "California"
        assert "San Francisco" in std
        assert "California" in std

    def test_city_state_full(self):
        """'Austin, Texas' → US."""
        is_us, city, state, std, wm = _parse_location("Austin, Texas")
        assert is_us is True

    def test_city_only_known(self):
        """Known US city without state → US + inferred state."""
        is_us, city, state, std, wm = _parse_location("Seattle")
        assert is_us is True
        assert city == "Seattle"
        assert state == "Washington"

    def test_usa_marker(self):
        """'United States' → US."""
        is_us, city, state, std, wm = _parse_location("United States")
        assert is_us is True

    def test_us_abbreviation(self):
        """'New York, NY' → US."""
        is_us, city, state, std, wm = _parse_location("New York, NY")
        assert is_us is True
        assert city == "New York"

    def test_empty_defaults_to_us(self):
        """Empty location → default US."""
        is_us, city, state, std, wm = _parse_location("")
        assert is_us is True
        assert wm == "Unknown"

    def test_none_defaults_to_us(self):
        """None location → default US."""
        is_us, city, state, std, wm = _parse_location(None)
        assert is_us is True


class TestParseLocationNonUS:
    """Non-US location parsing — should return is_us=False."""

    def test_london(self):
        is_us, city, state, std, wm = _parse_location("London, UK")
        assert is_us is False

    def test_toronto(self):
        is_us, city, state, std, wm = _parse_location("Toronto, Canada")
        assert is_us is False

    def test_bangalore(self):
        is_us, city, state, std, wm = _parse_location("Bangalore, India")
        assert is_us is False

    def test_germany(self):
        is_us, city, state, std, wm = _parse_location("Berlin, Germany")
        assert is_us is False


class TestParseLocationWorkMode:
    """Work mode detection from location text."""

    def test_remote_keyword(self):
        is_us, city, state, std, wm = _parse_location("Remote - US")
        assert wm == "Remote"

    def test_hybrid_keyword(self):
        is_us, city, state, std, wm = _parse_location("San Francisco, CA (Hybrid)")
        assert wm == "Hybrid"

    def test_onsite_default(self):
        is_us, city, state, std, wm = _parse_location("San Francisco, CA")
        assert wm == "On-site"

    def test_remote_no_country(self):
        """Remote without country markers → default US."""
        is_us, city, state, std, wm = _parse_location("Remote")
        assert is_us is True
        assert wm == "Remote"


# ═════════════════════════════════════════════════════════════════════════════
# _detect_seniority tests
# ═════════════════════════════════════════════════════════════════════════════

class TestDetectSeniority:
    """Seniority level detection from job titles."""

    def test_senior(self):
        assert _detect_seniority("Senior Data Analyst") == "Senior"

    def test_sr_abbreviation(self):
        assert _detect_seniority("Sr. Machine Learning Engineer") == "Senior"

    def test_junior(self):
        assert _detect_seniority("Junior Data Scientist") == "Junior"

    def test_jr_abbreviation(self):
        assert _detect_seniority("Jr. Analyst") == "Junior"

    def test_entry_level(self):
        assert _detect_seniority("Entry-Level AI Analyst") == "Junior"

    def test_associate(self):
        assert _detect_seniority("Associate Data Scientist") == "Junior"

    def test_lead(self):
        assert _detect_seniority("Lead AI Engineer") == "Lead"

    def test_staff(self):
        assert _detect_seniority("Staff Machine Learning Engineer") == "Staff"

    def test_principal(self):
        assert _detect_seniority("Principal Data Scientist") == "Principal"

    def test_director(self):
        assert _detect_seniority("Director of Analytics") == "Director"

    def test_vp(self):
        assert _detect_seniority("VP of Data Science") == "VP"

    def test_vice_president(self):
        assert _detect_seniority("Vice President, AI/ML") == "VP"

    def test_manager(self):
        assert _detect_seniority("Manager, Data Analytics") == "Manager"

    def test_head_of(self):
        assert _detect_seniority("Head of Machine Learning") == "Manager"

    def test_mid_default(self):
        assert _detect_seniority("Data Analyst") == "Mid"

    def test_empty(self):
        assert _detect_seniority("") == "Mid"

    def test_none(self):
        assert _detect_seniority(None) == "Mid"


# ═════════════════════════════════════════════════════════════════════════════
# Seniority priority tests — VP > Director > Principal > etc.
# ═════════════════════════════════════════════════════════════════════════════

class TestSeniorityPriority:
    """Ensure highest seniority wins when multiple keywords appear."""

    def test_vp_beats_manager(self):
        assert _detect_seniority("VP & Manager, Analytics") == "VP"

    def test_director_beats_senior(self):
        assert _detect_seniority("Director, Senior Data Science") == "Director"

    def test_principal_beats_lead(self):
        assert _detect_seniority("Principal Lead Engineer") == "Principal"


# ═════════════════════════════════════════════════════════════════════════════
# Pre-ingest gate integration tests — verify cmd_ingest_raw rejects bad rows
# ═════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def ingest_db(tmp_path, monkeypatch):
    """In-memory-like temp DB with schema for ingest tests."""
    import pipeline.db as db_mod
    import pipeline.ingest as ingest_mod

    db_path = tmp_path / "test.db"
    monkeypatch.setattr(db_mod, 'DB_PATH', db_path)
    monkeypatch.setattr(ingest_mod, 'get_db',
                        lambda: db_mod.get_db())

    from pipeline.db import cmd_init_db
    cmd_init_db()

    # Insert a company so company_lookup works
    conn = db_mod.get_db()
    conn.execute("""
        INSERT INTO companies_200 (company_name, canonical_name, tier, sector, in_scope)
        VALUES ('TestCorp', 'testcorp', 1, 'Tech', 1)
    """)
    conn.commit()
    conn.close()
    return db_path


def _insert_raw_posting(db_path, **overrides):
    """Insert a raw_posting row into raw_postings with sensible defaults."""
    defaults = {
        'company_name': 'TestCorp',
        'source_platform': 'Greenhouse',
        'source_job_id': 'job-001',
        'job_url': 'https://boards.greenhouse.io/testcorp/jobs/001',
        'title': 'AI Data Analyst',
        'location_raw': 'San Francisco, CA',
        'body_raw': 'We are looking for an AI data analyst with machine learning experience.',
        'posted_date': '2025-12-01',
        'salary_text': '',
        'processed': 0,
    }
    defaults.update(overrides)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        INSERT INTO raw_postings
        (company_name, source_platform, source_job_id, job_url, title,
         location_raw, body_raw, posted_date, salary_text, processed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        defaults['company_name'], defaults['source_platform'],
        defaults['source_job_id'], defaults['job_url'], defaults['title'],
        defaults['location_raw'], defaults['body_raw'],
        defaults['posted_date'], defaults['salary_text'], defaults['processed'],
    ))
    conn.commit()
    raw_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return raw_id


class TestIngestPreIngestGate:
    """Verify cmd_ingest_raw() calls validate_pre_ingest() and rejects bad rows."""

    def test_valid_row_is_inserted(self, ingest_db):
        """A valid raw posting should be promoted to gold."""
        _insert_raw_posting(ingest_db)
        cmd_ingest_raw()

        conn = sqlite3.connect(str(ingest_db))
        gold_count = conn.execute(
            "SELECT COUNT(*) FROM job_postings_gold"
        ).fetchone()[0]
        conn.close()
        assert gold_count == 1

    def test_missing_title_rejected(self, ingest_db):
        """Row with empty title should be rejected by pre-ingest gate."""
        raw_id = _insert_raw_posting(ingest_db, title='', source_job_id='job-bad-1')
        cmd_ingest_raw()

        conn = sqlite3.connect(str(ingest_db))
        # Should NOT be in gold
        gold_count = conn.execute(
            "SELECT COUNT(*) FROM job_postings_gold"
        ).fetchone()[0]
        # raw_postings.processed should be 5 (rejected)
        processed = conn.execute(
            "SELECT processed FROM raw_postings WHERE raw_id = ?", (raw_id,)
        ).fetchone()[0]
        conn.close()
        assert gold_count == 0
        assert processed == 5

    def test_missing_url_rejected(self, ingest_db):
        """Row with empty job_url should be rejected."""
        raw_id = _insert_raw_posting(ingest_db, job_url='', source_job_id='job-bad-2')
        cmd_ingest_raw()

        conn = sqlite3.connect(str(ingest_db))
        gold_count = conn.execute(
            "SELECT COUNT(*) FROM job_postings_gold"
        ).fetchone()[0]
        processed = conn.execute(
            "SELECT processed FROM raw_postings WHERE raw_id = ?", (raw_id,)
        ).fetchone()[0]
        conn.close()
        assert gold_count == 0
        assert processed == 5

    def test_invalid_url_format_rejected(self, ingest_db):
        """Row with non-http URL should be rejected."""
        raw_id = _insert_raw_posting(
            ingest_db, job_url='ftp://bad.com/job', source_job_id='job-bad-3'
        )
        cmd_ingest_raw()

        conn = sqlite3.connect(str(ingest_db))
        gold_count = conn.execute(
            "SELECT COUNT(*) FROM job_postings_gold"
        ).fetchone()[0]
        processed = conn.execute(
            "SELECT processed FROM raw_postings WHERE raw_id = ?", (raw_id,)
        ).fetchone()[0]
        conn.close()
        assert gold_count == 0
        assert processed == 5

    def test_valid_row_processed_as_1(self, ingest_db):
        """Valid row should get processed=1 after successful insert."""
        raw_id = _insert_raw_posting(ingest_db)
        cmd_ingest_raw()

        conn = sqlite3.connect(str(ingest_db))
        processed = conn.execute(
            "SELECT processed FROM raw_postings WHERE raw_id = ?", (raw_id,)
        ).fetchone()[0]
        conn.close()
        assert processed == 1

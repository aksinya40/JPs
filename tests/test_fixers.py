"""Tests for pipeline.fixers — data quality fixes and backfills."""
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest
from pipeline.db import cmd_init_db
from pipeline.fixers import (
    cmd_normalize_platforms,
    cmd_backfill_title_ai,
    cmd_backfill_ai_role_signature,
    cmd_fix_data_quality,
)


def _make_db(tmp_path):
    """Create a temp DB with full schema."""
    db_path = tmp_path / "test.db"
    with patch('pipeline.db.DB_PATH', db_path):
        cmd_init_db()
    return db_path


def _get_conn(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _insert_gold_row(db_path, **overrides):
    """Insert a valid gold row with sensible defaults using a fresh connection."""
    defaults = {
        'canonical_job_key': 'key_' + str(hash(frozenset(overrides.items()))),
        'company_name': 'Acme Corp',
        'source_platform': 'Greenhouse',
        'source_job_id': '12345',
        'job_url': 'https://boards.greenhouse.io/acme/jobs/12345',
        'title': 'AI Data Analyst',
        'title_normalized': 'AI Data Analyst',
        'role_cluster': 'AI Analyst',
        'is_us': 1,
        'status': 'Open',
        'enrich_status': 'pending',
    }
    defaults.update(overrides)
    cols = ', '.join(defaults.keys())
    placeholders = ', '.join('?' for _ in defaults)
    conn = _get_conn(db_path)
    conn.execute(
        f"INSERT INTO job_postings_gold ({cols}) VALUES ({placeholders})",
        list(defaults.values())
    )
    conn.commit()
    conn.close()


@pytest.fixture
def fixer_db(tmp_path):
    return _make_db(tmp_path)


# ═══════════════════════════════════════════════════════════════════════════════
# cmd_normalize_platforms
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalizePlatforms:

    def test_canonicalizes_platform(self, fixer_db):
        _insert_gold_row(fixer_db, canonical_job_key='k1', source_platform='greenhouse.io')

        with patch('pipeline.fixers.get_db', return_value=_get_conn(fixer_db)):
            cmd_normalize_platforms()

        conn = _get_conn(fixer_db)
        row = conn.execute("SELECT source_platform FROM job_postings_gold").fetchone()
        conn.close()
        assert row[0] == 'Greenhouse'

    def test_already_canonical_unchanged(self, fixer_db):
        _insert_gold_row(fixer_db, canonical_job_key='k2', source_platform='Greenhouse')

        with patch('pipeline.fixers.get_db', return_value=_get_conn(fixer_db)):
            cmd_normalize_platforms()

        conn = _get_conn(fixer_db)
        row = conn.execute("SELECT source_platform FROM job_postings_gold").fetchone()
        conn.close()
        assert row[0] == 'Greenhouse'


# ═══════════════════════════════════════════════════════════════════════════════
# cmd_backfill_title_ai
# ═══════════════════════════════════════════════════════════════════════════════

class TestBackfillTitleAI:

    def test_sets_has_ai_in_title(self, fixer_db):
        _insert_gold_row(fixer_db, canonical_job_key='k3', title='AI Data Analyst')

        with patch('pipeline.fixers.get_db', return_value=_get_conn(fixer_db)):
            cmd_backfill_title_ai()

        conn = _get_conn(fixer_db)
        row = conn.execute("SELECT has_ai_in_title, title_ai_terms FROM job_postings_gold").fetchone()
        conn.close()
        assert row[0] == 1
        assert row[1] is not None

    def test_no_ai_in_title(self, fixer_db):
        _insert_gold_row(fixer_db, canonical_job_key='k4', title='Data Analyst')

        with patch('pipeline.fixers.get_db', return_value=_get_conn(fixer_db)):
            cmd_backfill_title_ai()

        conn = _get_conn(fixer_db)
        row = conn.execute("SELECT has_ai_in_title FROM job_postings_gold").fetchone()
        conn.close()
        assert row[0] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# cmd_backfill_ai_role_signature
# ═══════════════════════════════════════════════════════════════════════════════

class TestBackfillAIRoleSignature:

    def test_classifies_role(self, fixer_db):
        _insert_gold_row(
            fixer_db, canonical_job_key='k5',
            title='AI Data Analyst',
            description_snippet='Work with LLM and machine learning models',
            skills_extracted='Python,SQL',
        )

        with patch('pipeline.fixers.get_db', return_value=_get_conn(fixer_db)):
            cmd_backfill_ai_role_signature()

        conn = _get_conn(fixer_db)
        row = conn.execute("SELECT ai_role_signature FROM job_postings_gold").fetchone()
        conn.close()
        assert row[0] is not None


# ═══════════════════════════════════════════════════════════════════════════════
# cmd_fix_data_quality (key steps)
# ═══════════════════════════════════════════════════════════════════════════════

class TestFixDataQuality:

    def test_deletes_non_us_rows(self, fixer_db):
        """Step 1: Non-US rows should be deleted."""
        _insert_gold_row(fixer_db, canonical_job_key='us1', is_us=1)
        _insert_gold_row(fixer_db, canonical_job_key='nonus1', is_us=0)

        with patch('pipeline.fixers.get_db', return_value=_get_conn(fixer_db)), \
             patch('pipeline.fixers.backup_db'):
            cmd_fix_data_quality()

        conn = _get_conn(fixer_db)
        count = conn.execute("SELECT COUNT(*) FROM job_postings_gold WHERE is_us=0").fetchone()[0]
        us_count = conn.execute("SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1").fetchone()[0]
        conn.close()
        assert count == 0
        assert us_count >= 1

    def test_deletes_aggregator_urls(self, fixer_db):
        """Step 2: Aggregator URLs should be deleted."""
        _insert_gold_row(fixer_db, canonical_job_key='good1',
                         job_url='https://boards.greenhouse.io/acme/jobs/1')
        _insert_gold_row(fixer_db, canonical_job_key='bad1',
                         job_url='https://builtin.com/jobs/12345')

        with patch('pipeline.fixers.get_db', return_value=_get_conn(fixer_db)), \
             patch('pipeline.fixers.backup_db'):
            cmd_fix_data_quality()

        conn = _get_conn(fixer_db)
        rows = conn.execute("SELECT job_url FROM job_postings_gold").fetchall()
        conn.close()
        urls = [r[0] for r in rows]
        assert not any('builtin.com' in u for u in urls)

    def test_strips_html_from_snippets(self, fixer_db):
        """Step 4: HTML in description_snippet should be stripped."""
        _insert_gold_row(fixer_db, canonical_job_key='html1',
                         description_snippet='<p>Looking for an <b>analyst</b></p>')

        with patch('pipeline.fixers.get_db', return_value=_get_conn(fixer_db)), \
             patch('pipeline.fixers.backup_db'):
            cmd_fix_data_quality()

        conn = _get_conn(fixer_db)
        row = conn.execute("SELECT description_snippet FROM job_postings_gold").fetchone()
        conn.close()
        assert '<p>' not in (row[0] or '')
        assert '<b>' not in (row[0] or '')

"""Tests for pipeline.enrichment — salary mining and HTTP helpers."""
import json
import sqlite3
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from pipeline.db import cmd_init_db
from pipeline.enrichment import (
    _http_get_json,
    _http_get_html,
    cmd_mine_salary_from_body,
)


def _make_db(tmp_path):
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
    defaults = {
        'canonical_job_key': 'key_' + str(hash(frozenset(overrides.items()))),
        'company_name': 'Test Corp',
        'source_platform': 'Greenhouse',
        'source_job_id': '999',
        'job_url': 'https://boards.greenhouse.io/test/jobs/999',
        'title': 'Data Analyst',
        'title_normalized': 'Data Analyst',
        'role_cluster': 'Analyst',
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


# ═══════════════════════════════════════════════════════════════════════════════
# _http_get_json
# ═══════════════════════════════════════════════════════════════════════════════

class TestHttpGetJson:

    @patch('pipeline.enrichment.urllib.request.urlopen')
    def test_success(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({'key': 'value'}).encode()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        data, status = _http_get_json('https://example.com/api')
        assert data == {'key': 'value'}
        assert status == 200

    @patch('pipeline.enrichment.urllib.request.urlopen')
    def test_http_error(self, mock_urlopen):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.HTTPError(
            'https://example.com', 404, 'Not Found', {}, None
        )
        data, status = _http_get_json('https://example.com/api')
        assert data is None
        assert status == 404

    @patch('pipeline.enrichment.urllib.request.urlopen')
    def test_connection_error(self, mock_urlopen):
        mock_urlopen.side_effect = ConnectionError("timeout")
        data, status = _http_get_json('https://example.com/api')
        assert data is None
        assert status == 0


# ═══════════════════════════════════════════════════════════════════════════════
# _http_get_html
# ═══════════════════════════════════════════════════════════════════════════════

class TestHttpGetHtml:

    @patch('pipeline.enrichment.urllib.request.urlopen')
    def test_success(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'<html><body>Hello</body></html>'
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        html, status = _http_get_html('https://example.com')
        assert 'Hello' in html
        assert status == 200

    @patch('pipeline.enrichment.urllib.request.urlopen')
    def test_404(self, mock_urlopen):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.HTTPError(
            'https://example.com', 404, 'Not Found', {}, None
        )
        html, status = _http_get_html('https://example.com')
        assert html is None
        assert status == 404


# ═══════════════════════════════════════════════════════════════════════════════
# cmd_mine_salary_from_body
# ═══════════════════════════════════════════════════════════════════════════════

class TestMineSalaryFromBody:

    def test_extracts_salary(self, tmp_path):
        db_path = _make_db(tmp_path)
        _insert_gold_row(
            db_path, canonical_job_key='sal1',
            description_snippet='Salary range: $120,000 - $160,000 per year. Looking for experienced analyst.',
            salary_min_usd=None,
        )

        with patch('pipeline.enrichment.get_db', return_value=_get_conn(db_path)):
            cmd_mine_salary_from_body()

        conn = _get_conn(db_path)
        row = conn.execute(
            "SELECT salary_min_usd, salary_max_usd FROM job_postings_gold WHERE canonical_job_key='sal1'"
        ).fetchone()
        conn.close()
        assert row[0] is not None
        assert row[1] is not None

    def test_no_salary_in_text(self, tmp_path):
        db_path = _make_db(tmp_path)
        _insert_gold_row(
            db_path, canonical_job_key='nosal1',
            description_snippet='We are looking for a data analyst to join our team with great benefits.',
            salary_min_usd=None,
        )

        with patch('pipeline.enrichment.get_db', return_value=_get_conn(db_path)):
            cmd_mine_salary_from_body()

        conn = _get_conn(db_path)
        row = conn.execute(
            "SELECT salary_min_usd FROM job_postings_gold WHERE canonical_job_key='nosal1'"
        ).fetchone()
        conn.close()
        assert row[0] is None

    def test_skips_rows_with_existing_salary(self, tmp_path):
        db_path = _make_db(tmp_path)
        _insert_gold_row(
            db_path, canonical_job_key='existing1',
            description_snippet='$200,000 salary offered for this role with great benefits.',
            salary_min_usd=100000,
            salary_max_usd=150000,
        )

        with patch('pipeline.enrichment.get_db', return_value=_get_conn(db_path)):
            cmd_mine_salary_from_body()

        conn = _get_conn(db_path)
        row = conn.execute(
            "SELECT salary_min_usd FROM job_postings_gold WHERE canonical_job_key='existing1'"
        ).fetchone()
        conn.close()
        assert row[0] == 100000

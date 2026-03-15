"""Tests for pipeline.export — CSV export and QA report helpers."""
import csv
import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest
from pipeline.db import cmd_init_db
from pipeline.export import _export_csv, _build_qa_report, _count_csv_field


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


def _insert_gold_row(conn, **overrides):
    defaults = {
        'canonical_job_key': 'key_' + str(id(overrides)),
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
    conn.execute(
        f"INSERT INTO job_postings_gold ({cols}) VALUES ({placeholders})",
        list(defaults.values())
    )
    conn.commit()


# ═══════════════════════════════════════════════════════════════════════════════
# _export_csv
# ═══════════════════════════════════════════════════════════════════════════════

class TestExportCsv:

    def test_exports_rows(self, tmp_path):
        db_path = _make_db(tmp_path)
        conn = _get_conn(db_path)
        _insert_gold_row(conn, canonical_job_key='exp1')
        _insert_gold_row(conn, canonical_job_key='exp2')

        rows = conn.execute("SELECT * FROM job_postings_gold").fetchall()
        csv_path = tmp_path / "test.csv"
        _export_csv(rows, csv_path)

        with open(csv_path) as f:
            reader = csv.DictReader(f)
            exported = list(reader)
        assert len(exported) == 2

    def test_empty_rows(self, tmp_path):
        csv_path = tmp_path / "empty.csv"
        _export_csv([], csv_path)
        assert csv_path.exists()
        content = csv_path.read_text()
        assert content == ''

    def test_csv_has_header(self, tmp_path):
        db_path = _make_db(tmp_path)
        conn = _get_conn(db_path)
        _insert_gold_row(conn, canonical_job_key='hdr1')

        rows = conn.execute("SELECT * FROM job_postings_gold").fetchall()
        csv_path = tmp_path / "header.csv"
        _export_csv(rows, csv_path)

        with open(csv_path) as f:
            reader = csv.reader(f)
            header = next(reader)
        assert 'gold_id' in header
        assert 'title' in header


# ═══════════════════════════════════════════════════════════════════════════════
# _build_qa_report
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildQaReport:

    def test_returns_dict(self, tmp_path):
        db_path = _make_db(tmp_path)
        conn = _get_conn(db_path)
        _insert_gold_row(conn, canonical_job_key='qa1')
        cur = conn.cursor()

        report = _build_qa_report(cur)
        assert isinstance(report, dict)
        assert 'total_gold_rows' in report
        assert report['total_gold_rows'] == 1

    def test_empty_db(self, tmp_path):
        db_path = _make_db(tmp_path)
        conn = _get_conn(db_path)
        cur = conn.cursor()

        report = _build_qa_report(cur)
        assert report['total_gold_rows'] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# _count_csv_field
# ═══════════════════════════════════════════════════════════════════════════════

class TestCountCsvField:

    def test_counts_csv_values(self, tmp_path):
        db_path = _make_db(tmp_path)
        conn = _get_conn(db_path)
        _insert_gold_row(conn, canonical_job_key='csv1', skills_extracted='Python,SQL,Python')
        _insert_gold_row(conn, canonical_job_key='csv2', skills_extracted='Python,R')
        cur = conn.cursor()

        result = _count_csv_field(cur, 'skills_extracted', 5)
        assert isinstance(result, dict)
        # Python appears in both rows
        assert 'Python' in result

    def test_empty_field(self, tmp_path):
        db_path = _make_db(tmp_path)
        conn = _get_conn(db_path)
        _insert_gold_row(conn, canonical_job_key='empty1', skills_extracted=None)
        cur = conn.cursor()

        result = _count_csv_field(cur, 'skills_extracted', 5)
        assert isinstance(result, dict)

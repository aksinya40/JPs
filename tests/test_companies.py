"""Tests for pipeline.companies — company list builder."""
import sqlite3
from unittest.mock import patch

import pytest
from pipeline.db import cmd_init_db
from pipeline.companies import cmd_build_companies, COMPANIES


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


class TestCmdBuildCompanies:

    def test_populates_companies(self, tmp_path):
        db_path = _make_db(tmp_path)

        with patch('pipeline.companies.get_db', return_value=_get_conn(db_path)):
            cmd_build_companies()

        conn = _get_conn(db_path)
        count = conn.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
        conn.close()
        assert count > 0

    def test_idempotent(self, tmp_path):
        """Running twice should not duplicate companies."""
        db_path = _make_db(tmp_path)

        with patch('pipeline.companies.get_db', return_value=_get_conn(db_path)):
            cmd_build_companies()

        conn = _get_conn(db_path)
        count1 = conn.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
        conn.close()

        with patch('pipeline.companies.get_db', return_value=_get_conn(db_path)):
            cmd_build_companies()

        conn = _get_conn(db_path)
        count2 = conn.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
        conn.close()

        assert count1 == count2

    def test_companies_list_has_entries(self):
        assert len(COMPANIES) > 100

    def test_company_tuple_structure(self):
        for c in COMPANIES:
            assert len(c) == 5, f"Company {c[0]} should be 5-tuple"
            name, tier, sector, ats, slug = c
            assert isinstance(name, str)
            assert tier in ('Tier1', 'Tier2', 'Tier3', 'Tier4')

    def test_all_tiers_represented(self):
        tiers = {c[1] for c in COMPANIES}
        assert tiers == {'Tier1', 'Tier2', 'Tier3', 'Tier4'}

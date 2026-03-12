"""Shared test fixtures for the pipeline test suite."""
import sqlite3
import pytest
import sys
from pathlib import Path

# Ensure pipeline package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def in_memory_db():
    """Provide a fresh in-memory SQLite database with WAL-like settings."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    yield conn
    conn.close()


@pytest.fixture
def sample_job_row():
    """A representative gold-table row dict for unit tests."""
    return {
        "gold_id": 1,
        "title": "Senior AI Data Analyst",
        "title_normalized": "AI Data Analyst",
        "company_name": "Acme Corp",
        "canonical_company": "acme corp",
        "url": "https://boards.greenhouse.io/acme/jobs/12345",
        "source_platform": "Greenhouse",
        "source_job_id": "12345",
        "posted_date": "2026-03-01",
        "description_snippet": "We are looking for an AI Data Analyst with Python and SQL skills.",
        "salary_min_usd": 120000,
        "salary_max_usd": 160000,
        "salary_period": "yearly",
        "location_raw": "San Francisco, CA",
        "state": "CA",
        "city": "San Francisco",
        "is_remote": 0,
        "work_mode": "On-site",
        "seniority": "Senior",
        "employment_type": "Full-time",
        "has_ai_in_title": 1,
        "title_ai_terms": "AI",
        "ai_role_signature": "AI-Titled Analyst",
        "skills_extracted": "Python,SQL",
        "has_python": 1,
        "has_sql": 1,
        "status": "Open",
        "url_http_status": 200,
    }

"""
AI Analyst Jobs — Database Layer
==================================
Connection management, helpers, schema initialization (cmd_init_db),
and all path constants.
"""
import re
import shutil
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from pipeline.constants import (
    PLATFORM_CANONICAL,
)

# ─── Paths ───────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_DIR = PROJECT_ROOT / "db"
DB_PATH = DB_DIR / "job_postings_gold.db"
REVIEW_DIR = PROJECT_ROOT / "review"
OUTPUT_DIR = PROJECT_ROOT / "output"

CLAUDE_DB = Path.home() / "Documents" / "Claude Code" / "ai_analyst_roles_2026" / "db" / "job_postings_gold.db"
CODEX_DB = Path.home() / "Documents" / "Codex" / "artifacts" / "ai_analyst_roles_2026" / "db" / "job_postings_gold.db"


def get_db(path: Path = None) -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode."""
    p = path or DB_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def open_db(path: Path = None):
    """Context manager for database connections. Auto-commits and closes."""
    conn = get_db(path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def backup_db(path: Path = None):
    """Create a .bak copy of the database before destructive operations."""
    p = path or DB_PATH
    if p.exists():
        bak = p.with_suffix('.db.bak')
        shutil.copy2(p, bak)
        log(f"  Backup created: {bak.name}")


def row_to_dict(row) -> dict:
    """Convert sqlite3.Row to a plain dict with .get() support."""
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    return {k: row[k] for k in row.keys()}


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: init_db
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_init_db():
    """Create all tables + ALTER TABLE migrations for existing DBs."""
    conn = get_db()
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS companies_200 (
        company_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name        TEXT NOT NULL,
        canonical_name      TEXT NOT NULL,
        tier                TEXT NOT NULL,
        sector              TEXT NOT NULL,
        hq_country          TEXT NOT NULL DEFAULT 'US',
        ats_platform        TEXT,
        ats_platform_secondary TEXT,
        career_page_url     TEXT,
        ats_board_slug      TEXT,
        in_scope            INTEGER NOT NULL DEFAULT 1,
        created_at          TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS job_postings_gold (
        gold_id             INTEGER PRIMARY KEY AUTOINCREMENT,
        canonical_job_key   TEXT NOT NULL UNIQUE,
        company_id          INTEGER REFERENCES companies_200(company_id),
        company_name        TEXT NOT NULL,
        source_platform     TEXT NOT NULL,
        source_job_id       TEXT NOT NULL,
        job_url             TEXT NOT NULL,
        url_http_status     INTEGER,
        url_checked_at      TEXT,
        title               TEXT NOT NULL,
        title_normalized    TEXT NOT NULL,
        role_cluster        TEXT NOT NULL,
        seniority           TEXT,
        posted_date         TEXT,
        date_uncertain      INTEGER NOT NULL DEFAULT 0,
        window_bucket       TEXT,
        location_raw        TEXT,
        location_city       TEXT,
        location_state      TEXT,
        location_standardized TEXT,
        country             TEXT DEFAULT 'US',
        is_us               INTEGER NOT NULL DEFAULT 0,
        work_mode           TEXT,
        status              TEXT NOT NULL DEFAULT 'Open',
        ai_signal_types     TEXT,
        ai_keywords_hit     TEXT,
        ai_role_signature   TEXT,
        skills_extracted    TEXT,
        has_python          INTEGER DEFAULT 0,
        has_sql             INTEGER DEFAULT 0,
        salary_currency     TEXT,
        salary_min_usd      INTEGER,
        salary_max_usd      INTEGER,
        salary_period       TEXT,
        salary_text         TEXT,
        has_ai_in_title     INTEGER DEFAULT 0,
        title_ai_terms      TEXT,
        description_snippet TEXT,
        enrich_status       TEXT NOT NULL DEFAULT 'pending',
        verified_date       TEXT,
        created_at          TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS raw_postings (
        raw_id              INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name        TEXT,
        source_platform     TEXT,
        source_job_id       TEXT,
        job_url             TEXT,
        title               TEXT,
        location_raw        TEXT,
        body_raw            TEXT,
        posted_date         TEXT,
        salary_text         TEXT,
        collected_at        TEXT NOT NULL DEFAULT (datetime('now')),
        processed           INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS qa_violations (
        violation_id        INTEGER PRIMARY KEY AUTOINCREMENT,
        gold_id             INTEGER,
        rule_name           TEXT NOT NULL,
        severity            TEXT NOT NULL,
        details             TEXT,
        created_at          TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS source_attempts (
        attempt_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        source_platform     TEXT,
        company_name        TEXT,
        url_attempted       TEXT,
        http_status         INTEGER,
        result              TEXT,
        attempted_at        TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS approval_state (
        id                  INTEGER PRIMARY KEY,
        approved_by_user    INTEGER NOT NULL DEFAULT 0,
        approved_at         TEXT,
        row_count_at_approval INTEGER,
        critical_violations INTEGER,
        warning_violations  INTEGER
    );

    CREATE TABLE IF NOT EXISTS scrape_runs (
        run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
        source              TEXT NOT NULL,
        company_slug        TEXT,
        company_name        TEXT,
        rows_found          INTEGER DEFAULT 0,
        rows_inserted       INTEGER DEFAULT 0,
        rows_deduped        INTEGER DEFAULT 0,
        errors              TEXT,
        started_at          TEXT NOT NULL DEFAULT (datetime('now')),
        finished_at         TEXT
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_job_key ON job_postings_gold(canonical_job_key);
    CREATE INDEX IF NOT EXISTS idx_gold_company ON job_postings_gold(company_id);
    CREATE INDEX IF NOT EXISTS idx_gold_status ON job_postings_gold(status);
    CREATE INDEX IF NOT EXISTS idx_gold_platform ON job_postings_gold(source_platform);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_platform_jobid ON raw_postings(source_platform, source_job_id);
    """)

    conn.commit()
    log("init_db: All tables created/verified.")

    # Run ALTER TABLE migrations for any missing columns
    existing_cols = {r[1] for r in cur.execute("PRAGMA table_info(job_postings_gold)").fetchall()}
    migrations = {
        'has_ai_in_title': 'INTEGER DEFAULT 0',
        'title_ai_terms': 'TEXT',
        'ai_role_signature': 'TEXT',
        'description_snippet': 'TEXT',
        'has_python': 'INTEGER DEFAULT 0',
        'has_sql': 'INTEGER DEFAULT 0',
        'location_city': 'TEXT',
        'location_state': 'TEXT',
        'verified_date': 'TEXT',
        'ats_platform_secondary': 'TEXT',
    }
    for col, dtype in migrations.items():
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE job_postings_gold ADD COLUMN {col} {dtype}")
            log(f"  Migration: added {col} to job_postings_gold")

    # companies_200 migrations
    comp_cols = {r[1] for r in cur.execute("PRAGMA table_info(companies_200)").fetchall()}
    comp_migrations = {
        'ats_platform_secondary': 'TEXT',
        'ats_board_slug': 'TEXT',
    }
    for col, dtype in comp_migrations.items():
        if col not in comp_cols:
            cur.execute(f"ALTER TABLE companies_200 ADD COLUMN {col} {dtype}")
            log(f"  Migration: added {col} to companies_200")

    # raw_postings migrations
    raw_cols = {r[1] for r in cur.execute("PRAGMA table_info(raw_postings)").fetchall()}
    raw_migrations = {
        'salary_min': 'INTEGER',
        'salary_max': 'INTEGER',
        'salary_period': 'TEXT',
        'employment_type': 'TEXT',
        'scrape_run_id': 'INTEGER',
    }
    for col, dtype in raw_migrations.items():
        if col not in raw_cols:
            cur.execute(f"ALTER TABLE raw_postings ADD COLUMN {col} {dtype}")
            log(f"  Migration: added {col} to raw_postings")

    # Populate ats_board_slug from career_page_url for Greenhouse/Lever companies
    rows = cur.execute("""
        SELECT company_id, career_page_url, ats_platform
        FROM companies_200
        WHERE career_page_url IS NOT NULL
          AND career_page_url != ''
          AND (ats_board_slug IS NULL OR ats_board_slug = '')
    """).fetchall()
    slug_count = 0
    for r in rows:
        rd = row_to_dict(r)
        url = rd.get('career_page_url', '')
        platform = (rd.get('ats_platform') or '').lower()
        slug = None
        if 'greenhouse' in platform or 'greenhouse.io' in url:
            m = re.search(r'boards\.greenhouse\.io/([^/?#]+)', url)
            if m:
                slug = m.group(1)
        elif 'lever' in platform or 'lever.co' in url:
            m = re.search(r'jobs\.lever\.co/([^/?#]+)', url)
            if m:
                slug = m.group(1)
        if slug:
            cur.execute("UPDATE companies_200 SET ats_board_slug = ? WHERE company_id = ?",
                        (slug, rd['company_id']))
            slug_count += 1
    if slug_count:
        log(f"  Populated ats_board_slug for {slug_count} companies")

    conn.commit()
    conn.close()
    log("init_db: Migrations complete.")

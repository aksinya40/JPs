#!/usr/bin/env python3
"""
LinkedIn Job Collection via JobSpy
====================================
Standalone script (not part of the monolith) because it requires
non-stdlib dependencies: python-jobspy and pandas.

Usage:
    python scripts/collect_jobs.py linkedin [options]

Options:
    --keywords K1 K2 ...   Custom keywords (default: 10 AI-analyst terms)
    --locations L1 L2 ...  Custom locations (default: 6 US metro areas)
    --max-results N        Max results per query (default: 200)
    --hours-old N          Only jobs posted within N hours (default: 168 = 7 days)
    --dry-run              Show query grid without scraping
    --verbose              Print each query as it runs

Requires:
    uv pip install python-jobspy pandas   (needs Python >= 3.10)

    PROXY_URL env var recommended for LinkedIn (residential proxy).
    Set in .env or export PROXY_URL=http://user:pass@host:port
"""

import argparse
import math
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

# ─── Paths ───────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "db" / "job_postings_gold.db"
ENV_PATH = PROJECT_ROOT / ".env"

# ─── Default Query Grid ─────────────────────────────────────────────────────
DEFAULT_KEYWORDS = [
    "AI analyst",
    "data analyst AI",
    "generative AI analyst",
    "LLM data analyst",
    "machine learning analyst",
    "AI data scientist",
    "analytics AI",
    "decision intelligence analyst",
    "NLP analyst",
    "agentic analytics",
]

DEFAULT_LOCATIONS = [
    "United States",
    "New York, NY",
    "San Francisco, CA",
    "Seattle, WA",
    "Austin, TX",
    "Remote",
]


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def load_dotenv():
    """Load .env file into os.environ (no external dependency)."""
    if not ENV_PATH.exists():
        return
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, _, value = line.partition('=')
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def safe_str(val, default='') -> str:
    """Convert pandas value to clean string, handling NaN/NaT/None."""
    if val is None:
        return default
    if isinstance(val, float) and math.isnan(val):
        return default
    s = str(val)
    if s in ('nan', 'NaT', 'None', 'NaN', '<NA>'):
        return default
    return s


def safe_int(val) -> int | None:
    """Convert pandas numeric to int, returning None for NaN/None."""
    if val is None:
        return None
    if isinstance(val, float):
        if math.isnan(val):
            return None
        return int(val)
    if isinstance(val, int):
        return val
    return None


def get_db() -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def load_existing_ids(conn: sqlite3.Connection) -> set:
    """Load existing source_job_ids from raw_postings + gold for pre-dedup."""
    ids = set()
    for r in conn.execute(
        "SELECT source_job_id FROM raw_postings WHERE source_platform = 'LinkedIn'"
    ).fetchall():
        ids.add(r[0])
    for r in conn.execute(
        "SELECT source_job_id FROM job_postings_gold WHERE source_platform = 'LinkedIn'"
    ).fetchall():
        ids.add(r[0])
    return ids


def collect_linkedin(keywords: list, locations: list, max_results: int,
                     hours_old: int, dry_run: bool, verbose: bool):
    """Run LinkedIn collection via JobSpy."""
    # Build query grid (before imports so dry-run works without deps)
    queries = []
    for kw in keywords:
        for loc in locations:
            queries.append((kw, loc))

    log(f"Query grid: {len(keywords)} keywords × {len(locations)} locations = {len(queries)} queries")
    log(f"Max results per query: {max_results}, hours_old: {hours_old}")

    if dry_run:
        log("DRY RUN — query grid:")
        for i, (kw, loc) in enumerate(queries, 1):
            log(f"  [{i:3d}] keyword={kw!r}  location={loc!r}")
        log(f"Total queries: {len(queries)}")
        log(f"Max possible rows: {len(queries) * max_results}")
        return

    try:
        from jobspy import scrape_jobs
    except ImportError:
        log("ERROR: python-jobspy not installed. Run: uv pip install python-jobspy")
        sys.exit(1)

    # Load .env for PROXY_URL
    load_dotenv()
    proxy = os.environ.get('PROXY_URL', '')
    if not proxy:
        log("WARNING: PROXY_URL not set. LinkedIn scraping may be rate-limited.")
        log("  Set in .env: PROXY_URL=http://user:pass@host:port")

    conn = get_db()
    cur = conn.cursor()
    existing_ids = load_existing_ids(conn)
    log(f"Pre-loaded {len(existing_ids)} existing LinkedIn job IDs for dedup")

    total_found = 0
    total_inserted = 0
    total_skipped = 0
    total_errors = 0

    for i, (kw, loc) in enumerate(queries, 1):
        # Record scrape run
        cur.execute("""
            INSERT INTO scrape_runs (source, company_slug, company_name)
            VALUES ('linkedin', ?, ?)
        """, (kw, loc))
        run_id = cur.lastrowid
        conn.commit()

        try:
            if verbose:
                log(f"  [{i}/{len(queries)}] keyword={kw!r} location={loc!r}")

            jobs_df = scrape_jobs(
                site_name=["linkedin"],
                search_term=kw,
                location=loc,
                results_wanted=max_results,
                hours_old=hours_old,
                linkedin_fetch_description=True,
                description_format='html',
                proxies=[proxy] if proxy else None,
            )

            if jobs_df is None or jobs_df.empty:
                if verbose:
                    log(f"    → 0 results")
                cur.execute("""
                    UPDATE scrape_runs
                    SET rows_found=0, rows_inserted=0, rows_deduped=0,
                        finished_at=datetime('now')
                    WHERE run_id = ?
                """, (run_id,))
                conn.commit()
                time.sleep(2)
                continue

            found = len(jobs_df)
            inserted = 0
            skipped = 0

            for _, row in jobs_df.iterrows():
                job_id = safe_str(row.get('id'))
                if not job_id or job_id in existing_ids:
                    skipped += 1
                    continue

                title = safe_str(row.get('title'))
                company = safe_str(row.get('company'))
                location_raw = safe_str(row.get('location'))
                job_url = safe_str(row.get('job_url'))
                description = safe_str(row.get('description'))
                date_posted = safe_str(row.get('date_posted'))
                employment_type = safe_str(row.get('job_type'))

                # Structured salary from JobSpy
                sal_min = safe_int(row.get('min_amount'))
                sal_max = safe_int(row.get('max_amount'))
                sal_interval = safe_str(row.get('interval'))
                sal_currency = safe_str(row.get('currency'))

                # Build human-readable salary_text
                salary_text = ''
                if sal_min and sal_max:
                    salary_text = f"${sal_min:,} - ${sal_max:,}"
                    if sal_interval:
                        salary_text += f" ({sal_interval})"
                elif sal_min:
                    salary_text = f"${sal_min:,}+"

                # Salary period normalization
                salary_period = ''
                if sal_interval:
                    interval_lower = sal_interval.lower()
                    if 'year' in interval_lower or 'annual' in interval_lower:
                        salary_period = 'Annual'
                    elif 'hour' in interval_lower:
                        salary_period = 'Hourly'
                    elif 'month' in interval_lower:
                        salary_period = 'Monthly'

                # Enrich location with remote flag
                is_remote = row.get('is_remote')
                if is_remote is True and location_raw and 'remote' not in location_raw.lower():
                    location_raw = f"{location_raw} (Remote)" if location_raw else "Remote"

                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO raw_postings
                        (company_name, source_platform, source_job_id, job_url,
                         title, location_raw, body_raw, posted_date, salary_text,
                         salary_min, salary_max, salary_period, employment_type,
                         scrape_run_id)
                        VALUES (?, 'LinkedIn', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        company, job_id, job_url, title,
                        location_raw, description, date_posted,
                        salary_text, sal_min, sal_max,
                        salary_period, employment_type, run_id,
                    ))
                    if cur.rowcount > 0:
                        inserted += 1
                        existing_ids.add(job_id)
                    else:
                        skipped += 1
                except sqlite3.Error as e:
                    log(f"    DB error for job {job_id}: {e}")

            conn.commit()
            total_found += found
            total_inserted += inserted
            total_skipped += skipped

            cur.execute("""
                UPDATE scrape_runs
                SET rows_found=?, rows_inserted=?, rows_deduped=?,
                    finished_at=datetime('now')
                WHERE run_id = ?
            """, (found, inserted, skipped, run_id))
            conn.commit()

            if verbose:
                log(f"    → {found} found, {inserted} new, {skipped} skipped")

            # Rate limiting: 3s base + 5s extra every 5th query
            time.sleep(3 + (i % 5 == 0) * 5)

        except Exception as e:
            total_errors += 1
            error_msg = str(e)[:500]
            cur.execute("""
                UPDATE scrape_runs
                SET errors=?, finished_at=datetime('now')
                WHERE run_id = ?
            """, (error_msg, run_id))
            conn.commit()
            log(f"  ERROR query [{i}] {kw!r}/{loc!r}: {e}")
            time.sleep(5)

    conn.close()
    log(f"LinkedIn collection COMPLETE:")
    log(f"  Queries run:      {len(queries)}")
    log(f"  Total found:      {total_found}")
    log(f"  Total inserted:   {total_inserted}")
    log(f"  Total skipped:    {total_skipped}")
    log(f"  Errors:           {total_errors}")
    log(f"")
    log(f"Next step: python3 scripts/job_db_pipeline.py ingest_raw")


def main():
    parser = argparse.ArgumentParser(
        description='Job Collection Scripts — LinkedIn via JobSpy',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest='source', help='Data source')

    # LinkedIn subcommand
    li = sub.add_parser('linkedin', help='Collect from LinkedIn via JobSpy')
    li.add_argument('--keywords', nargs='+', default=DEFAULT_KEYWORDS,
                    help='Search keywords')
    li.add_argument('--locations', nargs='+', default=DEFAULT_LOCATIONS,
                    help='Search locations')
    li.add_argument('--max-results', type=int, default=200,
                    help='Max results per query (default: 200)')
    li.add_argument('--hours-old', type=int, default=168,
                    help='Only jobs posted within N hours (default: 168)')
    li.add_argument('--dry-run', action='store_true',
                    help='Show query grid without scraping')
    li.add_argument('--verbose', action='store_true',
                    help='Print each query as it runs')

    args = parser.parse_args()
    if not args.source:
        parser.print_help()
        sys.exit(1)

    if args.source == 'linkedin':
        collect_linkedin(
            keywords=args.keywords,
            locations=args.locations,
            max_results=args.max_results,
            hours_old=args.hours_old,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )


if __name__ == '__main__':
    main()

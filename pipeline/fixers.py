"""
AI Analyst Jobs — Data Fixers & Backfills
============================================
Data quality fixes: platform normalization, title AI backfill,
ai_role_signature classification, skills extraction, and the
11-step fix_data_quality pipeline.
"""
import re
from datetime import datetime

from pipeline.db import get_db, log
from pipeline.parsers import (
    canonicalize_platform,
    compute_title_ai_terms,
    extract_skills,
    normalize_title_to_segment,
    extract_company_from_url,
    strip_html,
)
from pipeline.filters import classify_ai_role_signature, resolve_work_mode
from pipeline.constants import (
    APPLIED_SCIENTIST_KEEP_KEYWORDS,
    APPLIED_SCIENTIST_REMOVE_KEYWORDS,
)


def cmd_normalize_platforms():
    """Canonicalize source_platform names."""
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT DISTINCT source_platform FROM job_postings_gold"
    ).fetchall()
    updated = 0
    for r in rows:
        raw = r[0]
        canonical = canonicalize_platform(raw)
        if canonical != raw:
            cur.execute(
                "UPDATE job_postings_gold SET source_platform = ? WHERE source_platform = ?",
                (canonical, raw)
            )
            updated += cur.rowcount
            log(f"  {raw} → {canonical} ({cur.rowcount} rows)")
    conn.commit()
    conn.close()
    log(f"normalize_platforms: Updated {updated} rows.")


def cmd_backfill_title_ai():
    """Compute has_ai_in_title + title_ai_terms for all rows."""
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute("SELECT gold_id, title FROM job_postings_gold").fetchall()
    updated = 0
    for r in rows:
        has_ai, terms = compute_title_ai_terms(r['title'])
        cur.execute("""
            UPDATE job_postings_gold
            SET has_ai_in_title = ?, title_ai_terms = ?
            WHERE gold_id = ?
        """, (has_ai, terms, r['gold_id']))
        updated += 1
    conn.commit()
    conn.close()
    log(f"backfill_title_ai: Updated {updated} rows.")


def cmd_backfill_ai_role_signature():
    """Classify ai_role_signature for all rows."""
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT gold_id, title, description_snippet, skills_extracted FROM job_postings_gold"
    ).fetchall()
    updated = 0
    for r in rows:
        sig = classify_ai_role_signature(
            r['title'], r['description_snippet'], r['skills_extracted']
        )
        cur.execute(
            "UPDATE job_postings_gold SET ai_role_signature = ? WHERE gold_id = ?",
            (sig, r['gold_id'])
        )
        updated += 1
    conn.commit()
    conn.close()
    log(f"backfill_ai_role_signature: Updated {updated} rows.")


def cmd_backfill_skills():
    """Extract skills from title + description_snippet + raw body_raw."""
    conn = get_db()
    cur = conn.cursor()
    # Join gold to raw_postings for full body text (6k+ chars vs 500 snippet)
    rows = cur.execute("""
        SELECT g.gold_id, g.title, g.description_snippet, g.skills_extracted,
               r.body_raw
        FROM job_postings_gold g
        LEFT JOIN raw_postings r
            ON g.source_job_id = r.source_job_id
           AND g.source_platform = r.source_platform
    """).fetchall()
    updated = 0
    for r in rows:
        title = r['title'] or ''
        desc = r['description_snippet'] or ''
        body = r['body_raw'] or ''
        existing = r['skills_extracted'] or ''
        # Combine title + full body + snippet + existing for maximum extraction
        combined = f"{title} {body} {desc} {existing}"
        skills, has_py, has_sql = extract_skills(combined)
        cur.execute("""
            UPDATE job_postings_gold
            SET skills_extracted = ?, has_python = ?, has_sql = ?
            WHERE gold_id = ?
        """, (skills or existing, has_py, has_sql, r['gold_id']))
        updated += 1
    conn.commit()
    conn.close()
    log(f"backfill_skills: Updated {updated} rows.")


def cmd_fix_data_quality():
    """Run all 13 data quality fixes in correct dependency order.

    Steps:
    1. DELETE non-US rows (is_us=0)
    2. DELETE aggregator URLs (builtin, theladders, etc.)
    3. Fix company_name='Unknown' from URL slug
    4. Strip HTML from description_snippet
    5. Re-extract skills_extracted with fixed regex
    6. Build title_normalized taxonomy (15-segment)
    7. Fix work_mode / location contradictions
    8. Fix salary_text corruption
    9. Re-compute posted_date date_uncertain flag (no ATS re-call)
    10. Classify Applied Scientist keep/remove
    11. Add in_target_list column
    12. Fallback posted_date from raw_postings collected_at
    13. Backfill short description_snippets from raw_postings body_raw
    """
    conn = get_db()
    cur = conn.cursor()

    # ── Step 1: DELETE non-US rows ──
    log("Step 1: Deleting non-US rows (is_us=0)...")
    non_us = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us = 0"
    ).fetchone()[0]
    cur.execute("DELETE FROM job_postings_gold WHERE is_us = 0")
    conn.commit()
    log(f"  Deleted {non_us} non-US rows")

    # ── Step 2: DELETE aggregator URLs ──
    log("Step 2: Deleting aggregator URLs...")
    agg_conditions = ' OR '.join(
        f"job_url LIKE '%{d}%'" for d in [
            'builtin.com', 'builtinnyc.com', 'builtinsf.com',
            'builtinchicago.com', 'builtinaustin.com', 'builtinboston.com',
            'builtincolorado.com', 'builtinla.com', 'builtinseattle.com',
            'theladders.com', 'themuse.com', 'towardsai.net',
            'wallstreetcareers.com', 'datasciencessjobs.com', 'technyjobs.com',
            'wellfound.com', 'angel.co',
        ]
    )
    before = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    cur.execute(f"DELETE FROM job_postings_gold WHERE {agg_conditions}")
    after = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    conn.commit()
    log(f"  Deleted {before - after} aggregator rows (was {before}, now {after})")

    # ── Step 3: Fix company_name='Unknown' from URL slug ──
    log("Step 3: Fixing company_name='Unknown' from URL slugs...")
    unknown_rows = cur.execute(
        "SELECT gold_id, job_url FROM job_postings_gold WHERE company_name = 'Unknown'"
    ).fetchall()
    fixed_companies = 0
    for r in unknown_rows:
        gid, url = r[0], r[1]
        name = extract_company_from_url(url)
        if name and name.lower() != 'unknown':
            cur.execute(
                "UPDATE job_postings_gold SET company_name = ? WHERE gold_id = ?",
                (name, gid)
            )
            fixed_companies += 1
    conn.commit()
    log(f"  Fixed {fixed_companies}/{len(unknown_rows)} Unknown company names")

    # ── Step 4: Strip HTML from description_snippet ──
    log("Step 4: Stripping HTML from description_snippet...")
    html_rows = cur.execute(
        "SELECT gold_id, description_snippet FROM job_postings_gold "
        "WHERE description_snippet LIKE '%<%' AND description_snippet IS NOT NULL"
    ).fetchall()
    for r in html_rows:
        gid, snippet = r[0], r[1]
        cleaned = strip_html(snippet)
        if len(cleaned) > 500:
            cleaned = cleaned[:500]
        cur.execute(
            "UPDATE job_postings_gold SET description_snippet = ? WHERE gold_id = ?",
            (cleaned, gid)
        )
    conn.commit()
    log(f"  Cleaned HTML from {len(html_rows)} description_snippets")

    # ── Step 5: Re-extract skills with fixed regex ──
    log("Step 5: Re-extracting skills with fixed R regex...")
    all_rows = cur.execute(
        "SELECT gold_id, description_snippet, title FROM job_postings_gold"
    ).fetchall()
    skills_fixed = 0
    for r in all_rows:
        gid = r[0]
        text = f"{r[2] or ''} {r[1] or ''}"
        new_skills, hp, hs = extract_skills(text)
        cur.execute(
            "UPDATE job_postings_gold SET skills_extracted=?, has_python=?, has_sql=? "
            "WHERE gold_id=?",
            (new_skills, hp, hs, gid)
        )
        skills_fixed += 1
    conn.commit()
    log(f"  Re-extracted skills for {skills_fixed} rows")

    # ── Step 6: Build title_normalized taxonomy ──
    log("Step 6: Normalizing title_normalized with taxonomy...")
    title_rows = cur.execute(
        "SELECT gold_id, title FROM job_postings_gold"
    ).fetchall()
    title_fixed = 0
    for r in title_rows:
        gid, title = r[0], r[1]
        norm = normalize_title_to_segment(title)
        if norm != title:
            title_fixed += 1
        cur.execute(
            "UPDATE job_postings_gold SET title_normalized = ? WHERE gold_id = ?",
            (norm, gid)
        )
    conn.commit()
    distinct = cur.execute(
        "SELECT COUNT(DISTINCT title_normalized) FROM job_postings_gold"
    ).fetchone()[0]
    log(f"  Normalized {title_fixed} titles → {distinct} distinct segments")

    # ── Step 7: Fix work_mode / location contradictions ──
    log("Step 7: Fixing work_mode / location contradictions...")
    wm_rows = cur.execute(
        "SELECT gold_id, work_mode, location_raw, location_standardized "
        "FROM job_postings_gold"
    ).fetchall()
    wm_fixed = 0
    for r in wm_rows:
        gid = r[0]
        old_wm = r[1] or 'Unknown'
        new_wm = resolve_work_mode(old_wm, r[2], r[3])
        if new_wm != old_wm:
            cur.execute(
                "UPDATE job_postings_gold SET work_mode = ? WHERE gold_id = ?",
                (new_wm, gid)
            )
            wm_fixed += 1
    conn.commit()
    log(f"  Fixed {wm_fixed} work_mode contradictions")

    # ── Step 8: Fix salary_text corruption ──
    log("Step 8: Fixing salary_text corruption...")
    corrupt = cur.execute("""
        SELECT gold_id, salary_text, salary_min_usd, salary_max_usd
        FROM job_postings_gold
        WHERE salary_text IS NOT NULL
          AND salary_max_usd IS NOT NULL
          AND LENGTH(salary_text) > 0
          AND salary_text LIKE '%1'
          AND CAST(salary_max_usd AS TEXT) != ''
    """).fetchall()
    sal_fixed = 0
    for r in corrupt:
        gid, st, smin, smax = r[0], r[1], r[2], r[3]
        if st and smax and st.endswith('1') and str(int(smax)) + '1' in st.replace(',', '').replace(' ', ''):
            # Likely corruption: salary_text has has_ai_in_title=1 appended
            clean_st = st[:-1]  # remove trailing "1"
            # Reformat nicely
            if smin and smax:
                clean_st = f"${int(smin):,} - ${int(smax):,}"
            cur.execute(
                "UPDATE job_postings_gold SET salary_text = ? WHERE gold_id = ?",
                (clean_st, gid)
            )
            sal_fixed += 1
    conn.commit()
    log(f"  Fixed {sal_fixed} corrupted salary_text values")

    # ── Step 9: Re-check date_uncertain for rows with enriched posted_date ──
    log("Step 9: Rechecking date_uncertain flags...")
    date_rows = cur.execute(
        "SELECT gold_id, posted_date, date_uncertain, enrich_status FROM job_postings_gold "
        "WHERE posted_date IS NOT NULL AND posted_date != '' AND date_uncertain = 1"
    ).fetchall()
    date_fixed = 0
    for r in date_rows:
        gid, pd, du, es = r[0], r[1], r[2], r[3]
        # Only mark certain if enrichment confirmed the date
        if es and es not in ('pending', 'failed', 'skipped'):
            try:
                datetime.strptime(pd[:10], '%Y-%m-%d')
                cur.execute(
                    "UPDATE job_postings_gold SET date_uncertain = 0 WHERE gold_id = ?",
                    (gid,)
                )
                date_fixed += 1
            except (ValueError, TypeError):
                pass
    conn.commit()
    log(f"  Cleared date_uncertain for {date_fixed} enriched rows with valid dates")

    # ── Step 10: Classify Applied Scientist scope ──
    log("Step 10: Classifying Applied Scientist scope...")
    as_rows = cur.execute(
        "SELECT gold_id, title, description_snippet, ai_keywords_hit "
        "FROM job_postings_gold WHERE title LIKE '%Applied Scientist%'"
    ).fetchall()
    kept = 0
    removed = 0
    for r in as_rows:
        gid, title, desc, kw = r[0], r[1], r[2] or '', r[3] or ''
        full_text = f"{title} {desc} {kw}".lower()
        # Check remove keywords first (higher priority)
        should_remove = any(k in full_text for k in APPLIED_SCIENTIST_REMOVE_KEYWORDS)
        should_keep = any(k in full_text for k in APPLIED_SCIENTIST_KEEP_KEYWORDS)
        if should_remove and not should_keep:
            cur.execute(
                "UPDATE job_postings_gold SET role_cluster = 'Applied Scientist (Excluded)', "
                "status = 'Excluded' WHERE gold_id = ?", (gid,)
            )
            removed += 1
        else:
            cur.execute(
                "UPDATE job_postings_gold SET role_cluster = 'Applied Scientist' "
                "WHERE gold_id = ?", (gid,)
            )
            kept += 1
    conn.commit()
    log(f"  Applied Scientist: kept {kept}, excluded {removed}")

    # ── Step 11: Add in_target_list column ──
    log("Step 11: Adding in_target_list column...")
    existing_cols = {r[1] for r in cur.execute(
        "PRAGMA table_info(job_postings_gold)"
    ).fetchall()}
    if 'in_target_list' not in existing_cols:
        cur.execute(
            "ALTER TABLE job_postings_gold ADD COLUMN in_target_list INTEGER DEFAULT 0"
        )
    cur.execute("UPDATE job_postings_gold SET in_target_list = 1 WHERE company_id > 0")
    # Also try matching by company_name against companies_200
    cur.execute("""
        UPDATE job_postings_gold
        SET in_target_list = 1
        WHERE company_name IN (SELECT company_name FROM companies_200)
          OR company_name IN (SELECT canonical_name FROM companies_200)
    """)
    conn.commit()
    in_target = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE in_target_list = 1"
    ).fetchone()[0]
    total = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    log(f"  in_target_list: {in_target}/{total} rows")

    # ── Step 12: Fallback posted_date from raw_postings collected_at ──
    log("Step 12: Fallback posted_date from raw_postings collected_at...")
    missing_date_rows = cur.execute("""
        SELECT g.gold_id, r.collected_at
        FROM job_postings_gold g
        JOIN raw_postings r
            ON g.source_job_id = r.source_job_id
           AND g.source_platform = r.source_platform
        WHERE (g.posted_date IS NULL OR g.posted_date = '')
          AND r.collected_at IS NOT NULL AND r.collected_at != ''
    """).fetchall()
    date_fallback = 0
    for r in missing_date_rows:
        gid, collected = r[0], r[1]
        # Use collected_at date portion as approximate posted_date
        try:
            fallback_date = collected[:10]  # YYYY-MM-DD from ISO datetime
            datetime.strptime(fallback_date, '%Y-%m-%d')  # validate format
            cur.execute(
                "UPDATE job_postings_gold SET posted_date = ?, date_uncertain = 1 "
                "WHERE gold_id = ?",
                (fallback_date, gid)
            )
            date_fallback += 1
        except (ValueError, TypeError):
            pass
    conn.commit()
    log(f"  Filled posted_date for {date_fallback} rows from raw_postings.collected_at")

    # ── Step 13: Backfill short description_snippets from body_raw ──
    log("Step 13: Backfilling short description_snippets from raw_postings.body_raw...")
    short_snippet_rows = cur.execute("""
        SELECT g.gold_id, r.body_raw
        FROM job_postings_gold g
        JOIN raw_postings r
            ON g.source_job_id = r.source_job_id
           AND g.source_platform = r.source_platform
        WHERE (g.description_snippet IS NULL
               OR LENGTH(g.description_snippet) < 50)
          AND r.body_raw IS NOT NULL
          AND LENGTH(r.body_raw) >= 50
    """).fetchall()
    snippet_backfill = 0
    for r in short_snippet_rows:
        gid, body = r[0], r[1]
        cleaned = strip_html(body)
        if len(cleaned) >= 50:
            snippet = cleaned[:500]
            cur.execute(
                "UPDATE job_postings_gold SET description_snippet = ? WHERE gold_id = ?",
                (snippet, gid)
            )
            snippet_backfill += 1
    conn.commit()
    log(f"  Backfilled {snippet_backfill} short description_snippets from body_raw")

    # ── Final summary ──
    total = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    active = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]
    distinct_titles = cur.execute(
        "SELECT COUNT(DISTINCT title_normalized) FROM job_postings_gold"
    ).fetchone()[0]
    unknowns = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE company_name='Unknown'"
    ).fetchone()[0]
    html_remaining = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE description_snippet LIKE '%<%'"
    ).fetchone()[0]
    r_only = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE skills_extracted='R'"
    ).fetchone()[0]

    conn.close()

    log(f"fix_data_quality COMPLETE:")
    log(f"  Total rows:           {total}")
    log(f"  Active US:            {active}")
    log(f"  Distinct titles:      {distinct_titles}")
    log(f"  Remaining Unknown:    {unknowns}")
    log(f"  Remaining HTML:       {html_remaining}")
    log(f"  R-only skills:        {r_only}")
    log(f"  In target list:       {in_target}/{total}")

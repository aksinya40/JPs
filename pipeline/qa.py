"""
AI Analyst Jobs — Quality Assurance
======================================
QA checks: populate qa_violations table with CRITICAL / WARNING
violations per row. Also provides aggregate checks.

cmd_health_check() — comprehensive per-column validation with health
score (0–100%) and timestamped JSON report.
"""
import json
import os
import re
from datetime import datetime, date

from pipeline.db import get_db, log, REVIEW_DIR, row_to_dict
from pipeline.filters import is_role_excluded
from pipeline.parsers import is_aggregator_url
from pipeline.constants import PLATFORM_CANONICAL, REMOTE_PATTERNS


def cmd_qa_check():
    """Run all quality gates → populate qa_violations table → print summary."""
    conn = get_db()
    cur = conn.cursor()

    # Clear previous violations
    cur.execute("DELETE FROM qa_violations")

    critical = 0
    warning = 0

    def _add_violation(gold_id, rule, severity, details=''):
        nonlocal critical, warning
        cur.execute("""
            INSERT INTO qa_violations (gold_id, rule_name, severity, details)
            VALUES (?, ?, ?, ?)
        """, (gold_id, rule, severity, details))
        if severity == 'CRITICAL':
            critical += 1
        else:
            warning += 1

    # ── CRITICAL checks ──

    # row_count_below_300
    active_count = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]
    if active_count < 300:
        _add_violation(None, 'row_count_below_300', 'CRITICAL',
                       f'Active US rows: {active_count} (need 300+)')

    # companies_200_count_wrong
    comp_count = cur.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
    if comp_count != 200:
        _add_violation(None, 'companies_200_count_wrong', 'CRITICAL',
                       f'companies_200 has {comp_count} rows (need exactly 200)')

    # duplicate_job_key
    dupes = cur.execute("""
        SELECT canonical_job_key, COUNT(*) as cnt
        FROM job_postings_gold GROUP BY canonical_job_key HAVING cnt > 1
    """).fetchall()
    for d in dupes:
        _add_violation(None, 'duplicate_job_key', 'CRITICAL',
                       f'Key {d["canonical_job_key"][:16]}... appears {d["cnt"]} times')

    # Per-row checks
    rows = cur.execute("SELECT * FROM job_postings_gold WHERE status = 'Open'").fetchall()
    for r in rows:
        gid = r['gold_id']

        # required_field_null
        for field in ['company_name', 'source_platform', 'source_job_id',
                      'job_url', 'title', 'title_normalized', 'role_cluster',
                      'canonical_job_key']:
            if not r[field]:
                _add_violation(gid, 'required_field_null', 'CRITICAL',
                               f'{field} is NULL')

        # not_us
        if not r['is_us'] and r['status'] == 'Open':
            _add_violation(gid, 'not_us', 'CRITICAL',
                           'is_us=0 on active row')

        # url_not_reachable — 404/410 are CRITICAL (confirmed dead),
        # transient errors (-1, 403, 429, 5xx) are WARNING
        hs = r['url_http_status']
        if hs and hs not in (0, 200, 301, 302):
            if hs in (404, 410):
                _add_violation(gid, 'url_not_reachable', 'CRITICAL',
                               f'HTTP {hs}')
            else:
                _add_violation(gid, 'url_not_reachable', 'WARNING',
                               f'HTTP {hs} (transient?)')

        # no_ai_signal
        kw = r['ai_keywords_hit']
        if not kw or kw in ('', '[]', 'null'):
            _add_violation(gid, 'no_ai_signal', 'CRITICAL',
                           'ai_keywords_hit empty')

        # role_excluded
        if is_role_excluded(r['title']):
            _add_violation(gid, 'role_excluded', 'CRITICAL',
                           f'Title matches exclusion: {r["title"]}')

        # unknown_company — WARNING for isolated cases, escalated later
        if r['company_name'] == 'Unknown':
            _add_violation(gid, 'unknown_company', 'WARNING',
                           f'company_name is Unknown, URL: {r["job_url"][:60]}')

        # html_in_snippet
        snippet = r['description_snippet'] or ''
        if '<' in snippet and '>' in snippet:
            _add_violation(gid, 'html_in_snippet', 'WARNING',
                           'description_snippet contains HTML tags')

        # aggregator_url
        if is_aggregator_url(r['job_url']):
            _add_violation(gid, 'aggregator_url', 'CRITICAL',
                           f'Aggregator URL: {r["job_url"][:80]}')

        # work_mode_contradiction
        loc_text = f"{r['location_raw'] or ''} {r['location_standardized'] or ''}".lower()
        if r['work_mode'] == 'On-site' and any(re.search(p, loc_text) for p in REMOTE_PATTERNS):
            _add_violation(gid, 'work_mode_contradiction', 'WARNING',
                           'work_mode=On-site but location has Remote')

        # date_out_of_window
        pd = r['posted_date']
        if pd and not r['date_uncertain']:
            try:
                d = datetime.strptime(pd[:10], '%Y-%m-%d').date()
                if d < date(2025, 7, 1) or d > date(2026, 3, 31):
                    _add_violation(gid, 'date_out_of_window', 'CRITICAL',
                                   f'posted_date={pd}')
            except ValueError:
                pass

        # ── WARNING checks ──

        # salary sanity
        if r['salary_min_usd']:
            if r['salary_min_usd'] < 15000 or (r['salary_max_usd'] and r['salary_max_usd'] > 600000):
                _add_violation(gid, 'salary_insane', 'WARNING',
                               f'${r["salary_min_usd"]}-${r["salary_max_usd"]}')
            if r['salary_max_usd'] and r['salary_min_usd'] > 0:
                if r['salary_max_usd'] / r['salary_min_usd'] > 5:
                    _add_violation(gid, 'salary_insane', 'WARNING',
                                   'max/min ratio > 5')

        # missing_description
        ds = r['description_snippet']
        if not ds or len(ds) < 50:
            _add_violation(gid, 'missing_description', 'WARNING',
                           'description_snippet NULL or < 50 chars')

    # unknown_company_high_count — CRITICAL if 5+ unknown companies
    unknown_count = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE company_name='Unknown' AND status='Open'"
    ).fetchone()[0]
    if unknown_count >= 5:
        _add_violation(None, 'unknown_company_high_count', 'CRITICAL',
                       f'{unknown_count} rows with company_name=Unknown')

    # date_uncertain_high_ratio
    uncertain = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE date_uncertain=1 AND status='Open'"
    ).fetchone()[0]
    if active_count > 0 and uncertain / active_count > 0.3:
        _add_violation(None, 'date_uncertain_high_ratio', 'WARNING',
                       f'{uncertain}/{active_count} = {100*uncertain//active_count}%')

    conn.commit()
    conn.close()

    log(f"qa_check COMPLETE: {critical} CRITICAL, {warning} WARNING")
    return critical, warning


# ═════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK — comprehensive per-column validation
# ═════════════════════════════════════════════════════════════════════════════

# Canonical platform names (from PLATFORM_CANONICAL values + known extras)
_VALID_PLATFORMS = set(PLATFORM_CANONICAL.values()) | {
    'Ashby', 'BambooHR', 'Rippling', 'Workable', 'Snap',
    # Custom / direct-sourced platforms found in live DB
    'Shopify', 'Spotify', 'microsoft.ai', 'TechNYC',
    'Palo Alto Networks', 'Oracle Careers', 'OpenAI Careers',
    'CVS Health Careers', 'DataScienceJobs', 'Other',
}
_VALID_WORK_MODES = {'On-site', 'Remote', 'Hybrid', 'Unknown'}
_VALID_STATUSES = {'Open', 'Closed', 'Excluded'}
_VALID_ENRICH = {'pending', 'enriched', 'api_enriched', 'failed', 'skipped'}


def cmd_health_check():
    """Validate EVERY column for EVERY row → health score + JSON report.

    Returns (score, critical_count, warning_count).
    Saves full report to review/health_YYYY-MM-DD_HHMMSS.json.
    """
    conn = get_db()
    cur = conn.cursor()

    now = datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S')
    file_ts = now.strftime('%Y-%m-%d_%H%M%S')

    # Fetch all rows + counts
    total_rows = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    active_rows = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]
    rows = cur.execute("SELECT * FROM job_postings_gold WHERE status='Open'").fetchall()

    violations = []  # list of {gold_id, rule, severity, details}
    coverage = {}    # column → {valid, total}

    def _v(gold_id, rule, severity, details=''):
        violations.append({
            'gold_id': gold_id,
            'rule': rule,
            'severity': severity,
            'details': details,
        })

    def _track(column, is_valid):
        if column not in coverage:
            coverage[column] = {'valid': 0, 'total': 0}
        coverage[column]['total'] += 1
        if is_valid:
            coverage[column]['valid'] += 1

    # ── Per-row column-level checks ──────────────────────────────────────────

    for row in rows:
        r = row_to_dict(row)
        gid = r['gold_id']

        # --- Required fields (CRITICAL if empty/null) ---
        for field in ['canonical_job_key', 'company_name', 'source_platform',
                      'source_job_id', 'job_url', 'title']:
            val = r.get(field)
            ok = bool(val and str(val).strip())
            _track(field, ok)
            if not ok:
                _v(gid, f'required_{field}', 'CRITICAL', f'{field} is empty')

        # title — length check
        title = r.get('title') or ''
        ok = len(title.strip()) >= 3
        _track('title_length', ok)
        if not ok and title.strip():  # non-empty but too short
            _v(gid, 'title_too_short', 'CRITICAL', f'title={title!r} (len<3)')

        # title_normalized — must not be empty AND must not be verbatim copy
        tn = r.get('title_normalized') or ''
        tn_ok = bool(tn and tn.strip())
        _track('title_normalized', tn_ok)
        if not tn_ok:
            _v(gid, 'required_title_normalized', 'CRITICAL',
               'title_normalized is empty')

        # role_cluster
        rc = r.get('role_cluster') or ''
        rc_ok = bool(rc and rc.strip())
        _track('role_cluster', rc_ok)
        if not rc_ok:
            _v(gid, 'required_role_cluster', 'CRITICAL', 'role_cluster is empty')

        # is_us must be 1 for Open rows
        is_us_ok = r.get('is_us') == 1
        _track('is_us', is_us_ok)
        if not is_us_ok:
            _v(gid, 'not_us_open', 'CRITICAL', f'is_us={r.get("is_us")}')

        # ai_keywords_hit — not empty/null/[]/null-string
        kw = r.get('ai_keywords_hit') or ''
        kw_ok = bool(kw and kw not in ('', '[]', 'null'))
        _track('ai_keywords_hit', kw_ok)
        if not kw_ok:
            _v(gid, 'missing_ai_keywords', 'CRITICAL', 'ai_keywords_hit empty')

        # date_uncertain must be 0 or 1
        du = r.get('date_uncertain')
        du_ok = du in (0, 1)
        _track('date_uncertain', du_ok)
        if not du_ok:
            _v(gid, 'invalid_date_uncertain', 'CRITICAL',
               f'date_uncertain={du!r}')

        # --- Enum/value checks (CRITICAL if invalid value) ---

        # source_platform
        sp = r.get('source_platform') or ''
        sp_ok = sp in _VALID_PLATFORMS
        _track('source_platform_enum', sp_ok)
        if not sp_ok and sp:
            _v(gid, 'invalid_source_platform', 'CRITICAL',
               f'source_platform={sp!r}')

        # work_mode
        wm = r.get('work_mode') or ''
        wm_ok = (wm in _VALID_WORK_MODES) or (not wm)  # NULL is ok (WARNING)
        _track('work_mode_enum', wm_ok)
        if not wm_ok:
            _v(gid, 'invalid_work_mode', 'CRITICAL', f'work_mode={wm!r}')

        # status — already filtered to Open, but validate anyway
        st = r.get('status') or ''
        st_ok = st in _VALID_STATUSES
        _track('status_enum', st_ok)
        if not st_ok:
            _v(gid, 'invalid_status', 'CRITICAL', f'status={st!r}')

        # enrich_status
        es = r.get('enrich_status') or ''
        es_ok = (es in _VALID_ENRICH) or (not es)
        _track('enrich_status_enum', es_ok)
        if not es_ok:
            _v(gid, 'invalid_enrich_status', 'CRITICAL',
               f'enrich_status={es!r}')

        # Boolean flags: has_python, has_sql, has_ai_in_title
        for bf in ['has_python', 'has_sql', 'has_ai_in_title']:
            bv = r.get(bf)
            bf_ok = bv in (0, 1, None)
            _track(bf, bf_ok)
            if not bf_ok:
                _v(gid, f'invalid_{bf}', 'CRITICAL', f'{bf}={bv!r}')

        # --- Numeric range checks ---

        # url_http_status — 404/410 are CRITICAL (dead URL)
        hs = r.get('url_http_status')
        if hs and hs in (404, 410):
            _v(gid, 'dead_url', 'CRITICAL', f'HTTP {hs}')
            _track('url_http_status', False)
        else:
            _track('url_http_status', True)

        # salary_min_usd
        sal_min = r.get('salary_min_usd')
        if sal_min and sal_min > 0:
            sal_min_ok = 15000 <= sal_min <= 500000
            _track('salary_min_usd', sal_min_ok)
            if not sal_min_ok:
                _v(gid, 'salary_min_out_of_range', 'CRITICAL',
                   f'salary_min_usd={sal_min}')
        else:
            _track('salary_min_usd', True)  # NULL is ok

        # salary_max_usd
        sal_max = r.get('salary_max_usd')
        if sal_max and sal_max > 0:
            sal_max_ok = sal_max <= 600000
            if sal_min and sal_min > 0:
                sal_max_ok = sal_max_ok and sal_max >= sal_min
                # ratio check
                if sal_min > 0 and sal_max / sal_min > 5:
                    _v(gid, 'salary_ratio_extreme', 'CRITICAL',
                       f'max/min ratio={sal_max/sal_min:.1f}')
            _track('salary_max_usd', sal_max_ok)
            if not sal_max_ok:
                _v(gid, 'salary_max_out_of_range', 'CRITICAL',
                   f'salary_max_usd={sal_max}, min={sal_min}')
        else:
            _track('salary_max_usd', True)

        # --- Format checks ---

        # posted_date format + window
        pd_val = r.get('posted_date')
        if pd_val and not r.get('date_uncertain'):
            try:
                d = datetime.strptime(str(pd_val)[:10], '%Y-%m-%d').date()
                in_window = date(2025, 7, 1) <= d <= date(2026, 3, 31)
                _track('posted_date_format', True)
                _track('posted_date_window', in_window)
                if not in_window:
                    _v(gid, 'date_out_of_window', 'CRITICAL',
                       f'posted_date={pd_val}')
            except (ValueError, TypeError):
                _track('posted_date_format', False)
                _track('posted_date_window', False)
                _v(gid, 'invalid_date_format', 'CRITICAL',
                   f'posted_date={pd_val!r}')
        elif pd_val:
            # date_uncertain=1, just check format
            try:
                datetime.strptime(str(pd_val)[:10], '%Y-%m-%d')
                _track('posted_date_format', True)
            except (ValueError, TypeError):
                _track('posted_date_format', False)
                _v(gid, 'invalid_date_format', 'CRITICAL',
                   f'posted_date={pd_val!r}')

        # job_url format
        url = r.get('job_url') or ''
        url_ok = url.startswith('https://') or url.startswith('http://')
        _track('job_url_format', url_ok)
        if not url_ok and url:
            _v(gid, 'invalid_job_url_format', 'CRITICAL',
               f'job_url does not start with https://')

        # ai_keywords_hit — valid format (JSON list OR comma-separated string)
        if kw and kw not in ('', '[]', 'null'):
            try:
                parsed = json.loads(kw)
                ai_fmt_ok = isinstance(parsed, list)
            except (json.JSONDecodeError, TypeError):
                # Accept plain CSV strings like "ai, machine learning"
                ai_fmt_ok = isinstance(kw, str) and len(kw.strip()) > 0
            _track('ai_keywords_format', ai_fmt_ok)
            if not ai_fmt_ok:
                _v(gid, 'invalid_ai_keywords_format', 'CRITICAL',
                   f'ai_keywords_hit not valid JSON list or CSV string')

        # created_at — parseable datetime (space or T separator, optional µs)
        ca = r.get('created_at')
        if ca:
            ca_str = str(ca)
            ca_ok = False
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
                try:
                    datetime.strptime(ca_str[:19], fmt)
                    ca_ok = True
                    break
                except (ValueError, TypeError):
                    pass
            _track('created_at_format', ca_ok)
            if not ca_ok:
                _v(gid, 'invalid_created_at', 'CRITICAL',
                   f'created_at={ca!r}')

        # --- Cross-column consistency (CRITICAL) ---

        # work_mode_contradiction: On-site but location has 'remote'
        loc_text = f"{r.get('location_raw') or ''} {r.get('location_standardized') or ''}".lower()
        if r.get('work_mode') == 'On-site' and any(
            re.search(p, loc_text) for p in REMOTE_PATTERNS
        ):
            _v(gid, 'work_mode_contradiction', 'CRITICAL',
               'work_mode=On-site but location has Remote')

        # salary_inverted
        if sal_min and sal_max and sal_min > sal_max:
            _v(gid, 'salary_inverted', 'CRITICAL',
               f'salary_min={sal_min} > salary_max={sal_max}')

        # title_verbatim
        if tn and title and tn == title:
            _track('title_not_verbatim', False)
        else:
            _track('title_not_verbatim', True)

        # --- Optional fields (WARNING if missing on Open rows) ---
        snippet = r.get('description_snippet') or ''
        _track('description_snippet_valid', len(snippet) >= 50)
        if len(snippet) < 50:
            _v(gid, 'short_description', 'WARNING',
               f'description_snippet len={len(snippet)}')

        skills = r.get('skills_extracted') or ''
        _track('skills_extracted_valid', bool(skills.strip()))
        if not skills.strip():
            _v(gid, 'missing_skills', 'WARNING', 'skills_extracted empty')

        _track('location_raw_set', bool(r.get('location_raw')))
        _track('location_standardized_set', bool(r.get('location_standardized')))
        _track('work_mode_set', bool(r.get('work_mode')))

        pd_set = bool(r.get('posted_date'))
        _track('posted_date_set', pd_set)
        if not pd_set:
            _v(gid, 'missing_posted_date', 'WARNING', 'posted_date is NULL')

        _track('seniority_set', bool(r.get('seniority')))
        _track('ai_role_signature_set', bool(r.get('ai_role_signature')))

    # ── Aggregate checks (CRITICAL if threshold breached) ────────────────────

    open_count = len(rows)

    # row_count_below_300
    if active_rows < 300:
        _v(None, 'row_count_below_300', 'CRITICAL',
           f'Active US rows: {active_rows} (need 300+)')

    # companies_200_count_wrong
    comp_count = cur.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
    if comp_count != 200:
        _v(None, 'companies_200_count_wrong', 'CRITICAL',
           f'companies_200 has {comp_count} rows (need 200)')

    # duplicate_job_key
    dupes = cur.execute("""
        SELECT canonical_job_key, COUNT(*) as cnt
        FROM job_postings_gold GROUP BY canonical_job_key HAVING cnt > 1
    """).fetchall()
    for d in dupes:
        _v(None, 'duplicate_job_key', 'CRITICAL',
           f'Key {d["canonical_job_key"][:16]}... appears {d["cnt"]}x')

    if open_count > 0:
        # title_verbatim_rate
        verbatim_count = cur.execute("""
            SELECT COUNT(*) FROM job_postings_gold
            WHERE status='Open' AND title_normalized = title
        """).fetchone()[0]
        if verbatim_count / open_count > 0.05:
            _v(None, 'title_verbatim_rate', 'CRITICAL',
               f'{verbatim_count}/{open_count} = '
               f'{100*verbatim_count/open_count:.1f}% verbatim')

        # skills_missing_rate
        skills_missing = cur.execute("""
            SELECT COUNT(*) FROM job_postings_gold
            WHERE status='Open'
              AND (skills_extracted IS NULL OR skills_extracted = '')
        """).fetchone()[0]
        if skills_missing / open_count > 0.20:
            _v(None, 'skills_missing_rate', 'CRITICAL',
               f'{skills_missing}/{open_count} = '
               f'{100*skills_missing/open_count:.1f}% missing skills')

        # description_missing_rate
        desc_missing = cur.execute("""
            SELECT COUNT(*) FROM job_postings_gold
            WHERE status='Open'
              AND (description_snippet IS NULL OR LENGTH(description_snippet) < 50)
        """).fetchone()[0]
        if desc_missing / open_count > 0.30:
            _v(None, 'description_missing_rate', 'CRITICAL',
               f'{desc_missing}/{open_count} = '
               f'{100*desc_missing/open_count:.1f}% short/missing descriptions')

        # unknown_company_rate
        unknown_count = cur.execute("""
            SELECT COUNT(*) FROM job_postings_gold
            WHERE status='Open' AND company_name='Unknown'
        """).fetchone()[0]
        if unknown_count >= 5:
            _v(None, 'unknown_company_rate', 'CRITICAL',
               f'{unknown_count} rows with company_name=Unknown')

        # date_uncertain_rate
        uncertain = cur.execute("""
            SELECT COUNT(*) FROM job_postings_gold
            WHERE status='Open' AND date_uncertain=1
        """).fetchone()[0]
        if uncertain / open_count > 0.30:
            _v(None, 'date_uncertain_rate', 'CRITICAL',
               f'{uncertain}/{open_count} = '
               f'{100*uncertain/open_count:.1f}% date_uncertain')

    conn.close()

    # ── Calculate health score ───────────────────────────────────────────────

    crit_count = sum(1 for v in violations if v['severity'] == 'CRITICAL')
    warn_count = sum(1 for v in violations if v['severity'] == 'WARNING')

    # Score: total column checks minus violations, divided by total checks
    total_checks = sum(c['total'] for c in coverage.values())
    total_valid = sum(c['valid'] for c in coverage.values())
    score = (total_valid / total_checks * 100) if total_checks > 0 else 0.0

    # ── Build coverage summary ───────────────────────────────────────────────

    key_columns = [
        'title_normalized', 'ai_keywords_hit', 'description_snippet_valid',
        'skills_extracted_valid', 'posted_date_set', 'work_mode_set',
        'location_raw_set', 'seniority_set', 'ai_role_signature_set',
    ]
    coverage_summary = {}
    for col in key_columns:
        if col in coverage:
            c = coverage[col]
            pct = (c['valid'] / c['total'] * 100) if c['total'] > 0 else 0
            coverage_summary[col] = {
                'valid': c['valid'],
                'total': c['total'],
                'pct': round(pct, 1),
            }

    # ── Console output ───────────────────────────────────────────────────────

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print(f"║  DB HEALTH CHECK  {ts:42s}║")
    print("╠══════════════════════════════════════════════════════════╣")
    print(f"║  Total rows: {total_rows:>6,}  |  Active (Open): {open_count:>6,}       ║")
    print("║                                                          ║")
    crit_icon = "✅" if crit_count == 0 else "❌"
    print(f"║  CRITICAL violations: {crit_count:>4}   {crit_icon}                         ║")
    print(f"║  WARNING  violations: {warn_count:>4}                                ║")
    print("║                                                          ║")
    print(f"║  Health score:     {score:>6.1f}%                               ║")
    print("╠══════════════════════════════════════════════════════════╣")
    print("║  COLUMN COVERAGE (Open rows)                              ║")

    for col, info in coverage_summary.items():
        label = col.replace('_', ' ').replace('valid', '').replace('set', '').strip()
        label = label[:25].ljust(25)
        icon = "✅" if info['pct'] >= 99.0 else "⚠️" if info['pct'] >= 90.0 else "❌"
        print(f"║  {label} {info['valid']:>5,} / {info['total']:>5,}  "
              f"{info['pct']:>5.1f}%  {icon}     ║")

    print("╚══════════════════════════════════════════════════════════╝")
    print()

    # ── Save JSON report ─────────────────────────────────────────────────────

    report = {
        'timestamp': ts,
        'total_rows': total_rows,
        'active_rows': open_count,
        'health_score': round(score, 2),
        'critical_count': crit_count,
        'warning_count': warn_count,
        'total_checks': total_checks,
        'total_valid': total_valid,
        'coverage': {k: v for k, v in coverage_summary.items()},
        'violations': violations[:200],  # cap at 200 for file size
        'violation_counts': {},
    }

    # Count by rule
    rule_counts = {}
    for v in violations:
        key = v['rule']
        rule_counts[key] = rule_counts.get(key, 0) + 1
    report['violation_counts'] = dict(
        sorted(rule_counts.items(), key=lambda x: -x[1])
    )

    REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REVIEW_DIR / f"health_{file_ts}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    log(f"health_check COMPLETE: score={score:.1f}%, "
        f"{crit_count} CRITICAL, {warn_count} WARNING → {report_path.name}")
    return score, crit_count, warn_count

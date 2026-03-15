"""
AI Analyst Jobs — Enrichment
===============================
ATS API enrichment (Greenhouse, Ashby, Lever, JSON-LD),
salary mining from body text, and URL freshness checking.
"""
import json
import re
import time
import urllib.request
import urllib.error
from datetime import datetime

from pipeline.db import get_db, log
from pipeline.parsers import extract_salary


def cmd_mine_salary_from_body():
    """Parse salary regex from stored body_raw/description_snippet. FREE, no HTTP."""
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT gold_id, description_snippet FROM job_postings_gold
        WHERE salary_min_usd IS NULL AND description_snippet IS NOT NULL
              AND length(description_snippet) > 20
    """).fetchall()
    log(f"mine_salary_from_body: Scanning {len(rows)} rows...")
    mined = 0
    for r in rows:
        result = extract_salary(r['description_snippet'])
        if result and not result.get('skip'):
            cur.execute("""
                UPDATE job_postings_gold
                SET salary_min_usd = ?, salary_max_usd = ?,
                    salary_currency = ?, salary_period = ?, salary_text = ?
                WHERE gold_id = ?
            """, (result['salary_min_usd'], result['salary_max_usd'],
                  result['salary_currency'], result['salary_period'],
                  result['salary_text'], r['gold_id']))
            mined += 1
        elif result and result.get('skip'):
            # Non-USD detected
            cur.execute("""
                INSERT INTO qa_violations (gold_id, rule_name, severity, details)
                VALUES (?, 'non_usd_salary', 'WARNING', 'Non-USD currency detected in body')
            """, (r['gold_id'],))
    conn.commit()
    conn.close()
    log(f"mine_salary_from_body: Mined salary for {mined} rows.")


# ═══════════════════════════════════════════════════════════════════════════════
# ENRICHMENT HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _http_get_json(url, headers=None, timeout=10):
    """Simple HTTP GET returning parsed JSON."""
    hdrs = headers or {}
    hdrs.setdefault('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)')
    hdrs.setdefault('Accept', 'application/json')
    req = urllib.request.Request(url, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode()), resp.status
    except urllib.error.HTTPError as e:
        return None, e.code
    except Exception:
        return None, 0


def _http_get_html(url, timeout=10):
    """Simple HTTP GET returning HTML text."""
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode('utf-8', errors='replace'), resp.status
    except urllib.error.HTTPError as e:
        return None, e.code
    except Exception:
        return None, 0


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: verify_and_enrich
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_verify_and_enrich():
    """Run all enrichment tiers in order."""
    conn = get_db()
    cur = conn.cursor()

    # PRE-STEP: mine salary from body (free)
    log("verify_and_enrich: PRE-STEP mine_salary_from_body")
    cmd_mine_salary_from_body()

    def _get_pending(platform_filter=None):
        """Get rows needing enrichment."""
        q = "SELECT * FROM job_postings_gold WHERE enrich_status IN ('pending','failed')"
        if platform_filter:
            q += " AND (source_platform = ? OR job_url LIKE ?)"
            return cur.execute(q, (platform_filter, f'%{platform_filter}%')).fetchall()
        return cur.execute(q).fetchall()

    def _update_enriched(gid, updates: dict, status='api_enriched'):
        """Apply enrichment updates to a row."""
        updates['enrich_status'] = status
        updates['url_checked_at'] = datetime.now().isoformat()
        set_clause = ', '.join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [gid]
        cur.execute(f"UPDATE job_postings_gold SET {set_clause} WHERE gold_id = ?", vals)

    # ── TIER 1A: Greenhouse ──
    log("verify_and_enrich: TIER 1A — Greenhouse")
    gh_rows = _get_pending('greenhouse')
    enriched_gh = 0
    for r in gh_rows:
        url = r['job_url']
        # Parse slug and job_id
        m = re.search(r'greenhouse\.io/([^/]+)/jobs/(\d+)', url)
        if not m:
            m = re.search(r'boards\.greenhouse\.io/([^/]+)/jobs/(\d+)', url)
        if not m:
            # Try from URL with gh_jid param
            m2 = re.search(r'gh_jid=(\d+)', url)
            slug_m = re.search(r'greenhouse\.io/([^/]+)', url)
            if m2 and slug_m:
                slug, jid = slug_m.group(1), m2.group(1)
            else:
                continue
        else:
            slug, jid = m.group(1), m.group(2)

        api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{jid}?pay_transparency=true"
        data, status = _http_get_json(api_url)
        time.sleep(0.5)

        if status == 200 and data:
            updates = {'url_http_status': 200}
            loc = data.get('location', {})
            if isinstance(loc, dict) and loc.get('name'):
                updates['location_standardized'] = loc['name']
            # Pay transparency
            pay = data.get('pay_input_ranges', [])
            if pay and isinstance(pay, list) and len(pay) > 0:
                p = pay[0]
                if p.get('min_cents') and p.get('max_cents'):
                    min_usd = p['min_cents'] / 100
                    max_usd = p['max_cents'] / 100
                    curr = p.get('currency_type', 'USD')
                    if curr == 'USD' and 15000 <= min_usd <= 600000:
                        updates['salary_min_usd'] = int(min_usd)
                        updates['salary_max_usd'] = int(max_usd)
                        updates['salary_currency'] = 'USD'
                        updates['salary_period'] = 'Annual'
            # Work mode from metadata
            meta = data.get('metadata', [])
            for md in (meta or []):
                if md.get('name') == 'Location Type' and md.get('value'):
                    updates['work_mode'] = md['value']
            # Check if closed
            content = json.dumps(data).lower()
            if 'this job has been filled' in content or not data.get('title'):
                updates['status'] = 'Closed'

            _update_enriched(r['gold_id'], updates)
            enriched_gh += 1
        elif status in (404, 410):
            _update_enriched(r['gold_id'], {'url_http_status': status, 'status': 'Closed'})
    conn.commit()
    log(f"  Greenhouse: enriched {enriched_gh}/{len(gh_rows)}")

    # ── TIER 1B: Ashby ──
    log("verify_and_enrich: TIER 1B — Ashby")
    ashby_rows = _get_pending('ashby')
    enriched_ashby = 0
    for r in ashby_rows:
        url = r['job_url'].rstrip('/').replace('/application', '')
        m = re.search(r'ashbyhq\.com/([^/]+)/([^/]+)', url)
        if not m:
            continue
        slug = m.group(1)
        api_url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
        data, status = _http_get_json(api_url)
        time.sleep(0.5)

        if status == 200 and data:
            # Find matching job by UUID
            job_uuid = m.group(2)
            jobs = data.get('jobs', [])
            matched = None
            for j in jobs:
                if j.get('id') == job_uuid or job_uuid in str(j.get('id', '')):
                    matched = j
                    break
            if matched:
                updates = {'url_http_status': 200}
                if matched.get('location'):
                    updates['location_standardized'] = matched['location']
                wt = matched.get('workplaceType', '')
                if wt:
                    wt_map = {'Remote': 'Remote', 'Hybrid': 'Hybrid', 'OnSite': 'On-site'}
                    updates['work_mode'] = wt_map.get(wt, wt)
                comp = matched.get('compensationTierSummary', '')
                if comp:
                    sal = extract_salary(comp)
                    if sal and not sal.get('skip'):
                        updates.update({k: v for k, v in sal.items()})
                _update_enriched(r['gold_id'], updates)
                enriched_ashby += 1
    conn.commit()
    log(f"  Ashby: enriched {enriched_ashby}/{len(ashby_rows)}")

    # ── TIER 1C: Lever ──
    log("verify_and_enrich: TIER 1C — Lever")
    lever_rows = _get_pending('lever')
    enriched_lever = 0
    for r in lever_rows:
        m = re.search(r'lever\.co/([^/]+)/([a-f0-9-]+)', r['job_url'])
        if not m:
            continue
        slug, pid = m.group(1), m.group(2)
        api_url = f"https://api.lever.co/v0/postings/{slug}/{pid}?mode=json"
        data, status = _http_get_json(api_url)
        time.sleep(0.5)

        if status == 200 and data:
            updates = {'url_http_status': 200}
            cats = data.get('categories', {})
            if cats.get('location'):
                updates['location_raw'] = cats['location']
            wt = data.get('workplaceType', '')
            if wt:
                updates['work_mode'] = wt.capitalize()
            sal = data.get('salaryRange', {})
            if sal and sal.get('min') and sal.get('max'):
                currency = sal.get('currency', 'USD')
                if currency == 'USD':
                    interval = sal.get('interval', 'per-year')
                    min_v = sal['min']
                    max_v = sal['max']
                    if 'month' in interval:
                        min_v *= 12; max_v *= 12
                    elif 'hour' in interval:
                        min_v *= 2080; max_v *= 2080
                    if 15000 <= min_v <= 600000:
                        updates['salary_min_usd'] = int(min_v)
                        updates['salary_max_usd'] = int(max_v)
                        updates['salary_currency'] = 'USD'
                        updates['salary_period'] = 'Annual'
            created = data.get('createdAt')
            if created:
                updates['posted_date'] = datetime.fromtimestamp(created / 1000).strftime('%Y-%m-%d')
            _update_enriched(r['gold_id'], updates)
            enriched_lever += 1
        elif status in (404, 410):
            _update_enriched(r['gold_id'], {'url_http_status': status, 'status': 'Closed'})
    conn.commit()
    log(f"  Lever: enriched {enriched_lever}/{len(lever_rows)}")

    # ── TIER 2A: JSON-LD from job page ──
    log("verify_and_enrich: TIER 2A — JSON-LD extraction")
    pending = cur.execute("""
        SELECT * FROM job_postings_gold
        WHERE enrich_status IN ('pending', 'failed') AND job_url IS NOT NULL
    """).fetchall()
    enriched_ld = 0
    for r in pending[:100]:  # Batch limit to avoid timeout
        html, status = _http_get_html(r['job_url'])
        time.sleep(1.5)
        if status == 200 and html:
            updates = {'url_http_status': 200}
            # Parse JSON-LD
            ld_matches = re.findall(
                r'<script\s+type=["\']application/ld\+json["\']\s*>(.*?)</script>',
                html, re.DOTALL | re.IGNORECASE
            )
            for ld_text in ld_matches:
                try:
                    ld = json.loads(ld_text)
                    if isinstance(ld, list):
                        ld = next((x for x in ld if x.get('@type') == 'JobPosting'), None)
                    if not ld or ld.get('@type') != 'JobPosting':
                        continue
                    # Location
                    jl = ld.get('jobLocation', {})
                    if isinstance(jl, dict):
                        addr = jl.get('address', {})
                        if isinstance(addr, dict):
                            if addr.get('addressLocality'):
                                updates['location_city'] = addr['addressLocality']
                            if addr.get('addressRegion'):
                                updates['location_state'] = addr['addressRegion']
                            if addr.get('addressCountry'):
                                country_val = addr['addressCountry']
                                if isinstance(country_val, dict):
                                    country_val = country_val.get('name', '')
                                updates['country'] = country_val
                    # Remote
                    if ld.get('jobLocationType') == 'TELECOMMUTE':
                        updates['work_mode'] = 'Remote'
                    # Date
                    if ld.get('datePosted'):
                        updates['posted_date'] = str(ld['datePosted'])[:10]
                    # Salary
                    bs = ld.get('baseSalary', {})
                    if isinstance(bs, dict):
                        val = bs.get('value', {})
                        if isinstance(val, dict) and val.get('minValue') and val.get('maxValue'):
                            curr = bs.get('currency', 'USD')
                            if curr == 'USD':
                                min_v = float(val['minValue'])
                                max_v = float(val['maxValue'])
                                unit = val.get('unitText', 'YEAR')
                                if 'HOUR' in unit.upper():
                                    min_v *= 2080; max_v *= 2080
                                if 15000 <= min_v <= 600000:
                                    updates['salary_min_usd'] = int(min_v)
                                    updates['salary_max_usd'] = int(max_v)
                                    updates['salary_currency'] = 'USD'
                                    updates['salary_period'] = 'Annual'
                    # Description snippet
                    desc = ld.get('description', '')
                    if desc and not r['description_snippet']:
                        # Strip HTML tags
                        clean = re.sub(r'<[^>]+>', ' ', desc)
                        clean = re.sub(r'\s+', ' ', clean).strip()[:500]
                        updates['description_snippet'] = clean
                    break
                except (json.JSONDecodeError, KeyError):
                    continue

            if len(updates) > 1:
                _update_enriched(r['gold_id'], updates)
                enriched_ld += 1
            else:
                _update_enriched(r['gold_id'], {'url_http_status': status}, 'pending')
        elif status in (404, 410):
            _update_enriched(r['gold_id'], {'url_http_status': status, 'status': 'Closed'}, 'failed')
    conn.commit()
    log(f"  JSON-LD: enriched {enriched_ld}/{len(pending[:100])}")

    # ── TIER 2B: Salary from description regex (already done in mine_salary_from_body) ──
    log("verify_and_enrich: TIER 2B — salary regex (already done in pre-step)")

    # ── Final summary ──
    stats = cur.execute("""
        SELECT enrich_status, COUNT(*) as cnt
        FROM job_postings_gold GROUP BY enrich_status
    """).fetchall()
    log("verify_and_enrich COMPLETE. Status distribution:")
    for s in stats:
        log(f"  {s['enrich_status']}: {s['cnt']}")

    sal_count = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE salary_min_usd IS NOT NULL AND is_us=1"
    ).fetchone()[0]
    total = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]
    log(f"  Salary coverage: {sal_count}/{total} ({100*sal_count//max(total,1)}%)")

    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND: check_freshness
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_check_freshness():
    """Check if job posting URLs are still live. Marks dead ones as Closed."""
    conn = get_db()
    cur = conn.cursor()

    # Only check open US rows that have URLs and haven't been checked in 3+ days
    rows = cur.execute("""
        SELECT gold_id, job_url, company_name, title, url_http_status, url_checked_at
        FROM job_postings_gold
        WHERE is_us = 1 AND status = 'Open'
          AND job_url IS NOT NULL AND job_url != ''
          AND (url_checked_at IS NULL
               OR url_checked_at < datetime('now', '-3 days'))
        ORDER BY url_checked_at ASC NULLS FIRST
        LIMIT 200
    """).fetchall()

    log(f"check_freshness: Checking {len(rows)} URLs")
    if not rows:
        log("  No URLs need checking")
        return

    alive = 0
    dead = 0
    errors = 0
    now_iso = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for i, (gid, url, company, title, prev_status, prev_checked) in enumerate(rows):
        if i % 20 == 0 and i > 0:
            log(f"  Progress: {i}/{len(rows)} checked...")
            conn.commit()

        try:
            req = urllib.request.Request(url, method='HEAD', headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
            })
            resp = urllib.request.urlopen(req, timeout=10)
            http_code = resp.getcode()

            cur.execute("""UPDATE job_postings_gold
                SET url_http_status = ?, url_checked_at = ?
                WHERE gold_id = ?""", (http_code, now_iso, gid))

            if http_code == 200:
                alive += 1
            else:
                log(f"  [{gid}] {http_code} {title[:40]} @ {company}")

        except urllib.error.HTTPError as e:
            http_code = e.code
            cur.execute("""UPDATE job_postings_gold
                SET url_http_status = ?, url_checked_at = ?
                WHERE gold_id = ?""", (http_code, now_iso, gid))

            if http_code in (404, 410):
                cur.execute("""UPDATE job_postings_gold
                    SET status = 'Closed'
                    WHERE gold_id = ?""", (gid,))
                dead += 1
                log(f"  [{gid}] CLOSED ({http_code}) {title[:40]} @ {company}")
            else:
                log(f"  [{gid}] HTTP {http_code} {title[:40]} @ {company}")
                errors += 1

        except Exception:
            cur.execute("""UPDATE job_postings_gold
                SET url_http_status = -1, url_checked_at = ?
                WHERE gold_id = ?""", (now_iso, gid))
            errors += 1

        # Small delay to be polite
        time.sleep(0.3)

    conn.commit()

    log(f"check_freshness COMPLETE:")
    log(f"  Checked:    {len(rows)}")
    log(f"  Alive:      {alive}")
    log(f"  Closed:     {dead}")
    log(f"  Errors:     {errors}")

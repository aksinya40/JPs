"""
AI Analyst Jobs — Export, Approval & Analysis
=================================================
CSV export, QA report generation, approval gate,
Plotly dashboard, and markdown research report.
"""
import csv
import json
import random
import sys
from datetime import datetime

from pipeline.db import get_db, log, REVIEW_DIR, OUTPUT_DIR
from pipeline.qa import cmd_qa_check


def cmd_export_review():
    """Export all CSVs to review/ + qa_report.json + random_spot_check_30.csv."""
    REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_db()
    cur = conn.cursor()

    # Export job_postings_gold.csv
    rows = cur.execute("SELECT * FROM job_postings_gold ORDER BY gold_id").fetchall()
    _export_csv(rows, REVIEW_DIR / "job_postings_gold.csv")
    log(f"  Exported {len(rows)} rows to job_postings_gold.csv")

    # Export companies_200.csv
    companies = cur.execute("SELECT * FROM companies_200 ORDER BY company_id").fetchall()
    _export_csv(companies, REVIEW_DIR / "companies_200.csv")
    log(f"  Exported {len(companies)} rows to companies_200.csv")

    # Export qa_violations.csv
    violations = cur.execute("SELECT * FROM qa_violations ORDER BY violation_id").fetchall()
    _export_csv(violations, REVIEW_DIR / "qa_violations.csv")
    log(f"  Exported {len(violations)} violations to qa_violations.csv")

    # Export source_attempts.csv
    attempts = cur.execute("SELECT * FROM source_attempts ORDER BY attempt_id").fetchall()
    _export_csv(attempts, REVIEW_DIR / "source_attempts.csv")

    # Random spot check
    active_rows = cur.execute(
        "SELECT gold_id, company_name, title, job_url, source_platform, salary_min_usd, salary_max_usd "
        "FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchall()
    sample_size = min(30, len(active_rows))
    spot_check = random.sample(active_rows, sample_size) if active_rows else []
    _export_csv(spot_check, REVIEW_DIR / "random_spot_check_30.csv")
    log(f"  Exported {sample_size} rows to random_spot_check_30.csv")

    # QA Report JSON
    report = _build_qa_report(cur)
    with open(REVIEW_DIR / "qa_report.json", 'w') as f:
        json.dump(report, f, indent=2, default=str)
    log(f"  Exported qa_report.json")

    conn.close()
    log("export_review COMPLETE.")


def _export_csv(rows, path):
    """Export sqlite3.Row list to CSV."""
    if not rows:
        with open(path, 'w') as f:
            f.write('')
        return
    keys = rows[0].keys()
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow(dict(r))


def _build_qa_report(cur) -> dict:
    """Build the comprehensive qa_report.json."""
    report = {}
    report['generated_at'] = datetime.now().isoformat()

    # Row counts
    report['total_gold_rows'] = cur.execute("SELECT COUNT(*) FROM job_postings_gold").fetchone()[0]
    report['active_us_rows'] = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]
    report['excluded_non_us'] = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE status='Excluded-NonUS'"
    ).fetchone()[0]
    report['closed_rows'] = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE status='Closed'"
    ).fetchone()[0]

    active = report['active_us_rows'] or 1
    sal = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE salary_min_usd IS NOT NULL AND is_us=1 AND status='Open'"
    ).fetchone()[0]
    report['pct_with_salary'] = round(100 * sal / active, 1)

    ai_title = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE has_ai_in_title=1 AND is_us=1 AND status='Open'"
    ).fetchone()[0]
    report['pct_with_ai_in_title'] = round(100 * ai_title / active, 1)

    uncertain = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE date_uncertain=1 AND is_us=1 AND status='Open'"
    ).fetchone()[0]
    report['pct_date_uncertain'] = round(100 * uncertain / active, 1)

    # Enrich status distribution
    report['enrich_status_distribution'] = {}
    for r in cur.execute(
        "SELECT enrich_status, COUNT(*) as cnt FROM job_postings_gold GROUP BY enrich_status"
    ).fetchall():
        report['enrich_status_distribution'][r['enrich_status']] = r['cnt']

    # Top role clusters
    report['top_role_clusters'] = {}
    for r in cur.execute("""
        SELECT role_cluster, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' GROUP BY role_cluster ORDER BY cnt DESC LIMIT 10
    """).fetchall():
        report['top_role_clusters'][r['role_cluster']] = r['cnt']

    # Top companies
    report['top_companies'] = {}
    for r in cur.execute("""
        SELECT company_name, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' GROUP BY company_name ORDER BY cnt DESC LIMIT 20
    """).fetchall():
        report['top_companies'][r['company_name']] = r['cnt']

    # Top AI keywords
    report['top_ai_keywords'] = _count_csv_field(cur, 'ai_keywords_hit', 25)
    report['top_title_ai_terms'] = _count_csv_field(cur, 'title_ai_terms', 15)
    report['top_ai_role_signatures'] = {}
    for r in cur.execute("""
        SELECT ai_role_signature, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' AND ai_role_signature IS NOT NULL
        GROUP BY ai_role_signature ORDER BY cnt DESC
    """).fetchall():
        report['top_ai_role_signatures'][r['ai_role_signature']] = r['cnt']

    # Violations
    report['critical_violation_count'] = cur.execute(
        "SELECT COUNT(*) FROM qa_violations WHERE severity='CRITICAL'"
    ).fetchone()[0]
    report['warning_violation_count'] = cur.execute(
        "SELECT COUNT(*) FROM qa_violations WHERE severity='WARNING'"
    ).fetchone()[0]

    # Companies represented
    report['companies_represented'] = cur.execute("""
        SELECT COUNT(DISTINCT company_name) FROM job_postings_gold
        WHERE is_us=1 AND status='Open'
    """).fetchone()[0]

    # Source platform distribution
    report['source_platform_distribution'] = {}
    for r in cur.execute("""
        SELECT source_platform, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' GROUP BY source_platform ORDER BY cnt DESC
    """).fetchall():
        report['source_platform_distribution'][r['source_platform']] = r['cnt']

    # Work mode distribution
    report['work_mode_distribution'] = {}
    for r in cur.execute("""
        SELECT work_mode, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' GROUP BY work_mode ORDER BY cnt DESC
    """).fetchall():
        report['work_mode_distribution'][r['work_mode'] or 'Unknown'] = r['cnt']

    # Window bucket distribution
    report['window_bucket_distribution'] = {}
    for r in cur.execute("""
        SELECT window_bucket, COUNT(*) as cnt FROM job_postings_gold
        WHERE is_us=1 AND status='Open' GROUP BY window_bucket ORDER BY cnt DESC
    """).fetchall():
        report['window_bucket_distribution'][r['window_bucket'] or 'UNCERTAIN'] = r['cnt']

    # Approval state
    approval = cur.execute("SELECT * FROM approval_state ORDER BY id DESC LIMIT 1").fetchone()
    if approval:
        report['approval_state'] = {
            'approved': bool(approval['approved_by_user']),
            'approved_at': approval['approved_at'],
            'row_count_at_approval': approval['row_count_at_approval'],
        }
    else:
        report['approval_state'] = {'approved': False}

    return report


def _count_csv_field(cur, field: str, top_n: int) -> dict:
    """Count frequency of comma-separated values in a field."""
    counts = {}
    rows = cur.execute(f"""
        SELECT {field} FROM job_postings_gold
        WHERE is_us=1 AND status='Open' AND {field} IS NOT NULL AND {field} != ''
    """).fetchall()
    for r in rows:
        val = r[0]
        if not val:
            continue
        # Handle JSON arrays
        if val.startswith('['):
            try:
                items = json.loads(val)
            except json.JSONDecodeError:
                items = [v.strip() for v in val.split(',')]
        else:
            items = [v.strip() for v in val.split(',')]
        for item in items:
            item = item.strip().strip('"').strip("'")
            if item:
                counts[item] = counts.get(item, 0) + 1
    # Sort and return top N
    sorted_items = sorted(counts.items(), key=lambda x: -x[1])[:top_n]
    return dict(sorted_items)


def cmd_approve_db():
    """Inline qa_check → block if CRITICAL > 0 → insert approval_state row."""
    critical, warning = cmd_qa_check()

    if critical > 0:
        log(f"approve_db: BLOCKED — {critical} CRITICAL violations found.")
        log("  Fix all CRITICAL violations before approving.")
        log("  Run: python scripts/job_db_pipeline.py export_review")
        log("  Then review review/qa_violations.csv")
        return False

    conn = get_db()
    cur = conn.cursor()
    active = cur.execute(
        "SELECT COUNT(*) FROM job_postings_gold WHERE is_us=1 AND status='Open'"
    ).fetchone()[0]

    cur.execute("DELETE FROM approval_state")
    cur.execute("""
        INSERT INTO approval_state (id, approved_by_user, approved_at,
                                    row_count_at_approval, critical_violations,
                                    warning_violations)
        VALUES (1, 1, ?, ?, 0, ?)
    """, (datetime.now().isoformat(), active, warning))
    conn.commit()
    conn.close()

    log(f"approve_db: APPROVED with {active} active rows, {warning} warnings.")
    return True


def cmd_analyze_approved():
    """HARD BLOCKED until approval_state.approved_by_user=1; runs full Phase 2."""
    conn = get_db()
    cur = conn.cursor()

    approval = cur.execute(
        "SELECT approved_by_user FROM approval_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not approval or not approval['approved_by_user']:
        log("analyze_approved: HARD BLOCKED — DB not approved.")
        log("  Run: python scripts/job_db_pipeline.py approve_db")
        conn.close()
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build QA report data
    report = _build_qa_report(cur)

    # ── Generate Dashboard HTML ──
    log("analyze_approved: Generating dashboard...")
    _generate_dashboard(cur, report)

    # ── Generate Markdown Report ──
    log("analyze_approved: Generating markdown report...")
    _generate_markdown_report(cur, report)

    conn.close()
    log("analyze_approved COMPLETE.")
    log(f"  Dashboard: {OUTPUT_DIR / 'dashboard.html'}")
    log(f"  Report:    {OUTPUT_DIR / 'AI_Analyst_Roles_Research_2026.md'}")


def _generate_dashboard(cur, report: dict):
    """Generate Plotly interactive dashboard HTML."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
        import plotly.io as pio
    except ImportError:
        log("  WARNING: plotly not installed. Run: pip install plotly")
        log("  Skipping dashboard generation.")
        return

    figs = []

    # 1. Role cluster distribution
    rc = report.get('top_role_clusters', {})
    if rc:
        fig1 = go.Figure(go.Bar(x=list(rc.values()), y=list(rc.keys()),
                                orientation='h', marker_color='#4C78A8'))
        fig1.update_layout(title='Role Cluster Distribution', xaxis_title='Count',
                          height=500, margin=dict(l=200))
        figs.append(fig1)

    # 2. Work mode distribution
    wm = report.get('work_mode_distribution', {})
    if wm:
        fig2 = go.Figure(go.Bar(x=list(wm.keys()), y=list(wm.values()),
                                marker_color='#72B7B2'))
        fig2.update_layout(title='Work Mode Distribution', yaxis_title='Count')
        figs.append(fig2)

    # 3. Posting volume by time bucket
    wb = report.get('window_bucket_distribution', {})
    if wb:
        fig3 = go.Figure(go.Bar(x=list(wb.keys()), y=list(wb.values()),
                                marker_color='#F58518'))
        fig3.update_layout(title='Posting Volume by Time Bucket', yaxis_title='Count')
        figs.append(fig3)

    # 4. Top 20 companies
    tc = report.get('top_companies', {})
    if tc:
        fig4 = go.Figure(go.Bar(x=list(tc.values()), y=list(tc.keys()),
                                orientation='h', marker_color='#E45756'))
        fig4.update_layout(title='Top 20 Companies by Posting Count',
                          xaxis_title='Count', height=600, margin=dict(l=200))
        figs.append(fig4)

    # 5. AI keyword frequency
    ak = report.get('top_ai_keywords', {})
    if ak:
        fig5 = go.Figure(go.Bar(x=list(ak.values()), y=list(ak.keys()),
                                orientation='h', marker_color='#54A24B'))
        fig5.update_layout(title='Top AI Keywords from Descriptions',
                          xaxis_title='Frequency', height=700, margin=dict(l=200))
        figs.append(fig5)

    # 6. Title AI terms
    tt = report.get('top_title_ai_terms', {})
    if tt:
        fig6 = go.Figure(go.Bar(x=list(tt.values()), y=list(tt.keys()),
                                orientation='h', marker_color='#B279A2'))
        fig6.update_layout(title='Title AI Terms Frequency',
                          xaxis_title='Frequency', height=500, margin=dict(l=150))
        figs.append(fig6)

    # 7. Salary ranges box plot
    salary_data = cur.execute("""
        SELECT role_cluster, salary_min_usd, salary_max_usd
        FROM job_postings_gold
        WHERE salary_min_usd IS NOT NULL AND is_us=1 AND status='Open'
    """).fetchall()
    if salary_data:
        fig7 = go.Figure()
        clusters_sal = {}
        for r in salary_data:
            cl = r['role_cluster']
            if cl not in clusters_sal:
                clusters_sal[cl] = []
            clusters_sal[cl].extend([r['salary_min_usd'], r['salary_max_usd']])
        for cl, vals in sorted(clusters_sal.items()):
            fig7.add_trace(go.Box(y=vals, name=cl))
        fig7.update_layout(title='Salary Ranges by Role Cluster ($USD)',
                          yaxis_title='Annual Salary (USD)', height=500)
        figs.append(fig7)

    # 8. Top skills
    skills_data = _count_csv_field(cur, 'skills_extracted', 25)
    if skills_data:
        fig8 = go.Figure(go.Bar(x=list(skills_data.values()),
                                y=list(skills_data.keys()),
                                orientation='h', marker_color='#FF9DA6'))
        fig8.update_layout(title='Top 25 Skills', xaxis_title='Frequency',
                          height=700, margin=dict(l=150))
        figs.append(fig8)

    # 9. Source platform distribution
    sp = report.get('source_platform_distribution', {})
    if sp:
        fig9 = go.Figure(go.Bar(x=list(sp.keys()), y=list(sp.values()),
                                marker_color='#9D755D'))
        fig9.update_layout(title='Source Platform Distribution', yaxis_title='Count')
        figs.append(fig9)

    # 10. ai_role_signature donut
    ars = report.get('top_ai_role_signatures', {})
    if ars:
        fig10 = go.Figure(go.Pie(labels=list(ars.keys()), values=list(ars.values()),
                                 hole=0.4))
        fig10.update_layout(title='AI Role Signature Distribution')
        figs.append(fig10)

    # Combine into single HTML
    html_parts = [f"""<!DOCTYPE html>
<html><head><title>AI Analyst Jobs Dashboard 2026</title>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 20px; background: #f8f9fa; }}
.header {{ background: #1a1a2e; color: white; padding: 30px; border-radius: 12px; margin-bottom: 30px; }}
.stats {{ display: flex; gap: 20px; flex-wrap: wrap; margin-top: 15px; }}
.stat {{ background: rgba(255,255,255,0.1); padding: 15px 25px; border-radius: 8px; }}
.stat-val {{ font-size: 28px; font-weight: bold; }}
.stat-label {{ font-size: 12px; opacity: 0.8; }}
.chart {{ background: white; border-radius: 12px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
</style></head><body>
<div class="header">
<h1>AI Analyst Roles — US Job Market Research 2026</h1>
<div class="stats">
<div class="stat"><div class="stat-val">{report['active_us_rows']}</div><div class="stat-label">Active US Postings</div></div>
<div class="stat"><div class="stat-val">{report['companies_represented']}</div><div class="stat-label">Companies</div></div>
<div class="stat"><div class="stat-val">{report['pct_with_salary']}%</div><div class="stat-label">With Salary</div></div>
<div class="stat"><div class="stat-val">{report['pct_with_ai_in_title']}%</div><div class="stat-label">AI in Title</div></div>
</div></div>"""]

    for i, fig in enumerate(figs):
        div_id = f"chart_{i}"
        fig_json = pio.to_json(fig)
        html_parts.append(f"""
<div class="chart"><div id="{div_id}"></div></div>
<script>Plotly.newPlot('{div_id}', {fig_json}.data, {fig_json}.layout, {{responsive: true}});</script>
""")

    html_parts.append("</body></html>")

    with open(OUTPUT_DIR / "dashboard.html", 'w') as f:
        f.write('\n'.join(html_parts))
    log(f"  Dashboard written: {OUTPUT_DIR / 'dashboard.html'}")


def _generate_markdown_report(cur, report: dict):
    """Generate the 10-section markdown research report."""
    sections = []

    # 1. Executive Summary
    sections.append(f"""# AI Analyst Roles — US Job Market Research 2026

## 1. Executive Summary

- **{report['active_us_rows']} active US job postings** across {report['companies_represented']} companies
- **{report['pct_with_salary']}% salary coverage** with data from ATS APIs and pay transparency disclosures
- **{report['pct_with_ai_in_title']}% of postings** have AI/LLM terms directly in the job title
- Top role clusters: {', '.join(list(report.get('top_role_clusters', {}).keys())[:5])}
- Sources: {', '.join(f"{k}({v})" for k, v in list(report.get('source_platform_distribution', {}).items())[:5])}
""")

    # 2. Dataset Overview
    sections.append(f"""## 2. Dataset Overview

| Metric | Value |
|--------|-------|
| Total gold rows | {report['total_gold_rows']} |
| Active US rows | {report['active_us_rows']} |
| Excluded (non-US) | {report['excluded_non_us']} |
| Closed rows | {report['closed_rows']} |
| With salary | {report['pct_with_salary']}% |
| AI in title | {report['pct_with_ai_in_title']}% |
| Date uncertain | {report['pct_date_uncertain']}% |
| Critical violations | {report['critical_violation_count']} |
| Warning violations | {report['warning_violation_count']} |
""")

    # 3. Role Family Landscape
    rc = report.get('top_role_clusters', {})
    sections.append(f"""## 3. Role Family Landscape

| Role Cluster | Count |
|-------------|-------|
""" + '\n'.join(f"| {k} | {v} |" for k, v in rc.items()))

    # 4. AI/LLM Signal Analysis
    ars = report.get('top_ai_role_signatures', {})
    sections.append(f"""## 4. AI/LLM Signal Analysis

| AI Role Signature | Count |
|------------------|-------|
""" + '\n'.join(f"| {k} | {v} |" for k, v in ars.items()))

    # 5. Compensation Benchmarks
    sections.append("""## 5. Compensation Benchmarks

See dashboard charts for interactive salary range analysis by cluster and tier.
""")

    # 6. Work Model Distribution
    wm = report.get('work_mode_distribution', {})
    sections.append(f"""## 6. Work Model Distribution

| Work Mode | Count |
|-----------|-------|
""" + '\n'.join(f"| {k} | {v} |" for k, v in wm.items()))

    # 7. Top Employers
    tc = report.get('top_companies', {})
    sections.append(f"""## 7. Top Employers & Tier Analysis

| Company | Postings |
|---------|----------|
""" + '\n'.join(f"| {k} | {v} |" for k, v in list(tc.items())[:20]))

    # 8. Skills Landscape
    sections.append("""## 8. Skills Landscape

See dashboard for interactive top-25 skills chart.
""")

    # 9. Emerging AI Title Patterns
    sections.append("""## 9. Emerging AI Title Patterns

See AI Role Signature analysis in Section 4 for emerging title patterns.
""")

    # 10. Methodology
    es = report.get('enrich_status_distribution', {})
    sections.append(f"""## 10. Methodology & Coverage Gaps

| Enrich Status | Count |
|--------------|-------|
""" + '\n'.join(f"| {k} | {v} |" for k, v in es.items()) + f"""

**Pipeline**: Two-source merge (Claude DB + Codex DB) → dedup → ATS API enrichment → JSON-LD → salary regex → QA gates → approval
**Date window**: 2025-07-01 to 2026-03-31
**Company scope**: 200 US big-tech & AI companies
""")

    md = '\n\n'.join(sections)
    with open(OUTPUT_DIR / "AI_Analyst_Roles_Research_2026.md", 'w') as f:
        f.write(md)
    log(f"  Report written: {OUTPUT_DIR / 'AI_Analyst_Roles_Research_2026.md'}")

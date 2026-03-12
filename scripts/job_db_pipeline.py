#!/usr/bin/env python3
"""
AI Analyst Jobs — Unified Research Pipeline
=============================================
CLI shim: dispatches commands to the pipeline package.

Usage:
    python scripts/job_db_pipeline.py <command>

Commands:
    init_db                 Create all tables + migrations
    merge_dbs               Merge Claude DB + Codex DB with dedup
    build_companies         Populate companies_200 (exactly 200)
    collect_ats             Fetch jobs from ATS APIs (Greenhouse/Lever/Ashby)
    ingest_raw              Promote raw_postings → job_postings_gold
    normalize_platforms     Canonicalize source_platform names
    mine_salary_from_body   Parse salary from stored descriptions (FREE, no HTTP)
    verify_and_enrich       Run enrichment Tiers 0 → 1 → 2A → 2B → 2C → 3
    backfill_title_ai       Compute has_ai_in_title + title_ai_terms
    backfill_ai_role_signature  Classify ai_role_signature for all rows
    backfill_skills         Extract skills_extracted, has_python, has_sql
    qa_check                Run all quality gates → qa_violations
    export_review           Export CSVs to review/ + qa_report.json
    approve_db              QA gate → block if CRITICAL > 0 → approval_state
    analyze_approved        HARD BLOCKED until approved; runs Phase 2
    fix_data_quality        Run 13-step data quality pipeline
    fix_title_normalization Re-normalize all titles to 15-segment taxonomy
    fix_all                 Run ALL fix steps in correct dependency order
    health_check            Full column-level validation + health score + JSON report
    check_freshness         Re-verify job URLs and check liveness
"""

import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path so `pipeline` package is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.db import cmd_init_db, log
from pipeline.ingest import cmd_merge_dbs, cmd_collect_ats, cmd_ingest_raw
from pipeline.companies import cmd_build_companies
from pipeline.fixers import (
    cmd_normalize_platforms,
    cmd_backfill_title_ai,
    cmd_backfill_ai_role_signature,
    cmd_backfill_skills,
    cmd_fix_data_quality,
)
from pipeline.enrichment import (
    cmd_mine_salary_from_body,
    cmd_verify_and_enrich,
    cmd_check_freshness,
)
from pipeline.qa import cmd_qa_check, cmd_health_check
from pipeline.runner import cmd_fix_all
from pipeline.export import cmd_export_review, cmd_approve_db, cmd_analyze_approved
from pipeline.titles import cmd_fix_title_normalization

COMMANDS = {
    'init_db': cmd_init_db,
    'merge_dbs': cmd_merge_dbs,
    'build_companies': cmd_build_companies,
    'normalize_platforms': cmd_normalize_platforms,
    'mine_salary_from_body': cmd_mine_salary_from_body,
    'verify_and_enrich': cmd_verify_and_enrich,
    'backfill_title_ai': cmd_backfill_title_ai,
    'backfill_ai_role_signature': cmd_backfill_ai_role_signature,
    'backfill_skills': cmd_backfill_skills,
    'qa_check': cmd_qa_check,
    'export_review': cmd_export_review,
    'approve_db': cmd_approve_db,
    'analyze_approved': cmd_analyze_approved,
    'fix_data_quality': cmd_fix_data_quality,
    'fix_title_normalization': cmd_fix_title_normalization,
    'fix_all': cmd_fix_all,
    'health_check': cmd_health_check,
    'collect_ats': cmd_collect_ats,
    'ingest_raw': cmd_ingest_raw,
    'check_freshness': cmd_check_freshness,
}


def main():
    parser = argparse.ArgumentParser(
        description='AI Analyst Jobs — Unified Research Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='\n'.join(f'  {k:30s}' for k in COMMANDS.keys())
    )
    parser.add_argument('command', choices=COMMANDS.keys(),
                        help='Pipeline command to run')
    args = parser.parse_args()

    log(f"Running: {args.command}")
    COMMANDS[args.command]()
    log(f"Done: {args.command}")


if __name__ == '__main__':
    main()

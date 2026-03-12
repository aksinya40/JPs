"""
AI Analyst Jobs — Pipeline Runner
====================================
Master orchestrator: cmd_fix_all() runs every fixer in correct
dependency order. Idempotent — safe to run anytime, any number of times.
"""
from pipeline.db import log
from pipeline.fixers import (
    cmd_normalize_platforms,
    cmd_fix_data_quality,
    cmd_backfill_skills,
    cmd_backfill_title_ai,
    cmd_backfill_ai_role_signature,
)
from pipeline.titles import cmd_fix_title_normalization


def cmd_fix_all():
    """Run every fix step in correct dependency order.

    Order matters:
        1. Normalize platforms (so downstream filters match canonical names)
        2. Fix data quality (11+2 steps: delete bad rows, fix fields, backfill)
        3. Backfill skills from full body_raw (needs clean snippets from step 2)
        4. Backfill title AI flags (needs clean titles from step 2)
        5. Backfill ai_role_signature (needs skills + title from steps 3-4)
        6. Fix title normalization (final pass — 7-step taxonomy)

    All steps are idempotent: running twice produces the same result.
    """
    log("fix_all: Starting full pipeline fix (6 steps)...")

    log("═══ Step 1/6: Normalize platforms ═══")
    cmd_normalize_platforms()

    log("═══ Step 2/6: Fix data quality (13 sub-steps) ═══")
    cmd_fix_data_quality()

    log("═══ Step 3/6: Backfill skills (body_raw join) ═══")
    cmd_backfill_skills()

    log("═══ Step 4/6: Backfill title AI flags ═══")
    cmd_backfill_title_ai()

    log("═══ Step 5/6: Backfill ai_role_signature ═══")
    cmd_backfill_ai_role_signature()

    log("═══ Step 6/6: Fix title normalization ═══")
    cmd_fix_title_normalization()

    log("fix_all COMPLETE: All 6 steps finished.")

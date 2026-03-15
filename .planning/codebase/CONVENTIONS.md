# Coding Conventions

**Analysis Date:** 2026-03-15

## Language and Runtime

**Primary:** Python 3.10+ (codebase uses int | None union syntax from 3.10)
**Style:** No formatter or linter configuration files detected. Code follows implicit PEP 8.

## Naming Patterns

**Files:**
- Pipeline modules: snake_case.py (e.g., pipeline/parsers.py, pipeline/filters.py)
- Test files: test_module.py matching the pipeline module they test (e.g., tests/test_parsers.py)
- Scripts: snake_case.py (e.g., scripts/job_db_pipeline.py)

**Functions:**
- Public functions: snake_case (e.g., extract_salary(), normalize_company())
- Command functions: cmd_command_name() prefix for all CLI-invocable commands
- Private/helper functions: _underscore_prefix (e.g., _parse_location(), _detect_seniority())

**Variables:**
- snake_case for all variables
- Loop variables: short single-letter for row iteration (r, c, m, d)
- Database row dicts: r, rd (after row_to_dict() conversion)
- Counters: descriptive names like imported_claude, enriched_gh, step1_fixed

**Constants:**
- UPPER_SNAKE_CASE for module-level constants
- All constants centralized in pipeline/constants.py
- Type-annotated with List, Tuple, Dict from typing module

## Code Style

**Formatting:**
- No automated formatter configured
- 4-space indentation throughout
- Line length: generally 80-120 characters, no strict limit enforced

**Linting:**
- No linter configured. Follow PEP 8 by convention.

## Module Design

Each pipeline module serves a single concern:
- pipeline/constants.py - Pure data declarations (no runtime logic)
- pipeline/parsers.py - Pure text transformation functions (no DB access)
- pipeline/filters.py - Classification and filtering functions (no DB access)
- pipeline/db.py - Database connection, helpers, schema init
- pipeline/ingest.py - Data ingestion (merge, collect, promote)
- pipeline/enrichment.py - ATS API enrichment
- pipeline/fixers.py - Data quality fixes and backfills
- pipeline/titles.py - Title normalization
- pipeline/qa.py - Quality assurance checks
- pipeline/export.py - CSV export, approval, dashboard generation
- pipeline/runner.py - Pipeline orchestration

**Key design rule:** Pure functions (parsers, filters) have no database access.

## Import Organization

**Order:**
1. Standard library imports (re, sqlite3, json, hashlib, sys, time)
2. Third-party imports (none in core pipeline; plotly only in pipeline/export.py)
3. Local pipeline imports (from pipeline.db import ...)

**Style:**
- Use from module import (specific, items) with parenthesized multi-line
- Group related imports from the same module

## Docstrings

**Module-level:** Every module has a module docstring with project name, header, and purpose.

**Function-level:** All public functions have single-line or short docstrings. Command functions include step descriptions in multi-line docstrings.

## Error Handling

1. Guard clauses with empty returns: if not text: return ''
2. Try/except with continue in loops for per-row error handling
3. HTTP error handling: differentiate 404/410 (permanent) from 403/5xx (transient)
4. Pre-ingest validation gate: validate_pre_ingest() returns error list
5. No custom exception classes

## Logging

Custom log() function in pipeline/db.py wrapping print() with timestamps.

Patterns:
- Every command starts/ends with log messages
- Sub-steps use indentation
- WARN: and ERROR prefixes for severity

## Database Patterns

- get_db() provides WAL-mode connection with row_factory = sqlite3.Row
- Each command opens/closes its own connection
- INSERT OR IGNORE for dedup
- CREATE TABLE IF NOT EXISTS for schema
- All fixer/backfill commands are idempotent

## CLI Pattern

Single entry point scripts/job_db_pipeline.py with argparse dispatching to cmd_* functions via COMMANDS dict.

---

*Convention analysis: 2026-03-15*

# Testing Patterns

**Analysis Date:** 2026-03-15

## Framework

- **Runner:** pytest
- **Config:** No `pytest.ini`, `pyproject.toml`, or `setup.cfg` — uses defaults
- **Test location:** `tests/` directory at project root
- **Total test files:** 7 (+ `conftest.py`)
- **Total test lines:** ~2,356

## Test File Organization

| Test File | Module Under Test | Test Count | Focus |
|-----------|-------------------|------------|-------|
| `test_parsers.py` | `pipeline/parsers.py` | ~35 | Pure text transformations |
| `test_filters.py` | `pipeline/filters.py` | ~30 | AI keyword matching, role classification |
| `test_skills.py` | `pipeline/parsers.py` | ~25 | Skill extraction specifically |
| `test_titles.py` | `pipeline/titles.py` | ~20 | Title normalization pipeline |
| `test_ingest.py` | `pipeline/ingest.py` | ~25 | Location parsing, seniority detection, validation |
| `test_qa.py` | `pipeline/qa.py` | ~15 | QA violation checks |
| `test_health_check.py` | `pipeline/qa.py` | ~35 | Health check scoring, field validation |

## Test Structure Patterns

### Class-per-function grouping
Tests are organized into classes named after the function they test:

```python
class TestNormalizeText:
    def test_basic(self):
        assert normalize_text("  Hello   World  ") == "hello world"

    def test_empty(self):
        assert normalize_text("") == ""
        assert normalize_text(None) == ""
```

### Section separators
Test files use Unicode box-drawing characters as visual section dividers:

```python
# ═══════════════════════════════════════════════════════════════════════════════
# normalize_text
# ═══════════════════════════════════════════════════════════════════════════════
```

### Test naming
- Pattern: `test_<scenario>` (e.g., `test_empty`, `test_basic`, `test_strips_inc`)
- Descriptive scenarios, not numbered
- Short docstrings for non-obvious test cases

## Fixtures

### Shared fixtures (`tests/conftest.py`)

**`in_memory_db`** — Fresh SQLite in-memory database:
```python
@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    yield conn
    conn.close()
```

**`sample_job_row`** — Representative gold-table row dict with all fields populated.

### Module-specific fixtures

**`test_qa.py`** has `qa_db` fixture that creates schema + seeds data:
```python
@pytest.fixture
def qa_db(in_memory_db):
    # Creates job_postings_gold table, seeds baseline rows
    ...
```

**`test_health_check.py`** has `hc_db` fixture with full schema + company table.

**`test_ingest.py`** has `ingest_db` fixture with raw_postings + gold tables.

## Mocking

- **No mocking framework** (no `unittest.mock`, `pytest-mock`, or `monkeypatch` usage detected)
- Tests use **in-memory SQLite** databases instead of mocking DB calls
- Pure functions tested with direct input/output assertions (no mocking needed)
- HTTP calls and external APIs are **not tested** (no mocking of `urllib`)

## What's Tested Well

1. **Pure parsers** — Comprehensive edge case coverage (empty, None, whitespace, special chars)
2. **Filters** — False positive detection, AI keyword matching, role exclusion
3. **Skills extraction** — Noise filtering (R language edge cases), category coverage
4. **Title normalization** — Seniority stripping, segment mapping, edge cases
5. **Location parsing** — US cities, states, international detection, remote/hybrid
6. **Seniority detection** — All levels from junior to VP, priority ordering
7. **Health check** — Field-level validation, scoring, critical vs warning

## Coverage Gaps

1. **`pipeline/enrichment.py`** — No tests (HTTP calls, ATS API, salary mining)
2. **`pipeline/export.py`** — No tests (CSV generation, dashboard, approval flow)
3. **`pipeline/companies.py`** — No tests (company list building)
4. **`pipeline/fixers.py`** — No tests (data quality fix pipeline)
5. **`pipeline/runner.py`** — No tests (orchestration)
6. **`pipeline/db.py`** — Schema init tested indirectly via fixtures only
7. **Integration tests** — No end-to-end pipeline tests
8. **`scripts/collect_jobs.py`** — No tests (job collection)

## Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_parsers.py

# Run specific test class
pytest tests/test_parsers.py::TestNormalizeText

# Verbose output
pytest -v
```

## Dependencies

- `pytest` (in `requirements.txt`)
- No additional test dependencies (no pytest plugins, no coverage tools configured)

---

*Testing analysis: 2026-03-15*

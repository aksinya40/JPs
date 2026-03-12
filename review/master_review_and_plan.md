# Master Review & 100% Validation Plan
_Date: 2026-03-12 | Goal: 100% validated AI Analyst Jobs database_

---

## PART 1 — WHAT WE BUILT (Review)

### Pipeline modules (all implemented)

| Module | Commands | Status |
|---|---|---|
| `db.py` | `cmd_init_db` | ✅ Done |
| `ingest.py` | `cmd_merge_dbs`, `cmd_collect_ats`, `cmd_ingest_raw` | ✅ Done |
| `fixers.py` | `cmd_fix_data_quality` (11 steps), `cmd_backfill_skills`, `cmd_backfill_title_ai`, `cmd_backfill_ai_role_signature`, `cmd_normalize_platforms` | ✅ Done |
| `titles.py` | `cmd_fix_title_normalization` (7 steps) | ✅ Done, NOT run on full DB |
| `qa.py` | `cmd_qa_check` | ✅ Done |
| `filters.py` | `is_role_excluded`, `classify_ai_role_signature`, `resolve_work_mode` | ✅ Done |
| `parsers.py` | `canonicalize_platform`, `extract_skills`, `normalize_title_to_segment`, `match_ai_keywords` | ✅ Done |
| `tests/test_qa.py` | Full QA rule coverage | ✅ Done |

### QA system
- `qa_violations` table with CRITICAL / WARNING severity
- 12 CRITICAL rules + 7 WARNING rules implemented
- Tests cover all major rules with `tmp_path` monkeypatch pattern

### Title normalization taxonomy (designed, not fully applied)
- **Type A**: AI IS the role → `"AI [function]"` (strips all qualifiers, team names, seniority)
- **Type B**: AI is team context → `"[Role] (AI Team)"` (all GenAI/Agentic/LLM/AGI variants → single suffix)
- **Verbatim fix**: `title_normalized = title` copies → normalized via `_normalize_verbatim()`
- Target: ~50 distinct title_normalized values

---

## PART 2 — CURRENT DB STATE (Snapshot as of 2026-03-12)

```
Total rows:               3,061
Active (Open + US):       2,976   ← massive growth from original 370
Excluded:                    76
Closed:                       9

Distinct title_normalized:  1,576  ← TARGET: ~50   [CRITICAL GAP]
Verbatim copies remaining:  2,214  ← 72% of rows   [CRITICAL GAP]
Type A rows (AI X):           179
Type B rows (X AI Team):        1  ← pipeline NOT run [CRITICAL GAP]

has_ai_in_title = 1:          750
Inconsistent ai signal:         0  ← clean
AGI regex bug:                  0  ← clean

QA CRITICAL violations:         0  ← clean
QA WARNING violations:        204
  └─ missing_description:     203  ← dominant issue
  └─ unknown_company:           1

HTML still in snippets:       161
Missing posted_date:          239
Missing skills_extracted:     885  ← 30% of active rows
```

### What's working well
- DB has 3,061 rows — excellent coverage after ATS collect runs
- AI signal filtering at ingest is clean (0 inconsistencies)
- QA has 0 CRITICAL violations
- Schema UNIQUE constraint on `canonical_job_key` prevents duplication
- Role exclusion filter at ingest is working

### Critical gaps
1. **title_normalized is broken** — 2,214 rows (72%) still have verbatim copies; only 1 Type B row exists
2. **885 rows missing skills** — `cmd_backfill_skills()` not run on expanded dataset
3. **161 rows with HTML in snippets** — `cmd_fix_data_quality()` not run since expansion
4. **239 rows missing posted_date** — needs enrichment or date_uncertain flag
5. **203 WARNING: missing_description** — snippets <50 chars; some expected for new ingest

---

## PART 3 — ACTION PLAN (ordered execution)

### Phase 1 — Data Fixes (run in coding session)

**Step 1.1 — Run full fix_data_quality on expanded dataset**
```python
cmd_fix_data_quality()
```
Fixes: HTML in snippets (161), work_mode contradictions, salary corruption, aggregator cleanup.
After: HTML in snippets → 0, work_mode clean.

**Step 1.2 — Run backfill_skills with body_raw join**
```python
cmd_backfill_skills()
```
Fixes: 885 rows with empty skills_extracted.
After: skills_extracted populated from full body_raw (6k+ chars), not just 500-char snippet.

**Step 1.3 — Run cmd_fix_title_normalization on ALL rows**
```python
cmd_fix_title_normalization()
```
This is the big one. Applies all 7 steps to 3,061 rows:
- AGI regex fix
- ai_role_signature reclassification
- Type A normalization: `"AI [function]"`
- Type B normalization: `"[Role] (AI Team)"`
- Verbatim copy fix
- Out-of-scope flagging
Target after: distinct title_normalized ≤ 60, verbatim = 0, Type B ≥ 40.

**Step 1.4 — Re-run qa_check and verify**
```python
cmd_qa_check()
```
After Phase 1 fixes: expect CRITICAL = 0, WARNING < 20.

---

### Phase 2 — Title Normalization Validation

After Step 1.3, run these validation queries manually:

```sql
-- Check 1: Verbatim copies should be 0
SELECT COUNT(*) FROM job_postings_gold
WHERE title_normalized = title AND title_normalized NOT LIKE '%(AI Team)%'
AND title_normalized NOT LIKE 'AI %';

-- Check 2: Type B count should be 40+
SELECT COUNT(*) FROM job_postings_gold
WHERE title_normalized LIKE '%(AI Team)%';

-- Check 3: Distinct count should be < 60
SELECT COUNT(DISTINCT title_normalized) FROM job_postings_gold;

-- Check 4: Top 20 titles to spot-check
SELECT title_normalized, COUNT(*) as n
FROM job_postings_gold
GROUP BY title_normalized ORDER BY n DESC LIMIT 20;

-- Check 5: Sample verbatim survivors (should be 0)
SELECT title, title_normalized FROM job_postings_gold
WHERE title_normalized = title LIMIT 20;
```

If any verbatim copies remain after Step 1.3, add them to `TITLE_SEGMENTS_EXTENDED` in `titles.py` and re-run.

---

### Phase 3 — Missing Data Enrichment

**Step 3.1 — posted_date for 239 rows**
Options (in order of effort):
1. If raw_postings has scraped_at timestamp → use that as fallback, set `date_uncertain = 1`
2. Re-hit ATS API for those companies to get fresh date
3. Mark as `date_uncertain = 1` with `posted_date = NULL` — they'll be excluded from date_out_of_window QA rule

Recommended: Option 1 — add to `cmd_fix_data_quality()` as Step 12:
```sql
UPDATE job_postings_gold g
SET posted_date = DATE(r.scraped_at), date_uncertain = 1
WHERE g.posted_date IS NULL
AND EXISTS (SELECT 1 FROM raw_postings r
            WHERE r.source_job_id = g.source_job_id
            AND r.source_platform = g.source_platform)
```

**Step 3.2 — description_snippet for 203 warning rows**
Short snippets (<50 chars) often mean the ATS page returned minimal content.
Options:
1. Re-scrape from job_url for those gold_id rows
2. Pull from `raw_postings.body_raw` (first 500 chars stripped of HTML)

Add as Step 13 in `cmd_fix_data_quality()`:
```sql
UPDATE job_postings_gold g
SET description_snippet = SUBSTR(TRIM(r.body_raw), 1, 500)
WHERE (g.description_snippet IS NULL OR LENGTH(g.description_snippet) < 50)
AND EXISTS (SELECT 1 FROM raw_postings r
            WHERE r.source_job_id = g.source_job_id
            AND r.source_platform = g.source_platform
            AND LENGTH(r.body_raw) > 50)
```

---

### Phase 4 — QA Hardening

**Step 4.1 — Pre-ingest validation** (implement this, it's worth it)

Add `validate_pre_ingest(raw_row)` to `ingest.py` before `cmd_ingest_raw` promotes rows:

```python
def validate_pre_ingest(r) -> list[str]:
    """Return list of error strings; empty = valid."""
    errors = []
    if not r.get('title') or len(r['title']) < 3:
        errors.append('title missing or too short')
    if not r.get('job_url') or not r['job_url'].startswith('http'):
        errors.append('invalid job_url')
    if not r.get('source_job_id'):
        errors.append('source_job_id missing')
    if not r.get('company_name'):
        errors.append('company_name missing')
    if r.get('salary_min_usd') and r['salary_min_usd'] > 0:
        if r['salary_min_usd'] < 10000:
            errors.append(f'salary_min_usd suspiciously low: {r["salary_min_usd"]}')
    return errors
```

This prevents dirty data entering gold table in the first place.

**Step 4.2 — Add test for title normalization** (new test class)

Add `TestTitleNormalization` to `tests/test_titles.py`:
```python
@pytest.mark.parametrize("raw,expected", [
    ("Senior AI Red Team Analyst", "AI Analyst"),
    ("Lead Data Scientist - Agentic AI", "Lead Data Scientist (AI Team)"),
    ("GenAI Platform Engineer", "Platform Engineer (AI Team)"),
    ("Director of AI", "AI Director"),
    ("Machine Learning Engineer (AI Team)", "ML Engineer (AI Team)"),
    ("AI Data Analyst II", "AI Data Analyst"),
])
def test_type_a_normalization(raw, expected):
    assert normalize_title_to_segment(raw) == expected
```

**Step 4.3 — Add QA rule: title_normalization_coverage**

Add to `cmd_qa_check()`:
```python
# title_normalization_coverage — verbatim copies are CRITICAL
verbatim = cur.execute("""
    SELECT COUNT(*) FROM job_postings_gold
    WHERE title_normalized = title
    AND title_normalized NOT LIKE '%(AI Team)%'
    AND title_normalized NOT LIKE 'AI %'
    AND status = 'Open'
""").fetchone()[0]
if verbatim > 0:
    _add_violation(None, 'title_not_normalized', 'CRITICAL',
                   f'{verbatim} rows have title_normalized = title (verbatim copy)')
```

This makes verbatim copies permanently detectable by QA.

---

### Phase 5 — Final 100% Validation Checklist

Run this checklist in order after all phases are complete:

```
[ ] cmd_fix_data_quality()       → HTML=0, aggregators=0, work_mode clean
[ ] cmd_backfill_skills()        → skills_extracted missing < 50 rows
[ ] cmd_fix_title_normalization() → distinct title_normalized < 60, verbatim = 0
[ ] cmd_qa_check()               → CRITICAL = 0, WARNING < 20
[ ] Spot-check 30 random rows    → title, snippet, ai_keywords_hit look reasonable
[ ] Verify date window coverage  → SELECT posted_date distribution (mostly 2025-2026)
[ ] Verify Type B count ≥ 40    → X (AI Team) rows exist across multiple base roles
[ ] Verify companies_200 = 200  → SELECT COUNT(*) FROM companies_200
[ ] Check duplicate keys = 0    → SELECT canonical_job_key, COUNT(*) GROUP BY ... HAVING cnt > 1
[ ] Check URL status spread     → mostly 200, minimal 404/410
```

---

## PART 4 — ON THE 4 PROPOSED IMPROVEMENTS

| Improvement | Recommendation | Reason |
|---|---|---|
| 1. QA framework upgrade (dataclass/decorator registry) | **SKIP** | Current hardcoded approach in `qa.py` is readable and works. Refactor adds complexity with no new capability. |
| 2. Pre-ingest validation `validate_pre_ingest()` | **DO IT** (Step 4.1 above) | Prevents dirty data entering gold table. Simple to add, high value. |
| 3. Integration tests | **DO IT** (partial) | Add `TestTitleNormalization` (Step 4.2). Full pipeline integration test is overkill for now. |
| 4. Test coverage gaps (companies.py, enrichment.py, export.py) | **DEFER** | Core pipeline is solid. Add tests here only if you hit bugs in those modules. |

---

## PART 5 — EXECUTION ORDER (coding session)

```
1. cmd_fix_data_quality()                    # ~2 min
2. cmd_backfill_skills()                     # ~5 min (joins raw_postings)
3. cmd_fix_title_normalization()             # ~3 min
4. Run validation queries (Phase 2)          # manual check
5. Fix any remaining verbatim titles         # edit titles.py TITLE_SEGMENTS_EXTENDED
6. Re-run cmd_fix_title_normalization()      # if step 5 needed
7. Add posted_date fallback (Step 12)        # edit fixers.py
8. Add description backfill (Step 13)        # edit fixers.py
9. cmd_fix_data_quality()                    # re-run to apply steps 12-13
10. Add validate_pre_ingest() to ingest.py   # edit
11. Add TestTitleNormalization to tests      # edit
12. Add title_not_normalized QA rule         # edit qa.py
13. cmd_qa_check()                           # final: expect CRITICAL=0, WARNING<20
14. Run full checklist (Phase 5)             # sign-off
```

---

## SUCCESS CRITERIA

| Metric | Current | Target |
|---|---|---|
| Total active rows | 2,976 | 2,976+ |
| QA CRITICAL violations | 0 | 0 |
| QA WARNING violations | 204 | < 20 |
| Distinct title_normalized | 1,576 | < 60 |
| Verbatim title copies | 2,214 | 0 |
| Type B (AI Team) rows | 1 | 40+ |
| Missing skills | 885 | < 50 |
| HTML in snippets | 161 | 0 |
| Pre-ingest validation | not implemented | implemented |
| Title normalization QA rule | not implemented | implemented |

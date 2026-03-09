# Change Plan: title_normalized AI Signal Fixes
**Date:** 2026-03-09  
**DB:** job_postings_gold.db  
**Total rows affected:** ~170  
**Columns modified:** has_ai_in_title, title_ai_terms, ai_role_signature, ai_signal_types, title_normalized

---

## Execution Order

Steps must run in sequence — each depends on the previous.

---

## Step 1 — Fix `has_ai_in_title` detection (12 rows)

**Why first:** Downstream steps depend on `has_ai_in_title` being correct.

**Problem:** Rows with "Artificial General Intelligence" in raw title have `has_ai_in_title = 0`.  
Regex only matches short acronym "AI", not the full phrase.

**Rule:**  
Set `has_ai_in_title = 1` where raw `title` contains any of:
- `"Artificial General Intelligence"` (case-insensitive)
- `"AGI"` (case-sensitive, word boundary)

Also add the matched term to `title_ai_terms`.

**Affected filter:**
```
WHERE title LIKE '%Artificial General Intelligence%'
   OR title REGEXP '\bAGI\b'
```

**Before → After:**
| title | has_ai_in_title | title_ai_terms |
|---|---|---|
| Applied Scientist (Contract), Artificial General Intelligence | 0 | (empty) |
| ↓ | ↓ | ↓ |
| Applied Scientist (Contract), Artificial General Intelligence | **1** | **Artificial General Intelligence** |

**Validation:** Count of rows with `has_ai_in_title = 1` should increase by 12.

---

## Step 2 — Fix `ai_role_signature` for AGI rows (12 rows)

**Problem:** The 12 newly corrected AGI rows have wrong or null `ai_role_signature`.  
"Artificial General Intelligence" is team context (the person is an Applied Scientist ON the AGI team), not an AI identity role.

**Rule:**  
For all rows where `title LIKE '%Artificial General Intelligence%'`:
```
ai_role_signature = 'ai_team_or_platform_in_title'
```

**Validation:** No rows should remain with `ai_role_signature` = null or wrong value where title contains "Artificial General Intelligence".

---

## Step 3 — Recompute `ai_signal_types` (37 rows)

**Problem:** 37 rows have `has_ai_in_title = 1` but `ai_signal_types` doesn't contain `"title"`.  
The field is inconsistent — it was written from a different code path than `has_ai_in_title`.

**Rule:**  
Recompute `ai_signal_types` from scratch for ALL rows as a pipe-separated list:
- Include `"title"` if `has_ai_in_title = 1`
- Include `"description"` if AI terms found in `description_snippet` or `description_full`
- Include `"scope"` if AI terms found in `scope` field

Build the list from source, overwrite the column entirely.

**Validation:** Zero rows where `has_ai_in_title = 1` AND `ai_signal_types` doesn't contain `"title"`.

---

## Step 4 — Normalize Type A rows: AI identity → `"AI [function]"` (9 rows)

**Filter:**
```
WHERE ai_role_signature = 'ai_in_title'
```

**Rule:**  
Reduce raw title to `"AI [base function]"`. Strip everything else:
- Strip seniority: Senior, Sr., Lead, Principal, Staff, Junior, Associate
- Strip team specs: Red Team, Blue Team, Applied, Agentic, Operations, Research (when not the function)
- Strip employment type: (Contract), (Temp), (Part-time)
- Strip level suffixes: I, II, III, 1, 2, 3
- Determine base function from what remains

**Explicit mapping (covers all 9 known rows):**

| Raw `title` | `title_normalized` |
|---|---|
| `AI Analyst` | `AI Analyst` |
| `AI Analyst, [anything]` | `AI Analyst` |
| `AI Red Team Analyst` | `AI Analyst` |
| `Senior AI Red Team Analyst` | `AI Analyst` |
| `Applied AI Analyst` | `AI Analyst` |
| `Agentic AI Researcher` | `AI Researcher` |

**Base function lookup:**  
If raw title (after stripping) contains:
- `Analyst` → `AI Analyst`
- `Researcher` or `Research Scientist` → `AI Researcher`
- `Engineer` → `AI Engineer`
- `Scientist` (without "Research") → `AI Scientist`
- `Strategist` → `AI Strategist`

**Validation:**  
All 9 rows have `title_normalized` starting with `"AI "`.  
No row has `title_normalized` containing "Red Team", "Applied", "Agentic", "Senior", "Lead".

---

## Step 5 — Normalize Type B rows: AI team → `"[Role] (AI Team)"` (58 rows)

**Filter:**
```
WHERE ai_role_signature = 'ai_team_or_platform_in_title'
```
(This includes the original 46 rows + the 12 AGI rows fixed in Steps 1-2.)

**Rule:**  
Two sub-steps per row:

**Sub-step A — Extract base role** (strip AI team context from raw title):

Remove these patterns from raw title (in this order):
```
, Artificial General Intelligence
(Contract), Artificial General Intelligence   → also remove "(Contract)"
, AGI
- Agentic AI
, Agentic AI
, GenAI
, Generative AI
, Generative Intelligence
, LLM [anything to end]
- AI [anything to end]
, AI [anything to end]
, Ads & GenAI [anything to end]
, AI Products
 - AI
, AI
```

What remains = base role string.

**Sub-step B — Normalize base role** (apply standard normalization to what remains):
- Strip seniority: Senior, Sr., Lead, Principal, Staff, Junior, Associate
- Strip employment type: (Contract), (Temp), (Part-time)
- Strip level suffixes: I, II, III

Then append `" (AI Team)"`.

**Explicit mapping (covers all known distinct raw titles in this group):**

| Raw `title` | `title_normalized` |
|---|---|
| `Data Scientist, AGI` | `Data Scientist (AI Team)` |
| `Applied Scientist (Contract), Artificial General Intelligence` | `Applied Scientist (AI Team)` |
| `Lead Data Scientist - Agentic AI` | `Data Scientist (AI Team)` |
| `Senior Data Scientist, GenAI` | `Data Scientist (AI Team)` |
| `Research Scientist, GenAI` | `Research Scientist (AI Team)` |
| `Senior Research Scientist, GenAI` | `Research Scientist (AI Team)` |
| `Business Intelligence Analyst, Ads & GenAI Insights` | `Business Intelligence Analyst (AI Team)` |
| `Senior Business Analyst - AI` | `Business Analyst (AI Team)` |
| `Data Analyst - AI Products` | `Data Analyst (AI Team)` |
| `Business Analyst, AI Platform` | `Business Analyst (AI Team)` |
| `Applied Scientist, LLM Evaluation` | `Applied Scientist (AI Team)` |

> **Note:** Run a query first to get the full list of distinct raw titles in this group. The logic above handles all known cases but verify against the live data.

**Validation:**  
All 58 rows have `title_normalized` ending with `" (AI Team)"`.  
No row ends with `" (AI Team)"` that isn't in `ai_role_signature = 'ai_team_or_platform_in_title'`.  
Count distinct `title_normalized` values in this group — should be a small set (8-12 distinct values max).

---

## Step 6 — Fix remaining verbatim copies (non-AI, ~30 rows)

**Filter:**
```
WHERE title_normalized = title
  AND ai_role_signature NOT IN ('ai_in_title', 'ai_team_or_platform_in_title')
```

After Steps 4 and 5, most verbatim copies in the AI group are fixed. The remaining verbatim copies are standard roles that were never normalized.

**Rule:**  
Apply standard normalization:
1. Strip seniority: Senior, Sr., Lead, Principal, Staff, Junior, Associate
2. Strip employment type: (Contract), (Temp), (Part-time), (Remote)
3. Strip level suffixes: I, II, III, 1, 2, 3
4. Strip department context: `, [Team Name]` suffixes that aren't AI-related
5. Map to nearest canonical role name (use existing taxonomy from current distinct `title_normalized` values as reference)

**Validation:**  
Zero rows where `title_normalized = title` (exact match) remain.  
Exception: single-word titles where raw = canonical are acceptable (e.g. raw "Analyst" → normalized "Analyst").

---

## Step 7 — Remove out-of-scope roles

**Filter (review these, confirm before deleting):**
```
WHERE title_normalized IN (
  'HR Analyst', 'Human Resources Analyst', 'People Analytics Analyst',
  'Finance Analyst', 'Financial Analyst',
  'ERP Analyst', 'SAP Analyst'
)
```

Or by `role_cluster` — check for non-data/non-AI clusters and remove.

**Action:** DELETE rows or flag with `is_excluded = 1` (safer — allows recovery).  
Recommended: flag first, delete after manual review.

**Validation:** Run `SELECT DISTINCT title_normalized, role_cluster` and confirm no out-of-scope roles remain.

---

## Final Validation Checklist

Run these checks after all steps complete:

| Check | Expected result |
|---|---|
| Rows with `has_ai_in_title=1` where `ai_signal_types` doesn't contain "title" | 0 |
| Rows with `title` containing "Artificial General Intelligence" and `has_ai_in_title=0` | 0 |
| Rows with `ai_role_signature='ai_in_title'` and `title_normalized` NOT starting with "AI " | 0 |
| Rows with `ai_role_signature='ai_team_or_platform_in_title'` and `title_normalized` NOT ending with "(AI Team)" | 0 |
| Rows where `title_normalized = title` (verbatim copy) | 0 (or near-0) |
| Distinct `title_normalized` values total | Target: 40-55 distinct values |
| Distinct `title_normalized` values ending with "(AI Team)" | Target: 8-12 |
| Rows where `title_normalized` contains "Red Team", "Applied AI", "Agentic AI" | 0 |

---

## Summary Table

| Step | What | Rows | Columns |
|---|---|---|---|
| 1 | Fix has_ai_in_title for AGI/Artificial General Intelligence | 12 | has_ai_in_title, title_ai_terms |
| 2 | Fix ai_role_signature for AGI rows | 12 | ai_role_signature |
| 3 | Recompute ai_signal_types from source | 37 | ai_signal_types |
| 4 | Normalize Type A AI identity → "AI [function]" | 9 | title_normalized |
| 5 | Normalize Type B AI team → "[Role] (AI Team)" | 58 | title_normalized |
| 6 | Fix remaining verbatim copies | ~30 | title_normalized |
| 7 | Remove/flag out-of-scope roles | TBD | is_excluded or DELETE |

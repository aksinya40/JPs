# AI Analyst Roles — US Job Market Research 2026

## 1. Executive Summary

- **1324 active US job postings** across 123 companies
- **31.6% salary coverage** with data from ATS APIs and pay transparency disclosures
- **13.6% of postings** have AI/LLM terms directly in the job title
- Top role clusters: Data Scientist, Applied Scientist, Operations Analyst, Data/Business Analyst, Analytics Engineer
- Sources: Greenhouse(1134), Amazon Jobs(57), Ashby(39), Lever(37), LinkedIn(17)


## 2. Dataset Overview

| Metric | Value |
|--------|-------|
| Total gold rows | 1377 |
| Active US rows | 1324 |
| Excluded (non-US) | 0 |
| Closed rows | 1 |
| With salary | 31.6% |
| AI in title | 13.6% |
| Date uncertain | 18.4% |
| Critical violations | 0 |
| Warning violations | 213 |


## 3. Role Family Landscape

| Role Cluster | Count |
|-------------|-------|
| Data Scientist | 198 |
| Applied Scientist | 52 |
| Operations Analyst | 17 |
| Data/Business Analyst | 16 |
| Analytics Engineer | 15 |
| Data Analyst | 12 |
| Product Analyst | 8 |
| Senior Data Scientist | 7 |
| Strategic Account Executive, Auth0 | 5 |
| Staff Product Designer | 5 |

## 4. AI/LLM Signal Analysis

| AI Role Signature | Count |
|------------------|-------|
| ai_in_description_only | 922 |
| agentic_in_scope | 130 |
| ai_team_or_platform_in_title | 106 |
| llm_or_genai_in_scope | 89 |
| ai_in_title | 66 |
| ai_team_or_platform_in_scope | 8 |
| emerging_ai_named_role | 3 |

## 5. Compensation Benchmarks

See dashboard charts for interactive salary range analysis by cluster and tier.


## 6. Work Model Distribution

| Work Mode | Count |
|-----------|-------|
| On-site | 949 |
| Remote | 324 |
| Hybrid | 46 |
| Unknown | 5 |

## 7. Top Employers & Tier Analysis

| Company | Postings |
|---------|----------|
| Brex | 168 |
| Okta | 149 |
| Coinbase | 98 |
| Intercom | 75 |
| Amazon | 58 |
| Roblox | 54 |
| Hightouch | 43 |
| Recursion Pharmaceuticals | 34 |
| Grammarly | 34 |
| Reddit | 33 |
| Airbnb | 30 |
| SoFi | 28 |
| Amplitude | 28 |
| Arize AI | 25 |
| Samsara | 23 |
| Datadog | 23 |
| Lyft | 21 |
| Fireworks AI | 19 |
| Mistral AI | 18 |
| Dropbox | 18 |

## 8. Skills Landscape

See dashboard for interactive top-25 skills chart.


## 9. Emerging AI Title Patterns

See AI Role Signature analysis in Section 4 for emerging title patterns.


## 10. Methodology & Coverage Gaps

| Enrich Status | Count |
|--------------|-------|
| api_enriched | 169 |
| enriched | 1026 |
| failed | 112 |
| pending | 70 |

**Pipeline**: Two-source merge (Claude DB + Codex DB) → dedup → ATS API enrichment → JSON-LD → salary regex → QA gates → approval
**Date window**: 2025-07-01 to 2026-03-31
**Company scope**: 200 US big-tech & AI companies

"""
AI Analyst Jobs — Shared Constants
====================================
All data constants, compiled regex patterns, and lookup tables used
across the pipeline. No runtime logic — only declarations.
"""
from __future__ import annotations

import re

# ─── AI Keywords (word-boundary regex, case-insensitive) ─────────────────────
AI_KEYWORDS: list[str] = [
    r'\bllm\b', r'\blarge language model\b', r'\bgenerative ai\b', r'\bgenai\b',
    r'\bgen ai\b', r'\bagentic\b', r'\bai agent\b', r'\bai agents\b',
    r'\bchatgpt\b', r'\bclaude\b', r'\bgemini\b', r'\bgpt-4\b', r'\bgpt-5\b',
    r'\bfoundation model\b', r'\brag\b', r'\bretrieval augmented\b',
    r'\bprompt engineering\b', r'\bvector database\b', r'\bembedding\b',
    r'\btext-to-sql\b', r'\bai evaluation\b', r'\bai adoption\b',
    r'\bai metrics\b', r'\bai product\b', r'\bai/ml\b',
    r'\bmachine learning\b', r'\bnatural language\b', r'\bnlp\b',
    r'\bcopilot\b', r'\bai assistant\b', r'\bai workflow\b',
    r'\bintelligent automation\b', r'\bai-powered\b', r'\bai-augmented\b',
    r'\bmultimodal\b', r'\brlhf\b', r'\bfine-tuning\b', r'\bfine tuning\b',
    r'\bai safety\b', r'\bresponsible ai\b',
]

# Title AI terms: regex pattern → human-readable label
TITLE_AI_TERMS: list[tuple[str, str]] = [
    (r'\bartificial general intelligence\b', 'Artificial General Intelligence'),
    (r'\bAGI\b', 'AGI'),
    (r'\bgenerative ai\b', 'generative AI'), (r'\bgenai\b', 'GenAI'),
    (r'\bgen ai\b', 'Gen AI'), (r'\blarge language model\b', 'large language model'),
    (r'\bfoundation model\b', 'foundation model'), (r'\bresponsible ai\b', 'responsible AI'),
    (r'\bai ethics\b', 'AI ethics'), (r'\bai safety\b', 'AI safety'),
    (r'\bai alignment\b', 'AI alignment'), (r'\bconversational ai\b', 'conversational AI'),
    (r'\bdecision intelligence\b', 'decision intelligence'), (r'\bmultimodal\b', 'multimodal'),
    (r'\bai/ml\b', 'AI/ML'), (r'\bai-ml\b', 'AI-ML'), (r'\bagentic\b', 'agentic'),
    (r'\bagents\b', 'agents'), (r'\bagent\b', 'agent'),
    (r'\bchatgpt\b', 'ChatGPT'), (r'\bgpt\b', 'GPT'), (r'\bgemini\b', 'Gemini'),
    (r'\bembedding\b', 'embedding'), (r'\bvector\b', 'vector'), (r'\bprompt\b', 'prompt'),
    (r'\bfine.tun', 'fine-tuning'), (r'\brlhf\b', 'RLHF'),
    (r'\bcopilot\b', 'copilot'), (r'\bintelligent\b', 'intelligent'),
    (r'\bai\b', 'AI'), (r'\bllms?\b', 'LLM'), (r'\bnlp\b', 'NLP'),
    (r'\bml\b', 'ML'), (r'\brag\b', 'RAG'),
]

# ─── Role Clusters ───────────────────────────────────────────────────────────
ROLE_CLUSTERS_INCLUDED = {
    'Product Analyst', 'Data Analyst', 'Analytics Analyst',
    'Data Scientist', 'Applied Data Scientist', 'Product Data Scientist',
    'Business Data Scientist', 'Growth Data Scientist', 'Decision Data Scientist',
    'Experimentation Scientist', 'A/B Testing Scientist',
    'Growth Analyst', 'Revenue Analyst', 'Marketing Analyst',
    'Lifecycle Analyst', 'GTM Analyst', 'Monetization Analyst', 'Pricing Analyst',
    'Analytics Engineer', 'Decision Scientist', 'Quantitative Analyst',
    'Operations Analyst',
    'AI Analyst', 'LLM Analyst', 'Agentic Analytics Lead',
    'Generative AI Analyst', 'Decision Intelligence Analyst',
    'AI Product Analyst', 'AI/ML Insights Analyst', 'AI Evaluation Analyst',
    'AI Trust Analyst', 'Data Scientist Strategic Intelligence',
    'Quantitative Intelligence Analyst',
    'Business Operations Analyst (AI & Automation)',
}

ROLE_EXCLUSION_PATTERNS: list[str] = [
    r'\bBI Engineer\b', r'\bML Engineer\b', r'\bMLOps Engineer\b',
    r'\bLLM Engineer\b', r'\bAI Platform Engineer\b',
    r'\bSoftware Engineer\b', r'\bInfrastructure Engineer\b',
    r'\bData Platform Engineer\b', r'\bData Engineer\b',
    r'\bProduct Manager\b', r'\bTPM\b', r'\bAPM\b', r'\bProgram Manager\b',
    r'\bResearch Scientist\b', r'\bDevOps\b',
    r'\bSecurity Engineer\b', r'\bSecurity Architect\b',
    r'\bCybersecurity Analyst\b', r'\bInfoSec\b',
    r'\bFinance Analyst\b',
]

# ─── Platform Mapping ────────────────────────────────────────────────────────
PLATFORM_CANONICAL = {
    'greenhouse': 'Greenhouse', 'greenhouse.io': 'Greenhouse',
    'lever': 'Lever', 'lever.co': 'Lever',
    'ashby': 'Ashby', 'ashbyhq': 'Ashby', 'ashbyhq.com': 'Ashby',
    'workday': 'Workday', 'myworkdayjobs': 'Workday',
    'smartrecruiters': 'SmartRecruiters',
    'amazon': 'Amazon Jobs', 'amazon.jobs': 'Amazon Jobs',
    'linkedin': 'LinkedIn', 'linkedin.com': 'LinkedIn',
    'google': 'Google Careers', 'google careers': 'Google Careers',
    'meta': 'Meta Careers', 'meta careers': 'Meta Careers',
    'apple': 'Apple Jobs', 'apple jobs': 'Apple Jobs',
    'netflix': 'Netflix', 'snap': 'Snap',
    'workable': 'Workable', 'bamboohr': 'BambooHR', 'rippling': 'Rippling',
}

# ─── Salary Patterns ─────────────────────────────────────────────────────────
SALARY_PATTERNS = [
    # "$150,000 - $200,000" or "$150k - $200k"
    re.compile(
        r'\$\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?\s*[-–—to]+\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?'
        r'(?:\s*(?:per\s+)?(year|annual|annually|hour|hourly|month|monthly))?',
        re.IGNORECASE
    ),
    # "USD 150,000 to 200,000"
    re.compile(
        r'(?:USD|US\$)\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?\s*(?:to|-|–|—)\s*(?:USD|US\$)?\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?'
        r'(?:\s*(?:per\s+)?(year|annual|annually|hour|hourly|month|monthly))?',
        re.IGNORECASE
    ),
    # "salary range: $X to $Y" / "compensation: $X - $Y"
    re.compile(
        r'(?:salary|compensation|pay)\s*(?:range)?[:\s]*\$\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?\s*[-–—to]+\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?',
        re.IGNORECASE
    ),
]

NON_USD_PATTERNS = re.compile(r'(?:CAD|C\$|CA\$|GBP|£|EUR|€)', re.IGNORECASE)

# ─── Skills Patterns ─────────────────────────────────────────────────────────
SKILL_PATTERNS: list[tuple[str, str]] = [
    ('Python', r'\bpython\b'), ('SQL', r'\bsql\b'),
    ('R', r'(?<![A-Za-z])\bR\b(?!\s*&|\w)'),
    ('Tableau', r'\btableau\b'), ('Looker', r'\blooker\b'),
    ('Power BI', r'\bpower\s*bi\b'), ('Excel', r'\bexcel\b'),
    ('dbt', r'\bdbt\b'), ('Spark', r'\bspark\b'), ('Airflow', r'\bairflow\b'),
    ('BigQuery', r'\bbigquery\b'), ('Snowflake', r'\bsnowflake\b'),
    ('Redshift', r'\bredshift\b'), ('Databricks', r'\bdatabricks\b'),
    ('Pandas', r'\bpandas\b'), ('NumPy', r'\bnumpy\b'),
    ('Scikit-learn', r'\bscikit'), ('TensorFlow', r'\btensorflow\b'),
    ('PyTorch', r'\bpytorch\b'), ('Keras', r'\bkeras\b'),
    ('Jupyter', r'\bjupyter\b'), ('Git', r'\bgit\b'),
    ('AWS', r'\baws\b'), ('GCP', r'\bgcp\b'), ('Azure', r'\bazure\b'),
    ('Docker', r'\bdocker\b'), ('Kubernetes', r'\bkubernetes\b'),
    ('Kafka', r'\bkafka\b'), ('Hadoop', r'\bhadoop\b'),
    ('Hive', r'\bhive\b'), ('Presto', r'\bpresto\b'),
    ('Mixpanel', r'\bmixpanel\b'), ('Amplitude', r'\bamplitude\b'),
    ('Segment', r'\bsegment\b'), ('Fivetran', r'\bfivetran\b'),
    ('LangChain', r'\blangchain\b'), ('LlamaIndex', r'\bllamaindex\b'),
    ('Hugging Face', r'\bhugging\s*face\b'), ('OpenAI API', r'\bopenai\b'),
    ('Statsmodels', r'\bstatsmodels\b'), ('SciPy', r'\bscipy\b'),
    ('A/B Testing', r'\ba/?b\s*test'), ('Causal Inference', r'\bcausal\s*inference\b'),
    ('Bayesian', r'\bbayesian\b'), ('NLP', r'\bnlp\b'),
    ('LLM', r'\bllm\b'), ('RAG', r'\brag\b'),
]

# ─── Title Segments ──────────────────────────────────────────────────────────
TITLE_SEGMENTS = {
    r'\bapplied scientist\b': 'Applied Scientist',
    r'\bstaff data scientist\b': 'Staff Data Scientist',
    r'\bsenior data scientist\b|sr\.?\s+data scientist': 'Senior Data Scientist',
    r'\bprincipal data scientist\b': 'Principal Data Scientist',
    r'\blead data scientist\b': 'Lead Data Scientist',
    r'\bdata scientist\b': 'Data Scientist',
    r'\bdata science manager\b|manager.*data science': 'Data Science Manager',
    r'\bdirector.*data|data.*director\b': 'Director, Data',
    r'\banalytics engineer\b': 'Analytics Engineer',
    r'\bsenior data analyst\b|sr\.?\s+data analyst': 'Senior Data Analyst',
    r'\bdata analyst\b': 'Data Analyst',
    r'\bproduct analyst\b': 'Product Analyst',
    r'\bbusiness analyst\b': 'Business Analyst',
    r'\boperations analyst\b|ops analyst': 'Operations Analyst',
    r'\bgrowth analyst\b': 'Growth Analyst',
    r'\bmarketing analyst\b': 'Marketing Analyst',
    r'\bquantitative analyst\b|quant analyst': 'Quantitative Analyst',
    r'\bdecision scientist\b': 'Decision Scientist',
    r'\bresearch scientist\b': 'Research Scientist',
    r'\bcompetitive intelligence analyst\b': 'Competitive Intelligence Analyst',
}

# ─── URL Company Extraction ──────────────────────────────────────────────────
ATS_SLUG_PATTERNS = [
    (r'boards\.greenhouse\.io/([^/]+)/', lambda m: m.group(1).replace('-', ' ').title()),
    (r'jobs\.lever\.co/([^/]+)/', lambda m: m.group(1).replace('-', ' ').title()),
    (r'jobs\.ashbyhq\.com/([^/]+)/', lambda m: m.group(1).replace('-', ' ').title()),
    (r'jobs\.([^.]+)\.com/', lambda m: m.group(1).replace('-', ' ').title()),
]

BLOCKED_DOMAINS = {
    'builtin.com', 'builtinnyc.com', 'builtinsf.com', 'builtinchicago.com',
    'builtinaustin.com', 'builtinboston.com', 'builtincolorado.com',
    'builtinla.com', 'builtinseattle.com',
    'theladders.com', 'themuse.com', 'towardsai.net',
    'wallstreetcareers.com', 'datasciencessjobs.com', 'technyjobs.com',
    'wellfound.com', 'angel.co',
}

# ─── Work Mode Detection ─────────────────────────────────────────────────────
REMOTE_PATTERNS: list[str] = [r'\bremote\b', r'\bwork from home\b', r'\bwfh\b', r'\bfully remote\b']
HYBRID_PATTERNS: list[str] = [r'\bhybrid\b', r'\bflexible\b', r'\b\d+\s*days.*office\b']

# ─── Applied Scientist Filtering ─────────────────────────────────────────────
APPLIED_SCIENTIST_KEEP_KEYWORDS: list[str] = [
    'analytics', 'measurement', 'insights', 'ads science',
    'experimentation', 'causal', 'decision', 'ranking',
    'recommendation', 'personalization', 'search relevance',
]
APPLIED_SCIENTIST_REMOVE_KEYWORDS: list[str] = [
    'llm agent', 'code agent', 'foundation model', 'pretraining',
    'robotics', 'computer vision', 'speech', 'autonomous',
    'systems', 'infrastructure', 'compiler',
]

# ─── Location Constants ──────────────────────────────────────────────────────
US_STATES = {
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga',
    'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md',
    'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj',
    'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc',
    'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy', 'dc',
}

US_STATE_NAMES = {
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado',
    'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho',
    'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana',
    'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota',
    'mississippi', 'missouri', 'montana', 'nebraska', 'nevada',
    'new hampshire', 'new jersey', 'new mexico', 'new york',
    'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon',
    'pennsylvania', 'rhode island', 'south carolina', 'south dakota',
    'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington',
    'west virginia', 'wisconsin', 'wyoming', 'district of columbia',
}

US_CITIES = {
    'new york', 'los angeles', 'chicago', 'houston', 'phoenix', 'san antonio',
    'san diego', 'dallas', 'san jose', 'austin', 'san francisco', 'seattle',
    'denver', 'boston', 'nashville', 'portland', 'las vegas', 'atlanta',
    'miami', 'minneapolis', 'raleigh', 'charlotte', 'pittsburgh',
    'salt lake city', 'washington', 'philadelphia', 'detroit', 'columbus',
    'indianapolis', 'memphis', 'milwaukee', 'baltimore', 'tampa',
    'st. louis', 'sacramento', 'kansas city', 'cincinnati', 'cleveland',
    'orlando', 'newark', 'palo alto', 'mountain view', 'menlo park',
    'sunnyvale', 'cupertino', 'redmond', 'bellevue', 'cambridge',
    'boulder', 'ann arbor', 'santa clara', 'irvine', 'plano',
}

NON_US_MARKERS = {
    # Cities
    'london', 'toronto', 'vancouver', 'berlin', 'munich', 'paris',
    'amsterdam', 'dublin', 'bangalore', 'hyderabad', 'singapore',
    'sydney', 'melbourne', 'tokyo', 'tel aviv', 'zurich', 'geneva',
    'stockholm', 'copenhagen', 'manila', 'calgary', 'montreal',
    'mumbai', 'pune', 'chennai', 'delhi', 'noida', 'gurgaon',
    'sao paulo', 'buenos aires', 'bogota', 'lima', 'santiago',
    # Countries
    'uk', 'united kingdom', 'canada', 'germany', 'france', 'india',
    'australia', 'japan', 'israel', 'ireland', 'netherlands',
    'switzerland', 'sweden', 'denmark', 'brazil', 'mexico', 'china',
    'south korea', 'poland', 'spain', 'italy', 'austria', 'belgium',
    'czech republic', 'romania', 'portugal', 'finland', 'norway',
    'new zealand', 'philippines', 'indonesia', 'malaysia', 'thailand',
    'vietnam', 'taiwan', 'hong kong', 'colombia', 'argentina',
    'chile', 'peru', 'nigeria', 'kenya', 'south africa', 'egypt',
    'turkey', 'saudi arabia', 'uae', 'qatar', 'pakistan', 'bangladesh',
    'sri lanka', 'ukraine', 'hungary', 'greece', 'croatia', 'serbia',
    'bulgaria', 'slovakia', 'lithuania', 'latvia', 'estonia',
    'luxembourg', 'iceland', 'costa rica',
    # Provinces / regions
    'ontario', 'british columbia', 'quebec', 'alberta',
    'banten',  # Indonesia province (LinkedIn format)
}

STATE_ABBR_TO_NAME = {
    'al': 'Alabama', 'ak': 'Alaska', 'az': 'Arizona', 'ar': 'Arkansas',
    'ca': 'California', 'co': 'Colorado', 'ct': 'Connecticut',
    'de': 'Delaware', 'fl': 'Florida', 'ga': 'Georgia', 'hi': 'Hawaii',
    'id': 'Idaho', 'il': 'Illinois', 'in': 'Indiana', 'ia': 'Iowa',
    'ks': 'Kansas', 'ky': 'Kentucky', 'la': 'Louisiana', 'me': 'Maine',
    'md': 'Maryland', 'ma': 'Massachusetts', 'mi': 'Michigan',
    'mn': 'Minnesota', 'ms': 'Mississippi', 'mo': 'Missouri',
    'mt': 'Montana', 'ne': 'Nebraska', 'nv': 'Nevada', 'nh': 'New Hampshire',
    'nj': 'New Jersey', 'nm': 'New Mexico', 'ny': 'New York',
    'nc': 'North Carolina', 'nd': 'North Dakota', 'oh': 'Ohio',
    'ok': 'Oklahoma', 'or': 'Oregon', 'pa': 'Pennsylvania',
    'ri': 'Rhode Island', 'sc': 'South Carolina', 'sd': 'South Dakota',
    'tn': 'Tennessee', 'tx': 'Texas', 'ut': 'Utah', 'vt': 'Vermont',
    'va': 'Virginia', 'wa': 'Washington', 'wv': 'West Virginia',
    'wi': 'Wisconsin', 'wy': 'Wyoming', 'dc': 'District of Columbia',
}

# ─── AI Role Signature Constants ─────────────────────────────────────────────
EMERGING_AI_PATTERNS: list[str] = [
    r'\bai analyst\b', r'\bllm analyst\b', r'\bagentic analytics\b',
    r'\bdecision intelligence analyst\b', r'\bai evaluation analyst\b',
    r'\bgenerative ai analyst\b',
]

AI_TITLE_TERMS: list[str] = [
    r'\bai\b', r'\bllm\b', r'\bagentic\b', r'\bgenai\b',
    r'\bgenerative ai\b', r'\bml\b', r'\bnlp\b',
    r'\bartificial general intelligence\b', r'\bagi\b',
]

LLM_GENAI_TERMS: list[str] = [
    r'\bllm\b', r'\bgenai\b', r'\bgpt\b', r'\bgpt-4\b',
    r'\bfoundation model\b', r'\blarge language model\b',
    r'\bgenerative ai\b',
]

AGENTIC_TERMS: list[str] = [r'\bagentic\b', r'\bai agent\b', r'\bai agents\b']
AI_TEAM_TERMS: list[str] = [r'\bai platform\b', r'\bai team\b', r'\bfoundation ai\b']

# ─── Aggregator domains to exclude ──────────────────────────────────────────
AGGREGATOR_DOMAINS: list[str] = [
    'builtin.com', 'builtinnyc.com', 'builtinsf.com',
    'builtinchicago.com', 'builtinaustin.com', 'builtinboston.com',
    'builtincolorado.com', 'builtinla.com', 'builtinseattle.com',
    'theladders.com', 'themuse.com', 'towardsai.net',
    'wallstreetcareers.com', 'datasciencessjobs.com', 'technyjobs.com',
    'wellfound.com', 'angel.co',
]

"""
AI Analyst Jobs — Company List & Builder
==========================================
Populates companies_200 with the canonical list of 200 US
big-tech / AI companies across 4 tiers.
"""
from pipeline.db import get_db, log


# The 200 companies — canonical list of US big-tech + AI companies
# Format: (company_name, tier, sector, ats_platform, ats_board_slug)
COMPANIES = [
    # Tier 1: FAANG+ Mega-caps
    ("Google", "Tier1", "Big Tech", "Custom", None),
    ("Meta", "Tier1", "Big Tech", "Custom", None),
    ("Amazon", "Tier1", "Big Tech", "Custom", None),
    ("Apple", "Tier1", "Big Tech", "Custom", None),
    ("Microsoft", "Tier1", "Big Tech", "Workday", None),
    ("Netflix", "Tier1", "Big Tech", "Custom", None),
    # Tier 1: AI Leaders
    ("OpenAI", "Tier1", "AI Native", "Ashby", "openai"),
    ("Anthropic", "Tier1", "AI Native", "Greenhouse", "anthropic"),
    ("NVIDIA", "Tier1", "AI/Semiconductor", "Workday", None),
    ("Tesla", "Tier1", "AI/Automotive", "Custom", None),
    # Tier 2: Major Tech
    ("Salesforce", "Tier2", "Enterprise SaaS", "Workday", None),
    ("Adobe", "Tier2", "Software", "Workday", None),
    ("Oracle", "Tier2", "Enterprise", "Custom", None),
    ("IBM", "Tier2", "Enterprise", "Workday", None),
    ("Uber", "Tier2", "Marketplace", "Greenhouse", "uber"),
    ("Lyft", "Tier2", "Marketplace", "Greenhouse", "lyft"),
    ("Airbnb", "Tier2", "Marketplace", "Greenhouse", "airbnb"),
    ("Snap", "Tier2", "Social Media", "Custom", None),
    ("Pinterest", "Tier2", "Social Media", "Greenhouse", "pinterest"),
    ("Reddit", "Tier2", "Social Media", "Greenhouse", "reddit"),
    ("Twitter/X", "Tier2", "Social Media", "Custom", None),
    ("LinkedIn", "Tier2", "Social/Professional", "Custom", None),
    ("Spotify", "Tier2", "Streaming", "Greenhouse", "spotify"),
    ("Block (Square)", "Tier2", "Fintech", "Greenhouse", "block"),
    ("Stripe", "Tier2", "Fintech", "Greenhouse", "stripe"),
    ("PayPal", "Tier2", "Fintech", "Workday", None),
    ("Intuit", "Tier2", "Fintech", "Custom", None),
    ("Coinbase", "Tier2", "Crypto/Fintech", "Greenhouse", "coinbase"),
    ("Robinhood", "Tier2", "Fintech", "Greenhouse", "robinhood"),
    ("Plaid", "Tier2", "Fintech", "Greenhouse", "plaid"),
    ("DoorDash", "Tier2", "Marketplace", "Greenhouse", "doordash"),
    ("Instacart", "Tier2", "Marketplace", "Greenhouse", "instacart"),
    ("Grubhub", "Tier2", "Marketplace", "Greenhouse", None),
    ("Shopify", "Tier2", "E-commerce", "Greenhouse", "shopify"),
    ("Wayfair", "Tier2", "E-commerce", "Greenhouse", "wayfair"),
    ("Etsy", "Tier2", "E-commerce", "Greenhouse", "etsy"),
    ("eBay", "Tier2", "E-commerce", "Custom", None),
    ("Figma", "Tier2", "Design/SaaS", "Greenhouse", "figma"),
    ("Canva", "Tier2", "Design/SaaS", "Greenhouse", "canva"),
    ("Notion", "Tier2", "Productivity", "Greenhouse", "notion"),
    ("Slack (Salesforce)", "Tier2", "Productivity", "Greenhouse", None),
    ("Zoom", "Tier2", "Communication", "Workday", None),
    ("Atlassian", "Tier2", "Software", "Custom", None),
    ("ServiceNow", "Tier2", "Enterprise SaaS", "Workday", None),
    ("Snowflake", "Tier2", "Data Infrastructure", "Greenhouse", "snowflake"),
    ("Databricks", "Tier2", "Data Infrastructure", "Greenhouse", "databricks"),
    ("Palantir", "Tier2", "Data/Analytics", "Greenhouse", "palantir"),
    ("Datadog", "Tier2", "Observability", "Greenhouse", "datadog"),
    ("Splunk (Cisco)", "Tier2", "Observability", "Custom", None),
    ("Twilio", "Tier2", "Communication", "Greenhouse", "twilio"),
    # Tier 3: AI-Native / Growth Stage
    ("Cohere", "Tier3", "AI Native", "Greenhouse", "cohere"),
    ("Mistral AI", "Tier3", "AI Native", "Greenhouse", "mistral"),
    ("Perplexity AI", "Tier3", "AI Native", "Ashby", "perplexity-ai"),
    ("Inflection AI", "Tier3", "AI Native", "Greenhouse", "inflection"),
    ("Adept AI", "Tier3", "AI Native", "Greenhouse", "adept"),
    ("Character.AI", "Tier3", "AI Native", "Greenhouse", "character"),
    ("Stability AI", "Tier3", "AI Native", "Greenhouse", "stability-ai"),
    ("Runway", "Tier3", "AI/Creative", "Greenhouse", "runwayml"),
    ("Hugging Face", "Tier3", "AI Native", "Greenhouse", "huggingface"),
    ("Scale AI", "Tier3", "AI/Data", "Greenhouse", "scaleai"),
    ("Weights & Biases", "Tier3", "AI/MLOps", "Greenhouse", "wandb"),
    ("Anyscale", "Tier3", "AI Infrastructure", "Greenhouse", "anyscale"),
    ("Mosaic ML (Databricks)", "Tier3", "AI Native", "Greenhouse", None),
    ("Jasper AI", "Tier3", "AI/Content", "Greenhouse", "jasper"),
    ("Writer", "Tier3", "AI/Content", "Greenhouse", "writer"),
    ("Copy.ai", "Tier3", "AI/Content", "Greenhouse", None),
    ("Glean", "Tier3", "AI/Enterprise Search", "Greenhouse", "glean"),
    ("Harvey AI", "Tier3", "AI/Legal", "Ashby", "harvey"),
    ("Casetext (Thomson Reuters)", "Tier3", "AI/Legal", "Greenhouse", None),
    ("Moveworks", "Tier3", "AI/Enterprise", "Greenhouse", "moveworks"),
    ("Observe.AI", "Tier3", "AI/Contact Center", "Greenhouse", "observe-ai"),
    ("Ramp", "Tier3", "Fintech/AI", "Greenhouse", "ramp"),
    ("Brex", "Tier3", "Fintech/AI", "Greenhouse", "brex"),
    ("Navan (TripActions)", "Tier3", "Fintech/AI", "Greenhouse", "navan"),
    ("Rippling", "Tier3", "HR Tech/AI", "Rippling", None),
    ("Gusto", "Tier3", "HR Tech", "Greenhouse", "gusto"),
    ("Deel", "Tier3", "HR Tech", "Ashby", "deel"),
    ("Lattice", "Tier3", "HR Tech", "Greenhouse", "lattice"),
    ("Gong", "Tier3", "AI/Sales", "Greenhouse", "gong"),
    ("Clari", "Tier3", "AI/Sales", "Greenhouse", "clari"),
    ("Highspot", "Tier3", "AI/Sales", "Greenhouse", "highspot"),
    ("ZoomInfo", "Tier3", "Data/Sales", "Greenhouse", "zoominfo"),
    ("Amplitude", "Tier3", "Product Analytics", "Greenhouse", "amplitude"),
    ("Mixpanel", "Tier3", "Product Analytics", "Greenhouse", "mixpanel"),
    ("FullStory", "Tier3", "Product Analytics", "Greenhouse", "fullstory"),
    ("Heap (Contentsquare)", "Tier3", "Product Analytics", "Greenhouse", None),
    ("Braze", "Tier3", "Marketing Tech", "Greenhouse", "braze"),
    ("Iterable", "Tier3", "Marketing Tech", "Greenhouse", "iterable"),
    ("Klaviyo", "Tier3", "Marketing Tech", "Greenhouse", "klaviyo"),
    ("HubSpot", "Tier3", "Marketing/CRM", "Greenhouse", "hubspot"),
    ("MongoDB", "Tier3", "Database", "Greenhouse", "mongodb"),
    ("Elastic", "Tier3", "Search/Analytics", "Greenhouse", "elastic"),
    ("Confluent", "Tier3", "Data Streaming", "Greenhouse", "confluent"),
    ("dbt Labs", "Tier3", "Data/Analytics", "Greenhouse", "dbtlabs"),
    ("Fivetran", "Tier3", "Data Integration", "Greenhouse", "fivetran"),
    ("Census", "Tier3", "Data/Reverse ETL", "Greenhouse", "census"),
    ("Hex", "Tier3", "Data/Analytics", "Greenhouse", "hex"),
    ("Mode Analytics", "Tier3", "Data/Analytics", "Greenhouse", None),
    ("ThoughtSpot", "Tier3", "Analytics/AI", "Greenhouse", "thoughtspot"),
    ("Sigma Computing", "Tier3", "Analytics", "Greenhouse", "sigmacomputing"),
    ("Tableau (Salesforce)", "Tier3", "Analytics", "Workday", None),
    ("Looker (Google)", "Tier3", "Analytics", "Custom", None),
    ("Vercel", "Tier3", "Developer Platform", "Ashby", "vercel"),
    ("Supabase", "Tier3", "Developer Platform", "Ashby", "supabase"),
    ("Retool", "Tier3", "Developer Platform", "Greenhouse", "retool"),
    ("Postman", "Tier3", "Developer Platform", "Greenhouse", "postman"),
    # Tier 4: Established Tech / Late-stage
    ("Cisco", "Tier4", "Networking/Enterprise", "Custom", None),
    ("VMware (Broadcom)", "Tier4", "Enterprise", "Workday", None),
    ("Dell Technologies", "Tier4", "Enterprise", "Workday", None),
    ("HP Inc", "Tier4", "Enterprise", "Workday", None),
    ("Intel", "Tier4", "Semiconductor", "Workday", None),
    ("AMD", "Tier4", "Semiconductor", "Workday", None),
    ("Qualcomm", "Tier4", "Semiconductor", "Workday", None),
    ("Broadcom", "Tier4", "Semiconductor", "Workday", None),
    ("Micron", "Tier4", "Semiconductor", "Workday", None),
    ("Applied Materials", "Tier4", "Semiconductor", "Workday", None),
    ("Workday", "Tier4", "Enterprise SaaS", "Workday", None),
    ("SAP America", "Tier4", "Enterprise", "Custom", None),
    ("Palo Alto Networks", "Tier4", "Cybersecurity", "Workday", None),
    ("CrowdStrike", "Tier4", "Cybersecurity", "Workday", None),
    ("Fortinet", "Tier4", "Cybersecurity", "Workday", None),
    ("Okta", "Tier4", "Identity/Security", "Greenhouse", "okta"),
    ("Cloudflare", "Tier4", "Infrastructure", "Greenhouse", "cloudflare"),
    ("Akamai", "Tier4", "Infrastructure", "Workday", None),
    ("Fastly", "Tier4", "Infrastructure", "Greenhouse", "fastly"),
    ("DigitalOcean", "Tier4", "Cloud", "Greenhouse", "digitalocean"),
    ("HashiCorp (IBM)", "Tier4", "Infrastructure", "Greenhouse", None),
    ("Elastic", "Tier4", "Search", "Greenhouse", "elastic"),
    ("New Relic", "Tier4", "Observability", "Custom", None),
    ("PagerDuty", "Tier4", "Operations", "Greenhouse", "pagerduty"),
    ("Dynatrace", "Tier4", "Observability", "SmartRecruiters", None),
    ("Docusign", "Tier4", "Software", "Greenhouse", "docusign"),
    ("Dropbox", "Tier4", "Storage", "Greenhouse", "dropbox"),
    ("Box", "Tier4", "Storage", "Greenhouse", "box"),
    ("Asana", "Tier4", "Productivity", "Greenhouse", "asana"),
    ("Monday.com", "Tier4", "Productivity", "Greenhouse", "mondaycom"),
    ("Smartsheet", "Tier4", "Productivity", "SmartRecruiters", None),
    ("Zendesk", "Tier4", "CX", "Greenhouse", "zendesk"),
    ("Freshworks", "Tier4", "CX", "Greenhouse", "freshworks"),
    ("Sprinklr", "Tier4", "CX", "SmartRecruiters", None),
    ("Toast", "Tier4", "Restaurant Tech", "Greenhouse", "toast"),
    ("Square (Block)", "Tier4", "Fintech", "Greenhouse", None),
    ("Affirm", "Tier4", "Fintech", "Greenhouse", "affirm"),
    ("Marqeta", "Tier4", "Fintech", "Greenhouse", "marqeta"),
    ("SoFi", "Tier4", "Fintech", "Greenhouse", "sofi"),
    ("Chime", "Tier4", "Fintech", "Greenhouse", "chime"),
    ("Bill.com", "Tier4", "Fintech", "Greenhouse", "billcom"),
    ("Carta", "Tier4", "Fintech", "Greenhouse", "carta"),
    ("Wealthsimple", "Tier4", "Fintech", "Greenhouse", "wealthsimple"),
    ("Lemonade", "Tier4", "Insurtech/AI", "Greenhouse", "lemonade"),
    ("Root Insurance", "Tier4", "Insurtech/AI", "Greenhouse", "root-insurance"),
    ("Oscar Health", "Tier4", "Healthtech/AI", "Greenhouse", "oscar-health"),
    ("Tempus AI", "Tier4", "Healthtech/AI", "Greenhouse", "tempus"),
    ("Veeva Systems", "Tier4", "Healthtech", "Workday", None),
    ("Doximity", "Tier4", "Healthtech", "Greenhouse", "doximity"),
    ("Ro", "Tier4", "Healthtech", "Greenhouse", "ro"),
    ("Hims & Hers", "Tier4", "Healthtech", "Greenhouse", "hims"),
    ("Duolingo", "Tier4", "EdTech/AI", "Greenhouse", "duolingo"),
    ("Coursera", "Tier4", "EdTech", "Lever", "coursera"),
    ("Chegg", "Tier4", "EdTech", "Greenhouse", "chegg"),
    ("Quizlet", "Tier4", "EdTech/AI", "Greenhouse", "quizlet"),
    ("GitLab", "Tier4", "Developer Platform", "Greenhouse", "gitlab"),
    ("GitHub (Microsoft)", "Tier4", "Developer Platform", "Greenhouse", "github"),
    ("JFrog", "Tier4", "Developer Platform", "Greenhouse", "jfrog"),
    ("Snyk", "Tier4", "Developer Security", "Greenhouse", "snyk"),
    ("Sentry", "Tier4", "Developer Tools", "Greenhouse", "sentry"),
    ("LaunchDarkly", "Tier4", "Developer Tools", "Greenhouse", "launchdarkly"),
    ("Grafana Labs", "Tier4", "Observability", "Greenhouse", "grafanalabs"),
    ("Cockroach Labs", "Tier4", "Database", "Greenhouse", "cockroach-labs"),
    ("SingleStore", "Tier4", "Database", "Greenhouse", "singlestore"),
    ("Pinecone", "Tier4", "Vector Database/AI", "Ashby", "pinecone"),
    ("Weaviate", "Tier4", "Vector Database/AI", "Greenhouse", "weaviate"),
    ("Qdrant", "Tier4", "Vector Database/AI", "Greenhouse", None),
    ("LangChain", "Tier4", "AI Framework", "Greenhouse", None),
    ("Replit", "Tier4", "AI/Developer", "Ashby", "replit"),
    ("Cursor", "Tier4", "AI/Developer", "Ashby", "anysphere"),
    ("Codeium", "Tier4", "AI/Developer", "Greenhouse", None),
    ("Magic", "Tier4", "AI/Developer", "Greenhouse", None),
    ("Together AI", "Tier4", "AI Infrastructure", "Greenhouse", "togetherai"),
    ("Groq", "Tier4", "AI/Hardware", "Greenhouse", "groq"),
    ("Cerebras", "Tier4", "AI/Hardware", "Greenhouse", "cerebras"),
    ("SambaNova", "Tier4", "AI/Hardware", "Greenhouse", "sambanova"),
    ("d-Matrix", "Tier4", "AI/Hardware", "Greenhouse", None),
    ("Airtable", "Tier4", "No-Code/SaaS", "Greenhouse", "airtable"),
    ("Zapier", "Tier4", "Automation", "Greenhouse", "zapier"),
    ("UiPath", "Tier4", "Automation/AI", "Workday", None),
    ("C3.ai", "Tier4", "Enterprise AI", "Greenhouse", "c3-ai"),
    ("DataRobot", "Tier4", "AI/AutoML", "Greenhouse", "datarobot"),
    ("H2O.ai", "Tier4", "AI/AutoML", "Greenhouse", "h2o-ai"),
    ("Alteryx", "Tier4", "Analytics", "Custom", None),
    ("Sisense", "Tier4", "Analytics", "Greenhouse", "sisense"),
    ("Domo", "Tier4", "Analytics", "Workday", None),
    ("Firecrawl", "Tier4", "AI/Data", "Ashby", "firecrawl"),
    ("Anthropic", "Tier1", "AI Native", "Ashby", "anthropic"),
]


def cmd_build_companies():
    """Populate companies_200 with exactly 200 US big-tech/AI companies."""
    conn = get_db()
    cur = conn.cursor()
    existing = cur.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
    if existing == 200:
        log(f"build_companies: Already have {existing} companies. Done.")
        conn.close()
        return
    if existing > 0 and existing != 200:
        log(f"build_companies: Have {existing} companies, need exactly 200.")
        log("  Will add missing companies to reach 200.")

    # Deduplicate by canonical_name
    seen_canonical = set()
    for c in cur.execute("SELECT canonical_name FROM companies_200").fetchall():
        seen_canonical.add(c[0].lower())

    added = 0
    for name, tier, sector, ats, slug in COMPANIES:
        cn = name.lower().strip()
        if cn in seen_canonical:
            continue
        cur.execute("""
            INSERT INTO companies_200 (company_name, canonical_name, tier, sector,
                                       hq_country, ats_platform, ats_board_slug)
            VALUES (?, ?, ?, ?, 'US', ?, ?)
        """, (name, name, tier, sector, ats, slug))
        seen_canonical.add(cn)
        added += 1

    conn.commit()
    total = cur.execute("SELECT COUNT(*) FROM companies_200").fetchone()[0]
    conn.close()
    log(f"build_companies: Added {added} companies. Total: {total}")
    if total != 200:
        log(f"  WARNING: Expected 200 companies, got {total}. Adjust list manually.")

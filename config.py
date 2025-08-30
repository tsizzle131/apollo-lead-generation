import os
import json
import logging
from dotenv import load_dotenv

load_dotenv()

# Path to UI state file
UI_STATE_FILE = os.path.join(os.path.dirname(__file__), '..', '.app-state.json')

def load_ui_config():
    """Load configuration from UI state file, with fallbacks to .env and defaults"""
    ui_config = {}
    
    try:
        if os.path.exists(UI_STATE_FILE):
            with open(UI_STATE_FILE, 'r') as f:
                ui_state = json.load(f)
                ui_config = ui_state
                logging.info("✅ Loaded configuration from React UI")
    except Exception as e:
        logging.warning(f"Could not load UI config: {e}. Using defaults.")
    
    return ui_config

# Load UI configuration
_ui_config = load_ui_config()

# Organization context
CURRENT_ORGANIZATION_ID = os.getenv('CURRENT_ORGANIZATION_ID')
ORGANIZATION_CONFIG = {}  # Will be populated when prompt is loaded

# API Keys - Read from UI first, then .env, then defaults
def get_api_key(key_name, env_name, default=None):
    """Get API key from UI state, fallback to env, then default"""
    ui_keys = _ui_config.get('apiKeys', {})
    return ui_keys.get(key_name) or os.getenv(env_name) or default

APIFY_API_KEY = get_api_key('apify_api_key', 'APIFY_API_KEY', 'your_apify_api_key_here')
OPENAI_API_KEY = get_api_key('openai_api_key', 'OPENAI_API_KEY')

# Google Sheets Configuration
GOOGLE_SHEETS_ID = os.getenv('GOOGLE_SHEETS_ID', '1uRvJxPWdkJcEfXvZcWwVIm_FNy8fQecWvswH0hEYQSY')
SEARCH_URL_SHEET = os.getenv('SEARCH_URL_SHEET', 'seach url')
LEADS_SHEET = os.getenv('LEADS_SHEET', 'leads')

# Scraping Configuration
MAX_LINKS_PER_SITE = 3
REQUEST_TIMEOUT = 1800  # 30 minutes for Apollo scraping (was 30 seconds)
WEBSITE_TIMEOUT = 7  # 7 seconds for website scraping
MAX_RETRIES = 3
WEBSITE_MAX_RETRIES = 0  # No retries for failed websites (2x speedup)
BATCH_SIZE = 25  # Increased from 10 for 2x speedup

# Database Configuration
DATABASE_BATCH_SIZE = 50  # Increased from 25 for better throughput
DATABASE_TIMEOUT = 60  # 60 seconds timeout for database operations
DATABASE_MAX_RETRIES = 3  # Retry failed database operations

# AI Configuration - Read from UI first, then defaults
def get_ai_setting(key, default):
    """Get AI setting from UI state, fallback to default"""
    ui_settings = _ui_config.get('settings', {})
    return ui_settings.get(key, default)

AI_MODEL_SUMMARY = get_ai_setting('ai_model_summary', 'gpt-4o-mini')
AI_MODEL_ICEBREAKER = get_ai_setting('ai_model_icebreaker', 'gpt-4o')
AI_TEMPERATURE = get_ai_setting('ai_temperature', 0.5)

# Scheduling
SCHEDULE_INTERVAL_MINUTES = 15

# Rate limiting - Read from UI first, then defaults
DELAY_BETWEEN_REQUESTS = 1  # seconds
DELAY_BETWEEN_AI_CALLS = get_ai_setting('delay_between_ai_calls', 5)  # seconds - reduced from 45s for speed

# Concurrency settings for parallel processing
MAX_WEBSITE_WORKERS = 6  # Increased from 3 for faster website scraping
MAX_AI_WORKERS = 10  # Aggressive limit for OpenAI API calls
MAX_CONTACTS_PARALLEL = 10  # Increased from 5 for 2x parallel processing
ENABLE_PARALLEL_PROCESSING = True  # Master switch for parallel processing

# OpenAI Rate Limits (requests per minute)
OPENAI_GPT4_RPM = 10000  # GPT-4o rate limit
OPENAI_GPT4_MINI_RPM = 30000  # GPT-4o-mini rate limit

# Domain throttling
DOMAIN_REQUEST_DELAY = 2.0  # Minimum seconds between requests to same domain
WEBSITE_FAILURE_THRESHOLD = 3  # Mark domain as failed after this many consecutive failures

# Prompts - Read from UI state
def get_prompt(prompt_type, default=""):
    """Get prompt from UI state, fallback to default"""
    ui_prompts = _ui_config.get('prompts', {})
    return ui_prompts.get(prompt_type, default)

SUMMARY_PROMPT = get_prompt('summary', """You're provided a Markdown scrape of a website page. Your task is to provide a two-paragraph abstract of what this page is about.

Return in this JSON format:

{"abstract":"your abstract goes here"}

Rules:
- Your extract should be comprehensive—similar level of detail as an abstract to a published paper.
- Use a straightforward, spartan tone of voice.
- If it's empty, just say "no content".""")

def get_organization_prompt():
    """Get icebreaker prompt from organization's custom prompt or use default"""
    org_id = os.getenv('CURRENT_ORGANIZATION_ID')
    
    if org_id and 'supabase' in _ui_config:
        try:
            import requests
            import json
            
            # Try to fetch organization's custom prompt
            url = _ui_config['supabase']['url']
            key = _ui_config['supabase']['key']
            
            if url and key:
                response = requests.get(
                    f"{url}/rest/v1/organizations?id=eq.{org_id}&select=custom_icebreaker_prompt,product_name,product_description,value_proposition,target_audience,messaging_tone",
                    headers={
                        'apikey': key,
                        'Authorization': f'Bearer {key}'
                    }
                )
                
                if response.ok:
                    orgs = response.json()
                    if orgs and len(orgs) > 0:
                        org = orgs[0]
                        
                        # If organization has a custom prompt, use it
                        if org.get('custom_icebreaker_prompt'):
                            logging.info(f"✅ Using custom icebreaker prompt for organization {org_id}")
                            return org['custom_icebreaker_prompt']
                        
                        # If organization has product config, build dynamic prompt
                        if org.get('product_name'):
                            logging.info(f"🎯 Building dynamic prompt for {org.get('product_name')}")
                            # Store org config globally for AI processor to use
                            global ORGANIZATION_CONFIG
                            ORGANIZATION_CONFIG = org
                            
                            return f"""You're a sales expert writing personalized cold email openers for {org.get('product_name', 'our product')}.

**The Prospect:**
Name: {{first_name}} {{last_name}}
Role: {{headline}}
Company: {{company_name}}
Location: {{location}}

**Their Business Context:**
{{website_summaries}}

**Your Product - {org.get('product_name', 'Our Product')}:**
{org.get('product_description', 'Product/service')}

**Value Proposition:**
{org.get('value_proposition', 'Helps businesses grow')}

**Target Audience:**
{org.get('target_audience', 'Businesses')}

**Your Task:**
Write a 2-3 sentence icebreaker that:
1. References ONE specific, non-obvious detail from their business (avoid generic compliments)
2. Creates a natural bridge to how {org.get('product_name')} addresses a specific pain point or opportunity
3. Uses varied language patterns - avoid overusing: "noticed your", "curious about", "might align", "let's chat"

**Language Variety Guidelines:**
- Opening variations: Start with their name sometimes, a question other times, or an observation
- Connection phrases: Use "addresses", "solves", "helps with", "tackles", "streamlines" instead of always "aligns with"
- Closing variations: Mix between questions, statements, and soft CTAs
- Avoid these overused patterns: "noticed your knack for", "impressed by", "caught my eye"

**Tone:** {org.get('messaging_tone', 'professional')} but always conversational and genuine

**Subject Line Requirements:**
Create a subject line that:
- Is 30-50 characters max
- Creates genuine curiosity without clickbait
- Uses one of these patterns (vary them):
  • Question: "Quick question about [specific thing]"
  • Observation: "[Company] + [specific observation]"
  • Direct: "[Name], about [specific area]"
  • Connection: "[Their thing] → [benefit]"

Return EXACTLY this JSON format:
{{
  "icebreaker": "your personalized message here",
  "subject_line": "your subject line here"
}}"""
                            
        except Exception as e:
            logging.warning(f"Could not fetch organization prompt: {e}")
    
    # Fall back to default prompt from UI config
    return get_prompt('icebreaker', """We just scraped a series of web pages for a business called . Your task is to take their summaries and turn them into catchy, personalized openers for a cold email campaign to imply that the rest of the campaign is personalized.

You'll return your icebreakers in the following JSON format:

{"icebreaker":"Hey {name}. Love {thing}—also doing/like/a fan of {otherThing}. Wanted to run something by you.\\n\\nI hope you'll forgive me, but I creeped you/your site quite a bit, and know that {anotherThing} is important to you guys (or at least I'm assuming this given the focus on {fourthThing}). I put something together a few months ago that I think could help. To make a long story short, it's an outreach system that uses AI to find people and reseache them, and reach out. Costs just a few cents to run, very high converting, and I think it's in line with {someImpliedBeliefTheyHave}"}

Rules:
- Write in a spartan/laconic tone of voice.
- Make sure to use the above format when constructing your icebreakers. We wrote it this way on purpose.
- Shorten the company name wherever possible (say, "XYZ" instead of "XYZ Agency"). More examples: "Love AMS" instead of "Love AMS Professional Services", "Love Mayo" instead of "Love Mayo Inc.", etc.
- Do the same with locations. "San Fran" instead of "San Francisco", "BC" instead of "British Columbia", etc.
- For your variables, focus on small, non-obvious things to paraphrase. The idea is to make people think we *really* dove deep into their website, so don't use something obvious. Do not say cookie-cutter stuff like "Love your website!" or "Love your take on marketing!".""")

# Use dynamic prompt based on organization
ICEBREAKER_PROMPT = get_organization_prompt()

def reload_config():
    """Reload configuration from UI state file"""
    global _ui_config, APIFY_API_KEY, OPENAI_API_KEY, AI_MODEL_SUMMARY, AI_MODEL_ICEBREAKER, AI_TEMPERATURE, DELAY_BETWEEN_AI_CALLS, SUMMARY_PROMPT, ICEBREAKER_PROMPT
    
    _ui_config = load_ui_config()
    
    # Reload all dynamic values
    APIFY_API_KEY = get_api_key('apify_api_key', 'APIFY_API_KEY', 'your_apify_api_key_here')
    OPENAI_API_KEY = get_api_key('openai_api_key', 'OPENAI_API_KEY')
    
    AI_MODEL_SUMMARY = get_ai_setting('ai_model_summary', 'gpt-4o-mini')
    AI_MODEL_ICEBREAKER = get_ai_setting('ai_model_icebreaker', 'gpt-4o')
    AI_TEMPERATURE = get_ai_setting('ai_temperature', 0.5)
    DELAY_BETWEEN_AI_CALLS = get_ai_setting('delay_between_ai_calls', 5)  # Use fast 5s default
    
    SUMMARY_PROMPT = get_prompt('summary', SUMMARY_PROMPT)
    # Get organization-specific or default icebreaker prompt
    ICEBREAKER_PROMPT = get_organization_prompt()
    
    logging.info("🔄 Configuration reloaded from React UI")

# User agents for web scraping
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]
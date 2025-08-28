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
WEBSITE_MAX_RETRIES = 1  # Reduce website retries from 3 to 1 for speed
BATCH_SIZE = 10

# Database Configuration
DATABASE_BATCH_SIZE = 25  # Smaller batches for reliable database insertion
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
MAX_WEBSITE_WORKERS = 3  # Conservative limit for website scraping (avoid IP blocking)
MAX_AI_WORKERS = 10  # Aggressive limit for OpenAI API calls
MAX_CONTACTS_PARALLEL = 5  # Number of contacts to process in parallel
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
                            return f"""You're writing the opening lines of a cold email for {org.get('product_name', 'our product')}.

**The Person:**
Name: {{first_name}} {{last_name}}
Role: {{headline}}
Company: {{company_name}}
Location: {{location}}

**What you learned about their company:**
{{website_summaries}}

**Your Product/Service:**
- Name: {org.get('product_name', 'Our Product')}
- Description: {org.get('product_description', 'Product/service')}
- Value: {org.get('value_proposition', 'Helps businesses grow')}
- Target: {org.get('target_audience', 'Businesses')}

**Your Job:**
Write 2-3 sentences that:
1. Reference ONE specific thing about their business
2. Connect it to how {org.get('product_name', 'our product')} could help
3. Sound human and conversational

**Tone:** {org.get('messaging_tone', 'professional')}

Return your response in JSON format:
{{"icebreaker": "your message"}}"""
                            
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
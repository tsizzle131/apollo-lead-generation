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

ICEBREAKER_PROMPT = get_prompt('icebreaker', """We just scraped a series of web pages for a business called . Your task is to take their summaries and turn them into catchy, personalized openers for a cold email campaign to imply that the rest of the campaign is personalized.

You'll return your icebreakers in the following JSON format:

{"icebreaker":"Hey {name}. Love {thing}—also doing/like/a fan of {otherThing}. Wanted to run something by you.\\n\\nI hope you'll forgive me, but I creeped you/your site quite a bit, and know that {anotherThing} is important to you guys (or at least I'm assuming this given the focus on {fourthThing}). I put something together a few months ago that I think could help. To make a long story short, it's an outreach system that uses AI to find people and reseache them, and reach out. Costs just a few cents to run, very high converting, and I think it's in line with {someImpliedBeliefTheyHave}"}

Rules:
- Write in a spartan/laconic tone of voice.
- Make sure to use the above format when constructing your icebreakers. We wrote it this way on purpose.
- Shorten the company name wherever possible (say, "XYZ" instead of "XYZ Agency"). More examples: "Love AMS" instead of "Love AMS Professional Services", "Love Mayo" instead of "Love Mayo Inc.", etc.
- Do the same with locations. "San Fran" instead of "San Francisco", "BC" instead of "British Columbia", etc.
- For your variables, focus on small, non-obvious things to paraphrase. The idea is to make people think we *really* dove deep into their website, so don't use something obvious. Do not say cookie-cutter stuff like "Love your website!" or "Love your take on marketing!".""")

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
    ICEBREAKER_PROMPT = get_prompt('icebreaker', ICEBREAKER_PROMPT)
    
    logging.info("🔄 Configuration reloaded from React UI")

# User agents for web scraping
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]
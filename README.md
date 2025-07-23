# Apollo Lead Generation System

A high-performance Python lead generation system that automates contact scraping, research, and personalized outreach preparation.

## Features

- **🚀 Apollo.io Integration**: Scrapes high-quality contacts via Apify (2000+ contacts per run)
- **🔍 Website Research**: Automatically researches company websites with intelligent content extraction
- **🤖 AI-Powered Analysis**: GPT-4 generates website summaries and personalized icebreakers
- **💾 Supabase Database**: Robust PostgreSQL storage with automatic deduplication
- **⚡ Optimized Performance**: ~20-40 seconds per contact processing
- **🛡️ Error Handling**: Smart retry logic, rate limit handling, and graceful failures
- **📊 Two-Stage Pipeline**: Raw contact storage → AI-enhanced lead processing

## Setup

### 1. Install Dependencies

```bash
cd lead_generation
pip install -r requirements.txt
```

### 2. Configure API Keys

Copy the example environment file:
```bash
cp .env.example .env
```

Edit `.env` and add your API keys:
```bash
OPENAI_API_KEY=your_openai_api_key_here
APIFY_API_KEY=your_apify_api_key_here
```

### 3. Google Sheets Authentication

You have two options for Google Sheets authentication:

**Option A: Service Account (Recommended)**
1. Create a service account in Google Cloud Console
2. Download the credentials JSON file
3. Set the path in your code or use environment variable

**Option B: Environment Variable**
1. Set `GOOGLE_CREDENTIALS_JSON` environment variable with the entire JSON content

### 4. Configure Google Sheets

Update `config.py` with your Google Sheets information:
- `GOOGLE_SHEETS_ID`: Your spreadsheet ID
- `SEARCH_URL_SHEET`: Sheet name containing search URLs  
- `LEADS_SHEET`: Sheet name for saving results

## Usage

### Test Connections
```bash
python main.py test
```

### Run Once
```bash
python main.py once
```

### Run with Scheduling
```bash
python scheduler.py
```

### Manual Mode
```bash
python main.py
```

## Configuration

Edit `config.py` to customize:

- **Rate Limits**: Delays between requests and AI calls
- **Batch Sizes**: Number of contacts to process at once  
- **AI Models**: GPT model versions for summaries vs icebreakers
- **Scraping Limits**: Max links per website
- **Schedule**: How often to run (default: 15 minutes)

## How It Works

1. **Read Search URLs**: Gets LinkedIn search URLs from Google Sheets
2. **Scrape Contacts**: Uses Apify to extract contact information
3. **Filter Contacts**: Only processes contacts with email AND website
4. **Research Websites**: Scrapes company websites for internal links
5. **Generate Summaries**: Uses GPT-4o-mini to summarize website content
6. **Create Icebreakers**: Uses GPT-4o to generate personalized cold email openers
7. **Save Results**: Writes completed leads back to Google Sheets

## File Structure

```
lead_generation/
├── main.py              # Main orchestrator
├── scheduler.py         # Automated scheduling
├── config.py           # Configuration and settings
├── requirements.txt    # Python dependencies
├── .env.example       # Environment template
├── modules/
│   ├── apify_scraper.py    # Contact data scraping
│   ├── web_scraper.py      # Website research  
│   ├── ai_processor.py     # AI summaries & icebreakers
│   └── sheets_manager.py   # Google Sheets I/O
└── README.md          # This file
```

## Logging

The system creates detailed logs:
- `lead_generation.log`: Main workflow logs
- `scheduler.log`: Scheduler-specific logs

## Troubleshooting

### Common Issues

1. **Google Sheets Authentication**
   - Make sure service account has access to your spreadsheet
   - Check that credentials JSON is valid

2. **API Rate Limits**
   - Adjust delays in `config.py`
   - Monitor API usage in dashboards

3. **Empty Results**
   - Check search URLs format in Google Sheets
   - Verify Apify actor is working
   - Test individual components with debug mode

### Debug Mode

Run with a single contact for testing:
```bash
python main.py test
```

This will prompt for a search URL and process just one contact for debugging.

## Differences from n8n Version

- **Better Error Handling**: Proper retry logic and graceful failures
- **Configurable**: Easy to adjust settings without GUI
- **Logging**: Detailed logs for debugging and monitoring  
- **Modular**: Easy to modify individual components
- **Cost Effective**: No n8n hosting costs
- **Production Ready**: Built for reliability and scaling

## Security

- Never commit API keys to version control
- Use environment variables for sensitive data
- Regularly rotate API keys
- Monitor API usage and costs
#!/Users/tristanwaite/miniconda3/bin/python
"""
Lead Generation System - Python conversion of n8n workflow

This script replicates the functionality of your n8n workflow:
1. Read search URLs from Google Sheets
2. Scrape contact data using Apify
3. Research company websites
4. Generate AI-powered icebreakers
5. Save results back to Google Sheets
"""

import sys
import os
import logging
import time
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add modules directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'modules'))

try:
    from sheets_manager import GoogleSheetsManager
except ImportError:
    GoogleSheetsManager = None
from modules.supabase_manager import SupabaseManager
from modules.apify_scraper import ApifyScraper
from modules.local_business_scraper import LocalBusinessScraper
from modules.web_scraper import WebScraper
from modules.ai_processor import AIProcessor
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lead_generation.log'),
        logging.StreamHandler()
    ]
)

class LeadGenerationOrchestrator:
    def __init__(self, use_supabase=True, use_sheets=False, organization_id=None):
        """Initialize all components with organization context"""
        # Reload config to get latest UI settings
        config.reload_config()
        logging.info("🎛️  Using configuration from React UI control panel")
        
        # Initialize database managers
        self.use_supabase = use_supabase
        self.use_sheets = use_sheets
        self.organization_id = organization_id or config.CURRENT_ORGANIZATION_ID
        
        if use_supabase:
            try:
                # Get audience_id from environment if available (for audience scraping)
                audience_id = os.getenv('AUDIENCE_ID')
                self.supabase_manager = SupabaseManager(organization_id=self.organization_id, audience_id=audience_id)
                if self.organization_id:
                    org_msg = f"✅ Supabase initialized for organization: {self.organization_id}"
                    if audience_id:
                        org_msg += f" with audience: {audience_id}"
                    logging.info(org_msg)
                else:
                    logging.info("✅ Supabase initialized successfully (no organization context)")
            except Exception as e:
                logging.warning(f"Could not initialize Supabase: {e}")
                logging.info("Falling back to legacy mode")
                self.use_supabase = False
                self.supabase_manager = None
        else:
            self.supabase_manager = None
        
        # Legacy Google Sheets support
        if use_sheets and GoogleSheetsManager:
            try:
                self.sheets_manager = GoogleSheetsManager(config.GOOGLE_SHEETS_ID)
            except Exception as e:
                logging.warning(f"Could not initialize Google Sheets: {e}")
                self.use_sheets = False
                self.sheets_manager = None
        else:
            self.sheets_manager = None
            
        # Core processing components
        self.apify_scraper = ApifyScraper()
        self.local_scraper = LocalBusinessScraper()
        self.web_scraper = WebScraper()
        self.ai_processor = AIProcessor()  # Will automatically load latest API key
        
    def run_workflow(self, campaign_id: str = None) -> bool:
        """
        Run the complete lead generation workflow using Supabase two-stage pipeline
        
        Stage 1: Scrape and store ALL raw contact data
        Stage 2: Process qualified contacts into leads with AI icebreakers
        
        Args:
            campaign_id: Optional campaign ID to process specific campaign URLs
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if campaign_id:
                logging.info(f"🎯 Starting campaign workflow for campaign: {campaign_id}")
            else:
                logging.info("🚀 Starting enhanced lead generation workflow with Supabase")
            
            if not self.use_supabase:
                return self._run_legacy_workflow()
            
            # Stage 1: Get search URLs and scrape raw contact data
            if campaign_id:
                logging.info(f"🎯 Starting Campaign Mode for ID: {campaign_id}")
                logging.info(f"🏢 Organization Context: {self.organization_id}")
                
                # Get search URLs for specific campaign
                search_urls = self.supabase_manager.get_campaign_search_urls(campaign_id, status="pending")
                logging.info(f"📊 Found {len(search_urls) if search_urls else 0} pending URLs for campaign")
                
                if search_urls:
                    # Update campaign status to running
                    self.supabase_manager.update_campaign_status(campaign_id, "active")
                    campaign_info = self.supabase_manager.get_campaign_by_id(campaign_id)
                    logging.info(f"📋 Campaign: {campaign_info.get('name', 'Unknown')} - {len(search_urls)} URLs to process")
                    logging.info(f"🚀 STAGE 1: Starting Apollo scraping for campaign")
                else:
                    logging.warning(f"⚠️ No pending URLs found in campaign {campaign_id}")
                    logging.warning(f"Organization ID was: {self.organization_id}")
                    # Mark campaign as completed since there's nothing to process
                    try:
                        self.supabase_manager.update_campaign_status(campaign_id, "completed")
                        logging.info(f"✅ Campaign {campaign_id} marked as completed (no pending URLs)")
                    except Exception as e:
                        logging.error(f"Failed to update campaign status: {e}")
                    return False
            else:
                # Get all pending search URLs (legacy behavior)
                search_urls = self.supabase_manager.get_search_urls(status="pending")
            
            if not search_urls:
                # Create a default search URL for testing
                logging.info("No pending search URLs found, creating test URL")
                test_url = "https://app.apollo.io/#/people?page=1&organizationLocations[]=United%20States&organizationNumEmployeesRanges[]=1%2C10&organizationNumEmployeesRanges[]=51%2C100&organizationNumEmployeesRanges[]=11%2C20&organizationNumEmployeesRanges[]=21%2C50&organizationIndustryTagIds[]=5567cd4773696439b10b0000&organizationIndustryTagIds[]=5567cd4e7369643b70010000&sortByField=%5Bnone%5D&sortAscending=false&personTitles[]=manager&personTitles[]=ceo&personTitles[]=cmo"
                search_data = self.supabase_manager.find_or_create_search_url(test_url, "Auto-generated Apollo test URL")
                if search_data:
                    search_urls = [search_data]
            
            if not search_urls:
                logging.warning("No search URLs available")
                return False
            
            total_raw_contacts = 0
            total_processed_leads = 0
            
            for search_url_data in search_urls:
                search_url_id = search_url_data['id']
                search_url = search_url_data['url']
                
                logging.info(f"🔍 Stage 1: Scraping raw contacts from: {search_url}")
                
                # Update status to running
                self.supabase_manager.update_search_url_status(search_url_id, "running")
                
                try:
                    # Step 1: Scrape ALL contacts and store raw data immediately
                    # Get record count from environment (set by server)
                    record_count = int(os.getenv('RECORD_COUNT', '500'))
                    
                    # Check scraper type from environment or URL pattern
                    scraper_type = os.getenv('SCRAPER_TYPE', 'apollo').lower()
                    
                    # Detect scraper type from URL if not specified
                    if 'apollo.io' in search_url:
                        scraper_type = 'apollo'
                    elif 'local:' in search_url:  # Format: "local:query|location"
                        scraper_type = 'local'
                    
                    if scraper_type == 'local':
                        # Parse local business search parameters
                        # Enhanced formats:
                        # "local:hair salons|Austin, TX" - City search
                        # "local:hair salons|Virginia" - State search
                        # "local:hair salons|USA" - All 50 states
                        # "local:hair salons|37.0871,-76.4730|5" - Coordinates with 5 mile radius
                        if search_url.startswith('local:'):
                            parts = search_url[6:].split('|')
                            query = parts[0] if len(parts) > 0 else 'businesses'
                            location = parts[1] if len(parts) > 1 else 'United States'
                            
                            logging.info(f"🗺️ STAGE 1: Local Business Scraping - Starting scrape")
                            logging.info(f"🔍 Query: {query} in {location}")
                            logging.info(f"📊 Requesting up to {record_count} businesses")
                            
                            # Use the new raw scraping method for immediate database storage
                            raw_contacts = self.local_scraper.scrape_local_businesses_raw(
                                search_query=query,
                                location=location,
                                max_results=record_count
                            )
                        else:
                            logging.error(f"❌ Invalid local search URL format: {search_url}")
                            raw_contacts = []
                    else:
                        # Default to Apollo scraper
                        # Enforce minimum for Apollo scraper
                        if record_count < 500:
                            logging.info(f"⚠️ Adjusting record count from {record_count} to minimum 500 for Apollo scraper")
                            record_count = 500
                        logging.info(f"🔄 STAGE 1: Apollo Scraping - Starting scrape")
                        logging.info(f"🔍 DEBUG: Starting Apollo scrape for search_url_id={search_url_id}")
                        logging.info(f"📊 Requesting {record_count} records from Apollo")
                        logging.info(f"🌐 Apollo URL: {search_url}")
                        
                        raw_contacts = self.apify_scraper.scrape_contacts(search_url, total_records=record_count)
                    
                    # Log results based on scraper type
                    if scraper_type == 'local':
                        logging.info(f"🏪 Local business scrape returned {len(raw_contacts) if raw_contacts else 0} contacts")
                        if raw_contacts:
                            logging.info(f"📊 Sample contacts from local scraper:")
                            for i, contact in enumerate(raw_contacts[:3], 1):
                                logging.info(f"  {i}. {contact.get('name', 'Unknown')}")
                                logging.info(f"     Email: {contact.get('email', 'None')}")
                                logging.info(f"     Status: {contact.get('email_status', 'N/A')}")
                                logging.info(f"     Website: {'Yes' if contact.get('website_url') else 'No'}")
                    else:
                        logging.info(f"🔍 Apollo scrape returned {len(raw_contacts) if raw_contacts else 0} contacts")
                        if raw_contacts:
                            logging.info(f"🔍 First contact sample: {raw_contacts[0].keys() if raw_contacts else 'N/A'}")
                    
                    if not raw_contacts:
                        logging.error(f"❌ CRITICAL: No contacts found from {scraper_type} scraper for {search_url}")
                        self.supabase_manager.update_search_url_status(search_url_id, "failed")
                        continue
                    
                    # Step 2: Store all raw contact data in Supabase
                    logging.info(f"💾 ATTEMPTING: Storing {len(raw_contacts)} raw contacts in database")
                    logging.info(f"🔍 DEBUG: About to call batch_insert_raw_contacts with search_url_id={search_url_id}")
                    
                    try:
                        stored_count = self.supabase_manager.batch_insert_raw_contacts(raw_contacts, search_url_id)
                        logging.info(f"🔍 DEBUG: batch_insert_raw_contacts returned: {stored_count}")
                        total_raw_contacts += stored_count
                        
                        if stored_count > 0:
                            # Update search URL with results
                            self.supabase_manager.update_search_url_status(
                                search_url_id, "completed", stored_count
                            )
                            logging.info(f"✅ SUCCESSFULLY Stored {stored_count} raw contacts from {search_url}")
                        else:
                            logging.error(f"❌ CRITICAL: batch_insert_raw_contacts returned 0 - database insertion failed!")
                            self.supabase_manager.update_search_url_status(search_url_id, "failed")
                            continue
                            
                    except Exception as db_error:
                        logging.error(f"❌ CRITICAL: Database insertion exception: {db_error}")
                        logging.error(f"🔍 DEBUG: Exception type: {type(db_error)}")
                        self.supabase_manager.update_search_url_status(search_url_id, "failed")
                        continue
                    
                    # Rate limiting between search URLs
                    time.sleep(config.DELAY_BETWEEN_REQUESTS)
                    
                except Exception as e:
                    logging.error(f"❌ Failed to process search URL {search_url}: {e}")
                    self.supabase_manager.update_search_url_status(search_url_id, "failed")
                    continue
            
            # Stage 1.5: Enrich Google Maps contacts with emails (if needed)
            logging.info(f"🔍 Checking for Google Maps contacts needing email enrichment...")
            self._enrich_google_maps_contacts()
            
            # Stage 2: Process qualified raw contacts into leads
            logging.info(f"🔄 STAGE 2: Website Research - Starting website scraping and summary generation")
            logging.info(f"🤖 Stage 2: Processing qualified contacts into leads with AI")
            processed_count = self._process_raw_contacts_to_leads()
            total_processed_leads += processed_count
            
            # Final summary
            logging.info(f"✅ Workflow completed successfully!")
            logging.info(f"📊 Raw contacts stored: {total_raw_contacts}")
            logging.info(f"🎯 Leads generated: {total_processed_leads}")
            
            # Mark campaign as completed if this was a campaign run
            if campaign_id:
                try:
                    self.supabase_manager.update_campaign_status(campaign_id, "completed")
                    logging.info(f"✅ Campaign {campaign_id} marked as completed")
                except Exception as e:
                    logging.error(f"Failed to update campaign status to completed: {e}")
            
            return total_processed_leads > 0
                
        except Exception as e:
            logging.error(f"❌ Workflow failed: {e}")
            # Mark campaign as failed if this was a campaign run
            if campaign_id:
                try:
                    self.supabase_manager.update_campaign_status(campaign_id, "failed")
                    logging.error(f"❌ Campaign {campaign_id} marked as failed due to error")
                except Exception as status_error:
                    logging.error(f"Failed to update campaign status to failed: {status_error}")
            return False
    
    def _enrich_google_maps_contacts(self) -> int:
        """
        Stage 1.5: Enrich Google Maps contacts that have websites but no emails
        This runs AFTER saving to database but BEFORE AI processing
        MODIFIED: Limited time budget and email guessing to prevent blocking
        """
        try:
            # Spend up to 5 minutes on enrichment but don't block the flow
            MAX_ENRICHMENT_TIME = 300  # 5 minutes
            MAX_CONTACTS_TO_ENRICH = 50  # Process first 50 contacts
            
            # Get Google Maps contacts needing enrichment
            contacts = self.supabase_manager.get_google_maps_contacts_needing_enrichment(limit=MAX_CONTACTS_TO_ENRICH)
            
            if not contacts:
                logging.info("✅ No Google Maps contacts need email enrichment")
                return 0
            
            logging.info(f"📧 Found {len(contacts)} Google Maps contacts for quick enrichment (max {MAX_CONTACTS_TO_ENRICH})")
            logging.info(f"⏱️ Time budget: {MAX_ENRICHMENT_TIME} seconds")
            
            # Import web scraper for email extraction
            from modules.web_scraper import WebScraper
            web_scraper = WebScraper()
            
            enriched_count = 0
            guessed_count = 0
            start_time = time.time()
            
            for i, contact in enumerate(contacts):
                # Check time budget
                if time.time() - start_time > MAX_ENRICHMENT_TIME:
                    logging.info(f"⏰ Time budget exceeded after {i} contacts, continuing to Stage 2...")
                    break
                
                website = contact.get('website_url')
                if not website:
                    continue
                
                contact_name = contact.get('name', 'Unknown')
                
                # Quick attempt to scrape (with short timeout)
                email_found = False
                try:
                    # Try to extract email from website (this should have its own timeout)
                    scraped_data = web_scraper.scrape_website_content(website)
                    
                    if scraped_data and scraped_data.get('emails'):
                        # Found email(s) - use the first one
                        email = scraped_data['emails'][0]
                        
                        # Update contact with found email
                        if self.supabase_manager.update_contact_email(contact['id'], email, 'verified'):
                            enriched_count += 1
                            logging.info(f"  ✅ {contact_name}: Found {email}")
                            email_found = True
                        
                except Exception as e:
                    # Don't log full errors to reduce noise
                    pass
                
                # If no email found, just skip this contact
                if not email_found:
                    logging.info(f"  ⏭️  {contact_name}: No email found, skipping")
                
                # Minimal delay to avoid overwhelming
                time.sleep(0.5)
            
            logging.info(f"✅ Enrichment complete: Found {enriched_count} verified emails")
            logging.info(f"⏱️ Time spent: {int(time.time() - start_time)} seconds")
            logging.info(f"📊 Contacts without findable emails will be skipped")
            return enriched_count
            
        except Exception as e:
            logging.error(f"❌ Error enriching Google Maps contacts: {e}")
            return 0
    
    def _process_raw_contacts_to_leads(self) -> int:
        """
        Stage 2: Process raw contacts from database into leads with AI icebreakers
        Now with PARALLEL PROCESSING for massive speed improvements
        
        Returns:
            int: Number of leads successfully generated
        """
        # Check if parallel processing is enabled
        if config.ENABLE_PARALLEL_PROCESSING:
            return self._process_raw_contacts_parallel()
        else:
            return self._process_raw_contacts_sequential()
    
    def _process_raw_contacts_parallel(self) -> int:
        """
        Process contacts in parallel using ThreadPoolExecutor
        """
        total_leads_created = 0
        batch_number = 1
        
        try:
            while True:
                # Get next batch of unprocessed contacts
                logging.info(f"📋 Batch {batch_number}: Fetching {config.BATCH_SIZE} unprocessed contacts...")
                unprocessed_contacts = self.supabase_manager.get_unprocessed_contacts(
                    limit=config.BATCH_SIZE,
                    min_confidence=0.7
                )
                
                if not unprocessed_contacts:
                    logging.info(f"✅ No more unprocessed contacts found. Completed all batches!")
                    break
                
                logging.info(f"📊 Batch {batch_number}: Processing {len(unprocessed_contacts)} contacts in PARALLEL")
                logging.info(f"⚡ Using {config.MAX_CONTACTS_PARALLEL} parallel workers")
                
                batch_leads_created = 0
                
                # Process contacts in parallel
                with ThreadPoolExecutor(max_workers=config.MAX_CONTACTS_PARALLEL) as executor:
                    # Submit all contact processing tasks
                    future_to_contact = {}
                    for i, contact in enumerate(unprocessed_contacts, 1):
                        if not isinstance(contact, dict):
                            logging.error(f"Invalid contact type: {type(contact)}")
                            continue
                        
                        future = executor.submit(
                            self._process_single_contact,
                            contact, batch_number, i, len(unprocessed_contacts)
                        )
                        future_to_contact[future] = contact
                    
                    # Collect results as they complete
                    for future in as_completed(future_to_contact):
                        contact = future_to_contact[future]
                        try:
                            lead_created = future.result()
                            if lead_created:
                                batch_leads_created += 1
                        except Exception as e:
                            logging.error(f"Error processing contact {contact.get('name', 'Unknown')}: {e}")
                
                total_leads_created += batch_leads_created
                logging.info(f"✅ Batch {batch_number} completed: {batch_leads_created} leads created")
                
                batch_number += 1
                
                # Optional: Add a small delay between batches to prevent overwhelming the system
                if batch_number > 1:
                    time.sleep(2)
            
            return total_leads_created
            
        except Exception as e:
            logging.error(f"❌ Error in parallel contact processing: {e}")
            return total_leads_created
    
    def _process_raw_contacts_sequential(self) -> int:
        """
        Original sequential processing method (fallback)
        """
        total_leads_created = 0
        batch_number = 1
        
        try:
            logging.info(f"🚀 Starting batch processing loop...")
            while True:
                # Get next batch of unprocessed contacts that meet quality criteria
                logging.info(f"📋 Processing Batch {batch_number}: Fetching {config.BATCH_SIZE} unprocessed contacts...")
                logging.info(f"🔍 Calling get_unprocessed_contacts with limit={config.BATCH_SIZE}, organization_id={self.supabase_manager.organization_id}")
                unprocessed_contacts = self.supabase_manager.get_unprocessed_contacts(
                    limit=config.BATCH_SIZE,  # Process in manageable batches
                    min_confidence=0.7  # Only high-confidence emails
                )
                
                logging.info(f"🔍 get_unprocessed_contacts returned: {len(unprocessed_contacts) if unprocessed_contacts else 0} contacts")
                
                if not unprocessed_contacts:
                    logging.info(f"✅ No more unprocessed contacts found. Completed all batches!")
                    logging.info(f"📊 Final stats: {total_leads_created} total leads created from {batch_number - 1} batches")
                    break
                
                logging.info(f"📊 Batch {batch_number}: Found {len(unprocessed_contacts)} qualified contacts to process")
                
                batch_leads_created = 0
                
                # Process each contact in this batch
                for i, contact in enumerate(unprocessed_contacts, 1):
                    lead_created = self._process_single_contact(contact, batch_number, i, len(unprocessed_contacts))
                    if lead_created:
                        batch_leads_created += 1
                
                total_leads_created += batch_leads_created
                logging.info(f"✅ Batch {batch_number}: {batch_leads_created}/{len(unprocessed_contacts)} leads created")
                logging.info(f"📈 Total progress: {total_leads_created} leads created so far")
                
                batch_number += 1
                logging.info(f"🔄 Moving to batch {batch_number}...")
                
                # Rate limiting between batches
                if batch_number > 1:
                    logging.info(f"⏱️ Rate limiting: Waiting 2 seconds before next batch...")
                    time.sleep(2)
                    logging.info(f"▶️ Continuing with batch {batch_number}")
                    
        except Exception as e:
            import traceback
            logging.error(f"❌ Error processing batch {batch_number}: {e}")
            logging.error(f"❌ Exception type: {type(e).__name__}")
            logging.error(f"❌ Full traceback:\n{traceback.format_exc()}")
            logging.error(f"📊 Progress before error: {total_leads_created} leads created from {batch_number - 1} completed batches")
            # Don't silently continue - re-raise to make the error visible
            raise
        
        return total_leads_created
    
    def _process_single_contact(self, contact: dict, batch_number: int, contact_index: int, total_contacts: int) -> bool:
        """
        Process a single contact into a lead
        Returns True if lead was successfully created
        """
        try:
            # Debug contact data structure
            if not isinstance(contact, dict):
                logging.error(f"❌ CRITICAL: Contact is not a dictionary, it's {type(contact)}: {contact}")
                return False
                
            logging.info(f"🤖 [{batch_number}.{contact_index}/{total_contacts}] Processing: {contact.get('name', 'Unknown')} ({contact.get('email', 'No email')})")
            logging.info(f"📍 Progress: Batch {batch_number}, Contact {contact_index} of {total_contacts}")
            logging.info(f"🔄 STAGE 2: Processing contact {contact_index} of {total_contacts} - Researching websites")
            
            # Check if contact already has website summaries (from local business scraper)
            if contact.get('website_summaries'):
                logging.info(f"✅ Using pre-scraped website summaries from local business scraper")
                content_summaries = contact.get('website_summaries', [])
                website_url = contact.get('website_url', '')
                website_failed = False
            else:
                # Extract website and research it
                website_url = contact.get('website_url', '')
                content_summaries = []
                website_failed = False
            
            # Step 1: Scrape and summarize website if not already done
            if website_url and not content_summaries:
                logging.info(f"🌐 [{batch_number}.{contact_index}] Scraping website: {website_url}")
                try:
                    website_data = self.web_scraper.scrape_website_content(website_url)
                    page_summaries = website_data.get('summaries', [])
                    
                    if page_summaries:
                        # Step 2: Generate AI summaries of website content  
                        logging.info(f"🧠 [{batch_number}.{contact_index}] Generating AI summaries for website content")
                        logging.info(f"🔍 DEBUG: page_summaries type: {type(page_summaries)}, length: {len(page_summaries) if hasattr(page_summaries, '__len__') else 'N/A'}")
                        content_summaries = self.ai_processor.summarize_website_pages(page_summaries)
                        logging.info(f"🔍 DEBUG: content_summaries type: {type(content_summaries)}, length: {len(content_summaries) if hasattr(content_summaries, '__len__') else 'N/A'}")
                    else:
                        logging.warning(f"⚠️ Website scraping failed for {website_url} (blocked/inaccessible)")
                        website_failed = True
                        # Don't mention the website is blocked - just use job title/industry info
                        content_summaries = []
                except Exception as website_error:
                    logging.warning(f"⚠️ Website scraping exception for {website_url}: {website_error}")
                    website_failed = True
                    # Don't mention scraping failed - generate based on other data
                    content_summaries = []
            else:
                logging.warning(f"No website URL for contact {contact.get('name')}")
                website_failed = True
                # No website, so generate based on name/title/company only
                content_summaries = []
            
            # Step 3: Prepare contact data for icebreaker generation (ALWAYS PROCEED)
            contact_info = {
                'first_name': contact.get('name', '').split(' ')[0] if contact.get('name') else '',
                'last_name': contact.get('last_name', ''),
                'headline': contact.get('title', '') or contact.get('headline', ''),
                'location': f"{contact.get('city', '')} {contact.get('country', '')}".strip(),
                'company_name': contact.get('company_name') or contact.get('organization', {}).get('name', '') if isinstance(contact.get('organization'), dict) else '',
                'is_business_contact': contact.get('is_business_contact', False),
                'website_summaries': content_summaries
            }
            
            # Step 4: Generate icebreaker (ALWAYS ATTEMPT - even with limited data)
            logging.info(f"🔄 STAGE 3: Generating icebreaker {contact_index} of {total_contacts} - Creating AI-powered icebreaker")
            logging.info(f"💬 [{batch_number}.{contact_index}] Generating AI icebreaker for {contact.get('name')} {'(limited data)' if website_failed else ''}")
            icebreaker_response = self.ai_processor.generate_icebreaker(contact_info, content_summaries)
            
            # ENHANCED: Never skip leads - always create a lead entry
            if not icebreaker_response or not icebreaker_response.get('icebreaker'):
                logging.warning(f"AI icebreaker failed for {contact.get('name')} - using fallback")
                # Create a fallback icebreaker based on available contact info
                fallback_icebreaker = self._create_fallback_icebreaker(contact_info)
                fallback_subject = self._create_fallback_subject(contact_info)
                icebreaker_response = {"icebreaker": fallback_icebreaker, "subject_line": fallback_subject}
            
            # Step 5: Prepare lead data
            lead_data = {
                'first_name': contact_info['first_name'],
                'last_name': contact_info['last_name'],
                'email': contact.get('email'),
                'linkedin_url': contact.get('linkedin_url'),
                'headline': contact_info['headline'],
                'website_url': website_url,  # Fixed: was 'company_website'
                'location': contact_info['location'],
                'icebreaker': icebreaker_response['icebreaker'],
                'subject_line': icebreaker_response.get('subject_line', ''),
                'website_summaries': content_summaries
            }
            
            # Step 6: Store processed lead in Supabase
            logging.info(f"💾 [{batch_number}.{contact_index}] Storing lead in database for {contact.get('name')}")
            processing_settings = {
                'ai_model_summary': config.AI_MODEL_SUMMARY,
                'ai_model_icebreaker': config.AI_MODEL_ICEBREAKER,
                'ai_temperature': config.AI_TEMPERATURE,
                'delay_between_ai_calls': config.DELAY_BETWEEN_AI_CALLS,
                'min_confidence': 0.7
            }
            
            created_lead = self.supabase_manager.create_processed_lead(
                contact['id'],
                contact['search_url_id'],
                lead_data,
                processing_settings
            )
            
            if created_lead:
                # Mark raw contact as processed
                self.supabase_manager.mark_contact_processed(contact['id'])
                logging.info(f"✅ [{batch_number}.{contact_index}] SUCCESS: Created lead for {lead_data['first_name']} {lead_data['last_name']}")
                return True
            else:
                logging.error(f"❌ [{batch_number}.{contact_index}] FAILED: Could not create lead for {contact.get('name')}")
                return False
                
        except Exception as e:
            error_msg = str(e).lower()
            if "cloudflare" in error_msg or "403" in error_msg or "blocked" in error_msg:
                logging.warning(f"⚠️ Website blocked/protected for {contact.get('name', 'Unknown')}: {e}")
            else:
                logging.error(f"❌ Processing error for {contact.get('name', 'Unknown')}: {e}")
                logging.error(f"🔍 DEBUG: Exception type: {type(e)}")
                logging.error(f"🔍 DEBUG: Contact data type: {type(contact)}")
                if hasattr(contact, 'keys'):
                    logging.error(f"🔍 DEBUG: Contact keys: {list(contact.keys())}")
                else:
                    logging.error(f"🔍 DEBUG: Contact content: {contact}")
                import traceback
                logging.error(f"🔍 DEBUG: Full traceback: {traceback.format_exc()}")
            return False
    
    def _create_fallback_icebreaker(self, contact_info: Dict[str, Any]) -> str:
        """
        Create a fallback icebreaker when AI generation fails or website data is unavailable
        
        Args:
            contact_info: Basic contact information
            
        Returns:
            str: Simple personalized icebreaker based on available data
        """
        first_name = contact_info.get('first_name', 'there')
        headline = contact_info.get('headline', '')
        location = contact_info.get('location', '')
        
        # Create different fallback templates based on available data
        if headline and location:
            return f"Hi {first_name},\n\nSaw your profile as {headline} in {location}. Working on something in your space that might be relevant.\n\nWould love to connect and share what we're building."
        elif headline:
            return f"Hi {first_name},\n\nNoticed your work as {headline}. We're building something that aligns with your expertise.\n\nInterested in a quick chat about potential synergies?"
        elif location:
            return f"Hi {first_name},\n\nConnecting with professionals in {location}. Working on something that might interest your network.\n\nOpen to a brief conversation?"
        else:
            return f"Hi {first_name},\n\nCame across your profile and thought there might be some interesting overlap with what we're working on.\n\nWould you be open to a brief conversation?"
    
    def _create_fallback_subject(self, contact_info: Dict[str, Any]) -> str:
        """
        Create a fallback subject line with variety
        
        Args:
            contact_info: Basic contact information
            
        Returns:
            str: Short, engaging subject line
        """
        import random
        first_name = contact_info.get('first_name', 'there')
        company = contact_info.get('company_name', contact_info.get('company', ''))
        
        if company and len(company) > 3:
            short_company = company[:20] if len(company) > 20 else company
            return random.choice([
                f"Quick question about {short_company}",
                f"{first_name}, about {short_company[:15]}",
                f"Idea for {short_company}",
                f"{short_company} opportunity",
            ])
        else:
            return random.choice([
                f"Quick question, {first_name}",
                f"{first_name}, 30 seconds?",
                f"Idea for you, {first_name}",
                f"Relevant for you, {first_name}",
            ])
    
    def _run_legacy_workflow(self) -> bool:
        """Legacy workflow for Google Sheets compatibility"""
        if not self.use_sheets:
            logging.error("Neither Supabase nor Google Sheets is available")
            return False
            
        # Original Google Sheets workflow logic would go here
        logging.warning("Legacy Google Sheets workflow not implemented")
        return False
    
    def _process_contacts_batch(self, contacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a batch of contacts through the complete pipeline
        
        Args:
            contacts: List of contact dictionaries from Apify
            
        Returns:
            List of processed contacts with icebreakers
        """
        processed_contacts = []
        
        for contact in contacts[:config.BATCH_SIZE]:  # Limit batch size
            try:
                logging.info(f"🔬 Processing contact: {contact.get('first_name', '')} {contact.get('last_name', '')}")
                
                # Step 1: Extract website URL
                website_url = contact.get('organization', {}).get('website_url', '')
                if not website_url:
                    logging.warning(f"No website URL for contact {contact.get('first_name', '')}")
                    continue
                
                # Step 2: Research website
                website_data = self.web_scraper.scrape_website_content(website_url)
                page_summaries = website_data.get('summaries', [])
                
                if not page_summaries:
                    logging.warning(f"No website content found for {website_url}")
                    continue
                
                # Step 3: Generate AI summaries
                content_summaries = self.ai_processor.summarize_website_pages(page_summaries)
                
                # Step 4: Generate icebreaker
                contact_with_summaries = contact.copy()
                contact_with_summaries['website_summaries'] = content_summaries
                
                # Add location field (combining city and country)
                contact_with_summaries['location'] = f"{contact.get('city', '')} {contact.get('country', '')}".strip()
                
                icebreaker_result = self.ai_processor.generate_icebreaker(
                    contact_with_summaries, 
                    content_summaries
                )
                
                # Step 5: Prepare final contact data
                final_contact = {
                    'first_name': contact.get('first_name', ''),
                    'last_name': contact.get('last_name', ''),
                    'email': contact.get('email', ''),
                    'website_url': website_url,
                    'phone_number': '',  # Not available from this Apify scraper
                    'location': contact_with_summaries['location'],
                    'mutiline_icebreaker': icebreaker_result.get('icebreaker', ''),
                    'subject_line': icebreaker_result.get('subject_line', '')
                }
                
                processed_contacts.append(final_contact)
                logging.info(f"✅ Successfully processed {contact.get('first_name', '')} {contact.get('last_name', '')}")
                
            except Exception as e:
                logging.error(f"❌ Failed to process contact {contact.get('first_name', '')}: {e}")
                continue
        
        return processed_contacts
    
    def test_connections(self) -> bool:
        """Test all API connections"""
        logging.info("🧪 Testing API connections...")
        
        tests = [
            ("Apify API (Apollo)", self.apify_scraper.test_connection),
            ("Local Business Scraper", self.local_scraper.test_connection),
            ("OpenAI API", self.ai_processor.test_connection),
        ]
        
        # Add Supabase test if available
        if self.use_supabase and self.supabase_manager:
            tests.append(("Supabase Database", self.supabase_manager.test_connection))
        
        all_passed = True
        for test_name, test_func in tests:
            try:
                if test_func():
                    logging.info(f"✅ {test_name} connection successful")
                else:
                    logging.error(f"❌ {test_name} connection failed")
                    all_passed = False
            except Exception as e:
                logging.error(f"❌ {test_name} connection error: {e}")
                all_passed = False
        
        return all_passed
    
    def run_single_contact_test(self, search_url: str) -> bool:
        """
        Test the workflow with a single Apollo search URL for debugging
        
        Args:
            search_url: A single Apollo search URL to test
            
        Returns:
            bool: True if successful
        """
        try:
            logging.info(f"🧪 Running single contact test with: {search_url}")
            
            # Get contacts using the record count from environment (set by server)
            record_count = int(os.getenv('RECORD_COUNT', '500'))
            # Enforce minimum for Apollo scraper
            if record_count < 500:
                logging.info(f"⚠️ Test mode: Adjusting record count from {record_count} to minimum 500 for Apollo scraper")
                record_count = 500
            logging.info(f"📊 Test mode requesting {record_count} records from Apollo")
            
            # *** FIX: Use the same two-stage Supabase pipeline as production ***
            if not self.use_supabase:
                logging.error("❌ CRITICAL: Test mode requires Supabase to be enabled for raw contact storage")
                return False
                
            # Find existing search URL or create new one (allows re-running same URL)
            search_data = self.supabase_manager.find_or_create_search_url(search_url, "Test Apollo URL")
            if not search_data:
                logging.error("❌ CRITICAL: Failed to create search URL entry for test")
                return False
                
            search_url_id = search_data['id']
            logging.info(f"🔍 DEBUG TEST: Created search_url_id={search_url_id} for test")
            
            # Stage 1: Scrape and store raw contacts (same as production)
            raw_contacts = self.apify_scraper.scrape_contacts(search_url, total_records=record_count)
            
            if not raw_contacts:
                logging.error("❌ CRITICAL: No contacts found in test")
                return False
                
            # Stage 2: Store ALL raw contact data in Supabase first
            logging.info(f"💾 TEST MODE: Storing {len(raw_contacts)} raw contacts in database")
            stored_count = self.supabase_manager.batch_insert_raw_contacts(raw_contacts, search_url_id)
            
            if stored_count == 0:
                logging.error("❌ CRITICAL: Failed to store raw contacts in test mode")
                return False
                
            logging.info(f"✅ TEST SUCCESS: Stored {stored_count} raw contacts")
            
            # Stage 3: Run the same Stage 2 processing as production
            logging.info(f"🤖 TEST MODE: Running Stage 2 processing on stored raw contacts")
            processed_count = self._process_raw_contacts_to_leads()
            
            return processed_count > 0
                
        except Exception as e:
            logging.error(f"Single contact test error: {e}")
            return False

def main():
    """Main entry point"""
    try:
        # Get organization context from environment (set by server)
        organization_id = os.getenv('CURRENT_ORGANIZATION_ID')
        
        # Try Supabase first (new default) with organization context
        orchestrator = LeadGenerationOrchestrator(
            use_supabase=True, 
            use_sheets=False, 
            organization_id=organization_id
        )
    except Exception as e:
        logging.error(f"Failed to initialize orchestrator with Supabase: {e}")
        # Fallback to legacy mode without any database
        logging.info("Trying to initialize in legacy test mode...")
        try:
            orchestrator = LeadGenerationOrchestrator(use_supabase=False, use_sheets=False)
        except Exception as e2:
            logging.error(f"Failed to initialize orchestrator: {e2}")
            return
    
    # Test connections first
    if not orchestrator.test_connections():
        logging.error("❌ Connection tests failed. Please check your API keys.")
        return
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            # Run single contact test
            # Check if a test URL was provided via environment or use a default
            test_url = os.getenv('TEST_APOLLO_URL', '')
            if not test_url:
                # For UI-based testing, we'll use a default Apollo URL
                test_url = "https://app.apollo.io/#/people?page=1&organizationLocations[]=United%20States&organizationNumEmployeesRanges[]=1%2C10&organizationNumEmployeesRanges[]=51%2C100&organizationNumEmployeesRanges[]=11%2C20&organizationNumEmployeesRanges[]=21%2C50&organizationIndustryTagIds[]=5567cd4773696439b10b0000&organizationIndustryTagIds[]=5567cd4e7369643b70010000&sortByField=%5Bnone%5D&sortAscending=false&personTitles[]=manager&personTitles[]=ceo&personTitles[]=cmo"
                logging.info(f"Using default Apollo test URL: {test_url[:100]}...")
            
            orchestrator.run_single_contact_test(test_url)
        elif sys.argv[1] == "once":
            # Run workflow once
            if orchestrator.use_supabase or orchestrator.use_sheets:
                orchestrator.run_workflow()
            else:
                logging.error("Cannot run full workflow without database. Use 'test' mode instead.")
        elif sys.argv[1] == "campaign":
            # Run campaign workflow
            campaign_id = os.getenv('CAMPAIGN_ID')
            if not campaign_id:
                logging.error("Campaign ID not provided. Set CAMPAIGN_ID environment variable.")
                return
            
            if orchestrator.use_supabase:
                logging.info(f"🎯 Starting campaign execution for: {campaign_id}")
                orchestrator.run_workflow(campaign_id=campaign_id)
            else:
                logging.error("Campaign mode requires Supabase to be enabled.")
        else:
            print("Usage: python main.py [test|once|campaign]")
    else:
        # Default: run workflow once
        if orchestrator.use_supabase or orchestrator.use_sheets:
            orchestrator.run_workflow()
        else:
            logging.error("Cannot run full workflow without database. Use 'test' mode instead.")

if __name__ == "__main__":
    main()
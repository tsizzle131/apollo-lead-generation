"""
Google Maps Campaign Manager
Orchestrates the complete two-phase scraping process
Phase 1: Google Maps scraping by ZIP code
Phase 2: Facebook email enrichment
"""

import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from .coverage_analyzer import CoverageAnalyzer
from .local_business_scraper import LocalBusinessScraper
from .facebook_scraper import FacebookScraper
from .linkedin_scraper_parallel import LinkedInScraperParallel
from .bouncer_verifier import BouncerVerifier
from .gmaps_supabase_manager import GmapsSupabaseManager
from .ai_processor import AIProcessor
import sys
import os
# Add parent directory to path to import api_costs
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from api_costs import get_service_cost

class GmapsCampaignManager:
    # Phase timeout constants (in seconds)
    PHASE_TIMEOUTS = {
        'phase_1': 30 * 60,  # 30 minutes
        'phase_2_facebook': 60 * 60,  # 60 minutes
        'phase_2_5_linkedin': 90 * 60  # 90 minutes
    }
    def __init__(self, supabase_url: str = None, supabase_key: str = None,
                 apify_key: str = None, openai_key: str = None,
                 linkedin_actor_id: str = None, bouncer_api_key: str = None):
        """Initialize campaign manager with all necessary components"""

        # Initialize components
        self.db = GmapsSupabaseManager(supabase_url, supabase_key)
        self.coverage_analyzer = CoverageAnalyzer(self.db)

        # Initialize AI processor for icebreaker generation
        self.ai_processor = AIProcessor(openai_key) if openai_key else None

        # Initialize scrapers with AI processor
        self.google_scraper = LocalBusinessScraper(apify_key, ai_processor=self.ai_processor)
        self.facebook_scraper = FacebookScraper(apify_key)
        self.linkedin_scraper = LinkedInScraperParallel(apify_key, linkedin_actor_id)
        self.email_verifier = BouncerVerifier(bouncer_api_key)

        # Campaign ID for timeout error handling
        self.campaign_id = None

        # Heartbeat monitoring
        self.running = False
        self.heartbeat_thread = None

        # Thread-safe lock for parallel icebreaker generation
        self._icebreaker_lock = threading.Lock()

        # Parallel icebreaker configuration
        self.ICEBREAKER_MAX_WORKERS = 5  # Number of parallel workers

        logging.info("✅ Google Maps Campaign Manager initialized with PARALLEL LinkedIn enrichment")
        if self.ai_processor:
            logging.info("✅ AI Processor initialized for icebreaker generation")
        else:
            logging.warning("⚠️ AI Processor not initialized - icebreakers will not be generated")

    def _process_single_icebreaker(
        self,
        business: Dict[str, Any],
        campaign: Dict[str, Any],
        organization_data: Optional[Dict[str, Any]],
        web_scraper: Any,
        idx: int,
        total: int
    ) -> Tuple[bool, str]:
        """
        Process a single business for icebreaker generation (thread worker).

        Args:
            business: Business record from database
            campaign: Campaign record
            organization_data: Organization context for personalization
            web_scraper: WebScraper instance
            idx: Current index (for logging)
            total: Total count (for logging)

        Returns:
            Tuple of (success: bool, business_name: str)
        """
        business_name = business.get('name', 'Unknown Business')
        website = business.get('website')
        email = business.get('email')

        try:
            logging.info(f"  [{idx}/{total}] Processing: {business_name}")

            # Scrape website for context if available
            website_summaries = []
            if website:
                try:
                    website_data = web_scraper.scrape_website_content(website)
                    raw_summaries = website_data.get('summaries', [])

                    # Convert summaries from dicts to strings
                    if raw_summaries:
                        for summary in raw_summaries:
                            if isinstance(summary, dict):
                                summary_text = summary.get('summary') or summary.get('content') or summary.get('abstract')
                                if summary_text and summary_text != 'no content':
                                    website_summaries.append(summary_text)
                            elif isinstance(summary, str):
                                if summary and summary != 'no content':
                                    website_summaries.append(summary)

                    logging.debug(f"    Scraped website: {len(website_summaries)} summaries")
                except Exception as e:
                    logging.debug(f"    Could not scrape website: {e}")

            # Prepare contact info for AI with rich business context
            contact_info = {
                'first_name': business_name,
                'last_name': 'Business Contact',
                'name': business_name,
                'email': email,
                'headline': business.get('category', ''),
                'company_name': business_name,
                'is_business_contact': True,
                'organization': {
                    'name': business_name,
                    'category': business.get('category', ''),
                    'city': business.get('city', ''),
                    'state': business.get('state', ''),
                    'rating': business.get('rating'),
                    'reviews_count': business.get('reviews_count'),
                    'description': business.get('description', '')
                },
                'website_url': website,
                'city': business.get('city', ''),
                'state': business.get('state', ''),
                'rating': business.get('rating'),
                'reviews_count': business.get('reviews_count')
            }

            # Assign A/B test variant for this business
            variant = self.ai_processor._assign_variant(
                str(business['id']),
                str(campaign['id'])
            )

            # Check if this is a perfect-fit prospect
            target_categories = organization_data.get('target_categories', []) if organization_data else []
            is_perfect_fit = self.ai_processor._is_perfect_fit(
                business.get('category', ''),
                target_categories
            )

            # Get campaign-level template selection (default: 'auto')
            icebreaker_template = campaign.get('icebreaker_template', 'auto')

            # Generate icebreaker with organization data and template
            icebreaker_result = self.ai_processor.generate_icebreaker(
                contact_info,
                website_summaries,
                organization_data,
                icebreaker_template
            )

            if icebreaker_result and icebreaker_result.get('icebreaker'):
                # Get template/formula used from the result
                template_used = icebreaker_result.get('template_used', 'auto')
                formula_used = icebreaker_result.get('formula_used', 'unknown')

                # Build metadata for A/B testing analysis
                icebreaker_metadata = {
                    'is_perfect_fit': is_perfect_fit,
                    'target_categories': target_categories,
                    'prospect_category': business.get('category', ''),
                    'prompt_version': '2.0',
                    'has_website_content': bool(website_summaries),
                    'formula_used': formula_used
                }

                # Save to database with variant and template tracking (thread-safe via Supabase)
                self.db.client.table("gmaps_businesses")\
                    .update({
                        'icebreaker': icebreaker_result.get('icebreaker'),
                        'subject_line': icebreaker_result.get('subject_line'),
                        'icebreaker_generated_at': datetime.now().isoformat(),
                        'icebreaker_variant': variant,
                        'icebreaker_template': template_used,
                        'icebreaker_metadata': icebreaker_metadata
                    })\
                    .eq('id', business['id'])\
                    .execute()

                fit_status = "🎯 perfect-fit" if is_perfect_fit else "📝 general"
                logging.info(f"    [{idx}/{total}] ✅ Generated icebreaker ({template_used}/{formula_used}, {fit_status})")
                return (True, business_name)
            else:
                logging.debug(f"    [{idx}/{total}] ⚠️ No icebreaker generated")
                return (False, business_name)

        except Exception as e:
            logging.warning(f"    [{idx}/{total}] ⚠️ Failed to generate icebreaker for {business_name}: {e}")
            return (False, business_name)

    def create_campaign(self, name: str, location: str, keywords: List[str], 
                       coverage_profile: str = "balanced", 
                       description: str = None) -> Dict[str, Any]:
        """
        Create a new campaign with AI-powered ZIP code selection
        
        Args:
            name: Campaign name
            location: Target location (e.g., "Los Angeles, CA", "Austin, TX")
            keywords: Business keywords to search
            coverage_profile: 'aggressive', 'balanced', 'budget', or 'custom'
            description: Optional campaign description
            
        Returns:
            Campaign details with ID and selected ZIP codes
        """
        try:
            logging.info(f"📋 Creating campaign: {name}")
            logging.info(f"   Location: {location}")
            logging.info(f"   Keywords: {', '.join(keywords)}")
            logging.info(f"   Profile: {coverage_profile}")
            
            # Step 1: Use AI to analyze location and select ZIP codes
            logging.info("🤖 Analyzing location with AI...")
            coverage_analysis = self.coverage_analyzer.analyze_location(
                location=location,
                keywords=keywords,
                profile=coverage_profile
            )
            
            if not coverage_analysis.get("zip_codes"):
                logging.error("❌ No ZIP codes identified for location")
                return {"error": "Could not identify ZIP codes for the specified location"}
            
            # Step 2: Create campaign in database
            campaign_data = {
                "name": name,
                "description": description or f"Scraping {', '.join(keywords)} in {location}",
                "keywords": keywords,
                "location": location,
                "coverage_profile": coverage_profile,
                "status": "draft",
                "target_zip_count": len(coverage_analysis["zip_codes"]),
                "coverage_percentage": self._calculate_coverage_percentage(
                    len(coverage_analysis["zip_codes"]), 
                    coverage_profile
                ),
                "estimated_cost": coverage_analysis.get("cost_estimates", {}).get("total_cost", 0),
                "organization_id": self.db.organization_id
            }
            
            campaign = self.db.create_campaign(campaign_data)
            if not campaign or not campaign.get("id"):
                logging.error("❌ Failed to create campaign in database")
                return {"error": "Database error creating campaign"}
            
            campaign_id = campaign["id"]
            logging.info(f"✅ Campaign created with ID: {campaign_id}")
            
            # Step 3: Add ZIP codes to campaign coverage
            zip_codes_with_keywords = []
            for zip_data in coverage_analysis["zip_codes"]:
                zip_data["keywords"] = keywords
                # Use centralized cost calculation
                estimated_businesses = zip_data.get("estimated_businesses", 250)
                zip_data["estimated_cost"] = get_service_cost("google_maps", estimated_businesses)
                zip_codes_with_keywords.append(zip_data)
            
            added_count = self.db.add_campaign_coverage(campaign_id, zip_codes_with_keywords)
            logging.info(f"✅ Added {added_count} ZIP codes to campaign")
            
            # Step 4: Return campaign details
            return {
                "campaign_id": campaign_id,
                "name": name,
                "location": location,
                "keywords": keywords,
                "coverage_profile": coverage_profile,
                "zip_count": len(coverage_analysis["zip_codes"]),
                "estimated_businesses": coverage_analysis.get("total_estimated_businesses", 0),
                "estimated_cost": coverage_analysis.get("cost_estimates", {}).get("total_cost", 0),
                "estimated_emails": coverage_analysis.get("cost_estimates", {}).get("estimated_emails", 0),
                "cost_per_email": coverage_analysis.get("cost_estimates", {}).get("cost_per_email", 0),
                "status": "draft",
                "coverage_analysis": coverage_analysis
            }
            
        except Exception as e:
            logging.error(f"Error creating campaign: {e}")
            return {"error": str(e)}

    def _execute_phase_with_timeout(self, phase_func, phase_name, timeout_seconds):
        """Execute a phase with timeout protection"""
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(phase_func)
            try:
                return future.result(timeout=timeout_seconds)
            except concurrent.futures.TimeoutError:
                logging.error(f"{phase_name} timed out after {timeout_seconds}s")
                self.db.update_campaign(self.campaign_id, {
                    'status': 'failed',
                    'error_message': f'{phase_name} timed out after {timeout_seconds/60:.1f} minutes'
                })
                raise TimeoutError(f"{phase_name} exceeded {timeout_seconds/60:.1f} minute timeout")

    def _start_heartbeat(self):
        """Start background heartbeat thread to update campaign timestamp"""
        def heartbeat_loop():
            while self.running:
                try:
                    # Just touch updated_at to show we're alive
                    self.db.update_campaign(self.campaign_id, {})
                    time.sleep(60)
                except Exception as e:
                    logging.debug(f"Heartbeat error: {e}")
                    break

        self.heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        logging.info("✅ Heartbeat monitoring started")

    def _stop_heartbeat(self):
        """Stop heartbeat thread"""
        self.running = False
        if hasattr(self, 'heartbeat_thread') and self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=2)
            logging.info("✅ Heartbeat monitoring stopped")

    def execute_campaign(self, campaign_id: str, max_businesses_per_zip: int = 1000) -> Dict[str, Any]:
        """
        Execute a campaign - run both phases of scraping

        Args:
            campaign_id: Campaign ID to execute
            max_businesses_per_zip: Maximum businesses to scrape per ZIP

        Returns:
            Execution summary with results
        """
        try:
            # Store campaign_id for timeout error handling
            self.campaign_id = campaign_id

            # Get campaign details
            campaign = self.db.get_campaign(campaign_id)
            if not campaign:
                logging.error(f"Campaign {campaign_id} not found")
                return {"error": "Campaign not found"}

            logging.info("="*70)
            logging.info(f"🚀 EXECUTING CAMPAIGN: {campaign['name']}")
            logging.info("="*70)

            # Update campaign status to running
            self.db.update_campaign(campaign_id, {
                "status": "running",
                "started_at": datetime.now().isoformat()
            })

            # Start heartbeat monitoring
            self.running = True
            self._start_heartbeat()
            
            # Get ZIP codes to scrape
            coverage = self.db.get_campaign_coverage(campaign_id, scraped=False)
            if not coverage:
                # No ZIP codes configured - run coverage analysis now
                logging.info("🤖 No ZIP codes found - analyzing location...")

                coverage_analysis = self.coverage_analyzer.analyze_location(
                    location=campaign['location'],
                    keywords=campaign['keywords'],
                    profile=campaign.get('coverage_profile', 'balanced')
                )

                if not coverage_analysis.get("zip_codes"):
                    logging.error("❌ No ZIP codes identified for location")
                    return {"error": "Could not identify ZIP codes for the specified location"}

                # Add ZIP codes to campaign coverage
                zip_codes_with_keywords = []
                for zip_data in coverage_analysis["zip_codes"]:
                    zip_data["keywords"] = campaign['keywords']
                    # Use centralized cost calculation
                    estimated_businesses = zip_data.get("estimated_businesses", 250)
                    zip_data["estimated_cost"] = get_service_cost("google_maps", estimated_businesses)
                    zip_codes_with_keywords.append(zip_data)

                added_count = self.db.add_campaign_coverage(campaign_id, zip_codes_with_keywords)
                logging.info(f"✅ Added {added_count} ZIP codes to campaign")

                # Update campaign with ZIP count and estimated cost
                self.db.update_campaign(campaign_id, {
                    "target_zip_count": len(coverage_analysis["zip_codes"]),
                    "estimated_cost": coverage_analysis.get("cost_estimates", {}).get("total_cost", 0)
                })

                # Re-fetch coverage
                coverage = self.db.get_campaign_coverage(campaign_id, scraped=False)

                if not coverage:
                    logging.warning("No ZIP codes to scrape after analysis")
                    return {"error": "No ZIP codes configured for campaign"}
            
            logging.info(f"📍 Will scrape {len(coverage)} ZIP codes")
            
            # Initialize counters
            total_businesses = 0
            total_emails = 0
            total_facebook_pages = 0
            total_cost = 0
            
            # Phase 1: Google Maps Scraping
            logging.info("\n" + "="*50)
            logging.info("PHASE 1: GOOGLE MAPS SCRAPING")
            logging.info("="*50)

            gmaps_verified_emails = 0

            # Process ZIPs in batches of 10 for efficiency
            batch_size = 10
            for batch_idx in range(0, len(coverage), batch_size):
                batch = coverage[batch_idx:batch_idx+batch_size]
                zip_codes = [z["zip_code"] for z in batch]

                # Get keywords (assuming all ZIPs use same keywords)
                keywords = batch[0].get("keywords", campaign.get("keywords", []))

                logging.info(f"\n🔄 Batching {len(zip_codes)} ZIP codes: {', '.join(zip_codes)}")

                # Scrape all ZIPs in this batch at once
                businesses_by_zip = self._scrape_zip_codes_batched(
                    zip_codes=zip_codes,
                    keywords=keywords,
                    max_results=max_businesses_per_zip
                )

                # Process each ZIP's results
                for idx_offset, zip_coverage in enumerate(batch):
                    zip_code = zip_coverage["zip_code"]
                    overall_idx = batch_idx + idx_offset + 1

                    logging.info(f"\n[{overall_idx}/{len(coverage)}] Processing ZIP: {zip_code}")

                    # Get businesses for this specific ZIP
                    businesses = businesses_by_zip.get(zip_code, [])

                    if businesses:
                        # Save businesses to database
                        saved_count = self.db.save_businesses(businesses, campaign_id, zip_code)

                        # CRITICAL FIX: Query actual count from database instead of trusting return value
                        # This ensures we count the real deduplicated businesses saved
                        actual_count_result = self.db.client.table("gmaps_businesses")\
                            .select("id", count="exact")\
                            .eq("campaign_id", campaign_id)\
                            .eq("zip_code", zip_code)\
                            .execute()
                        saved_count = actual_count_result.count if actual_count_result.count is not None else saved_count

                        # Get saved business IDs for verification
                        saved_businesses = self.db.client.table("gmaps_businesses")\
                            .select("id, email")\
                            .eq("campaign_id", campaign_id)\
                            .eq("zip_code", zip_code)\
                            .eq("email_source", "google_maps")\
                            .not_.is_("email", "null")\
                            .execute()

                        # Verify Google Maps emails
                        if saved_businesses.data:
                            logging.info(f"   🔍 Verifying {len(saved_businesses.data)} Google Maps emails...")
                            for business in saved_businesses.data:
                                try:
                                    email = business.get("email")
                                    if email:
                                        verification = self.email_verifier.verify_email(email)

                                        if verification.get("is_safe"):
                                            gmaps_verified_emails += 1

                                        # Save verification
                                        self.db.update_google_maps_verification(
                                            business_id=business["id"],
                                            verification_data=verification
                                        )
                                except Exception as e:
                                    logging.warning(f"   Failed to verify email {email}: {e}")
                                    continue

                        # Count Facebook pages - check multiple fields where Facebook URLs might be
                        facebook_count = sum(1 for b in businesses if (
                            b.get("facebooks") or  # New field from enhanced scraping
                            b.get("facebookUrl") or
                            b.get("facebook") or
                            (b.get("website") and "facebook.com" in str(b.get("website", "")).lower())
                        ))
                        email_count = sum(1 for b in businesses if b.get("email") or b.get("directEmails"))

                        # Calculate cost for this ZIP
                        zip_cost = (len(businesses) / 1000) * 7

                        # Update coverage status
                        self.db.update_coverage_status(
                            campaign_id=campaign_id,
                            zip_code=zip_code,
                            businesses_found=saved_count,
                            emails_found=email_count,
                            actual_cost=zip_cost
                        )

                        # Update totals
                        total_businesses += saved_count
                        total_emails += email_count
                        total_facebook_pages += facebook_count
                        total_cost += zip_cost

                        logging.info(f"   ✅ Found {saved_count} businesses")
                        logging.info(f"   📧 {email_count} have emails")
                        logging.info(f"   ✅ {gmaps_verified_emails} verified emails")
                        logging.info(f"   📘 {facebook_count} have Facebook pages")

                        # Update ZIP code stats
                        self.db.update_zip_code_stats(zip_code, saved_count)
                    else:
                        logging.warning(f"   ❌ No businesses found")

                # Rate limiting between batches (not between individual ZIPs anymore)
                if batch_idx + batch_size < len(coverage):
                    time.sleep(2)

            logging.info(f"\n📊 Phase 1 Summary:")
            logging.info(f"   Total businesses: {total_businesses}")
            logging.info(f"   Google Maps emails verified: {gmaps_verified_emails}")
            
            # Track Google Maps costs
            self.db.track_api_cost(
                campaign_id=campaign_id,
                service="google_maps",
                items=total_businesses,
                cost_usd=total_cost
            )

            # CRITICAL FIX: Update business count NOW, before enrichment phases
            # This ensures the count is saved even if later phases fail
            logging.info(f"\n💾 Saving Phase 1 results: {total_businesses} businesses found")
            self.db.update_campaign(campaign_id, {
                "total_businesses_found": total_businesses,
                "total_emails_found": total_emails,
                "total_facebook_pages_found": total_facebook_pages,
                "google_maps_cost": total_cost
            })

            # Phase 2: Facebook Email Enrichment
            # CRITICAL: This phase is optional - don't let it fail the entire campaign
            logging.info("\n" + "="*50)
            logging.info("PHASE 2: FACEBOOK EMAIL ENRICHMENT")
            logging.info("="*50)

            try:
                # Helper function to normalize Facebook URLs for matching
                def normalize_fb_url(url: str) -> str:
                    """Normalize Facebook URL to handle format variations"""
                    if not url:
                        return ""

                    # Convert to lowercase and strip whitespace
                    url = url.lower().strip()

                    # Ensure https:// prefix
                    if not url.startswith("http"):
                        url = f"https://{url}"

                    # Standardize to https://www.facebook.com format
                    url = url.replace("http://", "https://")
                    if "facebook.com" in url and "www.facebook.com" not in url:
                        url = url.replace("facebook.com", "www.facebook.com")

                    # Remove trailing slash
                    url = url.rstrip("/")

                    # Remove query params and fragments
                    if "?" in url:
                        url = url.split("?")[0]
                    if "#" in url:
                        url = url.split("#")[0]

                    return url

                # Get businesses that need enrichment
                businesses_to_enrich = self.db.get_businesses_for_enrichment(campaign_id, limit=500)

                if businesses_to_enrich:
                    logging.info(f"📘 Found {len(businesses_to_enrich)} businesses with Facebook pages")

                    # Extract Facebook URLs (database stores as facebook_url)
                    # CRITICAL FIX: Use sets for deduplication and dict of lists for mapping
                    unique_facebook_urls = set()
                    url_to_businesses = {}  # Changed to plural - maps URL to LIST of businesses

                    for business in businesses_to_enrich:
                        # The database field is facebook_url (saved from facebookUrl/facebook in Google Maps)
                        fb_url = business.get("facebook_url")
                        if fb_url:
                            # Clean up the URL if needed
                            if not fb_url.startswith("http"):
                                fb_url = f"https://{fb_url}" if not fb_url.startswith("www.") else f"https://www.{fb_url}"

                            # Store in dict with NORMALIZED url as key for reliable matching
                            normalized_url = normalize_fb_url(fb_url)

                            # Add to unique set for Apify batch (prevents "duplicate items" error)
                            unique_facebook_urls.add(normalized_url)

                            # Map URL to LIST of businesses (handles chains/duplicates)
                            if normalized_url not in url_to_businesses:
                                url_to_businesses[normalized_url] = []
                            url_to_businesses[normalized_url].append(business)

                            logging.debug(f"    Found Facebook URL: {fb_url} (normalized: {normalized_url})")

                    # Convert set to list for batch processing
                    facebook_urls = list(unique_facebook_urls)

                    # Log deduplication stats
                    total_businesses = len(businesses_to_enrich)
                    unique_urls = len(facebook_urls)
                    if total_businesses > unique_urls:
                        logging.info(f"📊 Deduplicated {total_businesses} businesses down to {unique_urls} unique URLs")
                        logging.info(f"   (Found {total_businesses - unique_urls} duplicate Facebook pages - e.g., chains)")
                    else:
                        logging.info(f"📊 All {unique_urls} Facebook URLs are unique")

                    if not facebook_urls:
                        logging.warning("⚠️ No valid Facebook URLs found in businesses marked for enrichment")
                        logging.info("Skipping Facebook enrichment phase")
                    else:
                        logging.info(f"📊 Extracted {len(facebook_urls)} Facebook URLs for enrichment")

                        # Run Facebook enrichment in batches
                        batch_size = 50
                        enriched_count = 0
                        new_emails_found = 0
                        facebook_verified_emails = 0

                        for i in range(0, len(facebook_urls), batch_size):
                            batch = facebook_urls[i:i + batch_size]
                            logging.info(f"\nEnriching batch {i//batch_size + 1} ({len(batch)} pages)")

                            enrichments = self.facebook_scraper.enrich_with_facebook(batch)

                            logging.info(f"  Received {len(enrichments)} enrichment results from Facebook scraper")

                            # Save enrichment results
                            for enrichment in enrichments:
                                fb_url = enrichment.get("facebook_url")

                                # Normalize the URL for matching
                                normalized_url = normalize_fb_url(fb_url)

                                # Debug: Show URL matching attempt
                                if normalized_url:
                                    logging.debug(f"  Matching URL: {fb_url} -> {normalized_url}")

                                if normalized_url and normalized_url in url_to_businesses:
                                    # CRITICAL FIX: Get ALL businesses that share this URL
                                    businesses_for_url = url_to_businesses[normalized_url]

                                    logging.info(f"  💾 Saving enrichment for {len(businesses_for_url)} business(es) sharing URL: {normalized_url}")

                                    # Apply enrichment to ALL businesses with this URL
                                    for business in businesses_for_url:
                                        logging.info(f"    → {business.get('name', 'Unknown')}")

                                        # Save to database - ALWAYS save, even if no email found
                                        # This creates a record showing we attempted enrichment
                                        success = self.db.save_facebook_enrichment(
                                            business_id=business["id"],
                                            campaign_id=campaign_id,
                                            enrichment_data=enrichment
                                        )

                                        if success:
                                            if enrichment.get("primary_email"):
                                                enriched_count += 1
                                                logging.info(f"      ✅ Found email: {enrichment['primary_email']}")
                                                if not business.get("email"):  # New email found
                                                    new_emails_found += 1

                                                # Verify Facebook email with Bouncer
                                                try:
                                                    email = enrichment["primary_email"]
                                                    verification = self.email_verifier.verify_email(email)

                                                    if verification.get("is_safe"):
                                                        facebook_verified_emails += 1
                                                        logging.info(f"      ✅ Verified: {email}")
                                                    else:
                                                        logging.debug(f"      ⚠️  Email risky/undeliverable: {email}")

                                                    # Save verification
                                                    self.db.update_facebook_verification(
                                                        business_id=business["id"],
                                                        verification_data=verification
                                                    )
                                                except Exception as e:
                                                    logging.warning(f"      Failed to verify email {email}: {e}")
                                            else:
                                                logging.debug(f"      ⚠️  No email found (but enrichment saved)")
                                        else:
                                            logging.warning(f"      ❌ Failed to save enrichment to database")
                                else:
                                    # URL mismatch - should be rare now with proper deduplication
                                    logging.warning(f"  ⚠️  URL mismatch: {fb_url} (normalized: {normalized_url}) not found in business mapping")
                                    logging.warning(f"     Available normalized URLs: {list(url_to_businesses.keys())[:5]}")

                        logging.info(f"\n✅ Enriched {enriched_count} Facebook pages")
                        logging.info(f"📧 Found {new_emails_found} new emails")
                        logging.info(f"✅ Verified {facebook_verified_emails} Facebook emails")

                        # Track Facebook costs using centralized pricing
                        facebook_cost = get_service_cost("facebook", len(facebook_urls))
                        self.db.track_api_cost(
                            campaign_id=campaign_id,
                            service="facebook",
                            items=len(facebook_urls),
                            cost_usd=facebook_cost
                        )

                        total_cost += facebook_cost

                        # CRITICAL FIX: Query database for actual email count after Facebook enrichment
                        # Don't rely on local counter which may be inaccurate due to deduplication
                        actual_email_count = self._count_businesses_with_emails(campaign_id)
                        if actual_email_count is not None:
                            total_emails = actual_email_count
                            logging.info(f"💾 Updated email count from database: {total_emails}")
                else:
                    logging.info("No businesses need Facebook enrichment")

            except Exception as e:
                logging.error(f"Facebook enrichment phase failed: {e}")
                logging.error("Continuing with campaign completion despite Facebook failure")
                # Don't fail the entire campaign - just log and continue

            # Phase 2.5: LinkedIn Enrichment (PARALLEL)
            # CRITICAL: This phase is optional - don't let it fail the entire campaign
            logging.info("\n" + "="*50)
            logging.info("PHASE 2.5: LINKEDIN ENRICHMENT (PARALLEL)")
            logging.info("="*50)

            try:
                # Get all businesses for LinkedIn enrichment (even those with emails)
                all_businesses = self.db.get_all_businesses(campaign_id, limit=500)

                if all_businesses:
                    logging.info(f"🔗 Processing {len(all_businesses)} businesses for LinkedIn enrichment")

                    # Initialize counters
                    linkedin_profiles_found = 0
                    verified_emails = 0
                    new_contacts_found = 0

                    try:
                        # PARALLEL PROCESSING: All businesses processed at once with batch_size=15, max_parallel=3
                        # This processes 3 batches of 15 businesses simultaneously
                        linkedin_results = self.linkedin_scraper.enrich_with_linkedin_parallel(
                            businesses=all_businesses,
                            max_businesses=len(all_businesses),
                            batch_size=15,
                            max_parallel=3
                        )

                        logging.info(f"\n📊 Processing {len(linkedin_results)} LinkedIn enrichment results...")

                        # Process and save ALL results
                        for enrichment in linkedin_results:
                            business_id = enrichment.get('business_id')

                            if enrichment.get('linkedin_found'):
                                linkedin_profiles_found += 1

                                # Save LinkedIn enrichment to database
                                try:
                                    success = self.db.save_linkedin_enrichment(
                                        business_id=business_id,
                                        campaign_id=campaign_id,
                                        enrichment_data=enrichment
                                    )

                                    if success:
                                        logging.debug(f"  ✅ Saved LinkedIn enrichment for business {business_id}")
                                    else:
                                        logging.warning(f"  ⚠️  Failed to save LinkedIn enrichment for business {business_id}")

                                except Exception as e:
                                    logging.warning(f"Failed to save LinkedIn enrichment for business {business_id}: {e}")
                                    continue

                                # If we have emails to verify
                                if enrichment.get('primary_email'):
                                    new_contacts_found += 1

                                    try:
                                        # Verify email with Bouncer
                                        email = enrichment['primary_email']
                                        verification = self.email_verifier.verify_email(email)

                                        if verification.get('is_safe'):
                                            verified_emails += 1
                                            logging.info(f"  ✅ Verified email: {email}")
                                        else:
                                            logging.debug(f"  ⚠️  Email verification failed/risky: {email}")

                                        # Update LinkedIn enrichment with verification results
                                        self.db.update_linkedin_verification(
                                            business_id=business_id,
                                            verification_data=verification
                                        )
                                    except Exception as e:
                                        logging.warning(f"Failed to verify email {email}: {e}")
                                        continue
                            else:
                                # No LinkedIn profile found - still save the record to track we attempted enrichment
                                try:
                                    self.db.save_linkedin_enrichment(
                                        business_id=business_id,
                                        campaign_id=campaign_id,
                                        enrichment_data=enrichment
                                    )
                                    logging.debug(f"  📝 Saved 'not found' record for business {business_id}")
                                except Exception as e:
                                    logging.warning(f"Failed to save 'not found' record for business {business_id}: {e}")

                        logging.info(f"\n✅ LinkedIn Enrichment Results:")
                        logging.info(f"  🔗 LinkedIn profiles found: {linkedin_profiles_found}")
                        logging.info(f"  👤 New contacts found: {new_contacts_found}")
                        logging.info(f"  ✅ Verified emails: {verified_emails}")

                        # Track LinkedIn costs using centralized pricing
                        linkedin_cost = get_service_cost("linkedin", len(all_businesses))
                        bouncer_cost = get_service_cost("bouncer", new_contacts_found)

                        try:
                            self.db.track_api_cost(
                                campaign_id=campaign_id,
                                service="linkedin",
                                items=len(all_businesses),
                                cost_usd=linkedin_cost
                            )

                            self.db.track_api_cost(
                                campaign_id=campaign_id,
                                service="bouncer",
                                items=new_contacts_found,
                                cost_usd=bouncer_cost
                            )
                        except Exception as e:
                            logging.warning(f"Failed to track LinkedIn/Bouncer costs: {e}")

                        total_cost += linkedin_cost + bouncer_cost

                        # CRITICAL FIX: Query database for actual email count after LinkedIn enrichment
                        # Don't rely on local counter which may miss emails from previous phases
                        actual_email_count = self._count_businesses_with_emails(campaign_id)
                        if actual_email_count is not None:
                            total_emails = actual_email_count
                            logging.info(f"💾 Updated email count from database: {total_emails}")

                    except Exception as e:
                        logging.error(f"Error in parallel LinkedIn enrichment: {e}")
                        logging.error("Continuing with campaign completion despite LinkedIn failure")
                        # Don't fail the entire campaign - just log and continue

                else:
                    logging.info("No businesses available for LinkedIn enrichment")

            except Exception as e:
                logging.error(f"LinkedIn enrichment phase failed: {e}")
                logging.error("Continuing with campaign completion despite LinkedIn failure")
                # Don't fail the entire campaign - just log and continue

            # Phase 3: Icebreaker Generation
            # CRITICAL: This phase is optional - don't let it fail the entire campaign
            logging.info("\n" + "="*50)
            logging.info("PHASE 3: ICEBREAKER GENERATION")
            logging.info("="*50)

            if self.ai_processor:
                try:
                    # Fetch organization data for personalized icebreakers
                    organization_data = None
                    if campaign.get('organization_id'):
                        try:
                            org_result = self.db.client.table("organizations")\
                                .select("product_name, product_description, value_proposition, target_audience, messaging_tone, industry, company_mission, core_values, company_story")\
                                .eq("id", campaign['organization_id'])\
                                .single()\
                                .execute()
                            if org_result.data:
                                organization_data = org_result.data
                                logging.info(f"📋 Using organization product info: {organization_data.get('product_name', 'N/A')}")
                        except Exception as e:
                            logging.warning(f"Could not fetch organization data: {e}")

                    # Fetch product target_categories for perfect-fit matching
                    if campaign.get('product_id'):
                        try:
                            product_result = self.db.client.table("products")\
                                .select("target_categories, name, description, value_proposition")\
                                .eq("id", campaign['product_id'])\
                                .single()\
                                .execute()
                            if product_result.data and organization_data:
                                # Add product target_categories to organization_data for ai_processor
                                organization_data['target_categories'] = product_result.data.get('target_categories', [])
                                # Override with product-specific data if available
                                if product_result.data.get('name'):
                                    organization_data['product_name'] = product_result.data['name']
                                if product_result.data.get('description'):
                                    organization_data['product_description'] = product_result.data['description']
                                if product_result.data.get('value_proposition'):
                                    organization_data['value_proposition'] = product_result.data['value_proposition']
                                logging.info(f"📦 Using product target_categories: {organization_data.get('target_categories', [])}")
                        except Exception as e:
                            logging.warning(f"Could not fetch product data: {e}")

                    # Get all businesses with emails for icebreaker generation
                    # CRITICAL: Include ALL fields needed for personalized subject lines
                    businesses_with_emails = self.db.client.table("gmaps_businesses")\
                        .select("id, name, website, email, email_source, category, city, state, rating, reviews_count, description")\
                        .eq("campaign_id", campaign_id)\
                        .not_.is_("email", "null")\
                        .execute()

                    if businesses_with_emails.data:
                        logging.info(f"🤖 Generating icebreakers for {len(businesses_with_emails.data)} businesses...")

                        # Import web scraper for website content
                        from .web_scraper import WebScraper
                        web_scraper = WebScraper()

                        # PARALLEL ICEBREAKER GENERATION
                        # Using ThreadPoolExecutor for ~4x speedup (tested with 5 workers)
                        total_businesses = len(businesses_with_emails.data)
                        icebreakers_generated = 0
                        start_time = time.time()

                        logging.info(f"🚀 Starting PARALLEL icebreaker generation with {self.ICEBREAKER_MAX_WORKERS} workers...")

                        with ThreadPoolExecutor(max_workers=self.ICEBREAKER_MAX_WORKERS) as executor:
                            # Submit all jobs
                            future_to_business = {
                                executor.submit(
                                    self._process_single_icebreaker,
                                    business,
                                    campaign,
                                    organization_data,
                                    web_scraper,
                                    idx,
                                    total_businesses
                                ): (idx, business)
                                for idx, business in enumerate(businesses_with_emails.data, 1)
                            }

                            # Collect results as they complete
                            for future in as_completed(future_to_business):
                                idx, business = future_to_business[future]
                                try:
                                    success, business_name = future.result()
                                    if success:
                                        with self._icebreaker_lock:
                                            icebreakers_generated += 1
                                except Exception as e:
                                    logging.warning(f"    ⚠️ Future failed for business {idx}: {e}")

                                # Clean up to prevent memory leaks
                                del future_to_business[future]

                        elapsed_time = time.time() - start_time
                        avg_time_per_business = elapsed_time / total_businesses if total_businesses > 0 else 0

                        logging.info(f"\n✅ Icebreaker Generation Complete (PARALLEL):")
                        logging.info(f"  🤖 Generated {icebreakers_generated}/{total_businesses} icebreakers")
                        logging.info(f"  ⏱️  Total time: {elapsed_time:.1f}s ({avg_time_per_business:.1f}s avg per business)")
                        logging.info(f"  🚀 Speedup: ~{self.ICEBREAKER_MAX_WORKERS}x vs sequential")

                    else:
                        logging.info("No businesses with emails found for icebreaker generation")

                except Exception as e:
                    logging.error(f"Icebreaker generation phase failed: {e}")
                    logging.error("Continuing with campaign completion despite icebreaker failure")
                    # Don't fail the entire campaign - just log and continue
            else:
                logging.info("⚠️  Skipping icebreaker generation - AI Processor not initialized")

            # Update campaign with final results
            self.db.update_campaign(campaign_id, {
                "status": "completed",
                "completed_at": datetime.now().isoformat(),
                "actual_cost": total_cost,
                "total_businesses_found": total_businesses,
                "total_emails_found": total_emails,
                "total_facebook_pages_found": total_facebook_pages
            })

            # Refresh master leads database to include new data
            logging.info("🔄 Refreshing master leads database...")
            self.db.refresh_master_leads()

            # Generate summary
            summary = {
                "campaign_id": campaign_id,
                "campaign_name": campaign["name"],
                "status": "completed",
                "zip_codes_scraped": len(coverage),
                "total_businesses": total_businesses,
                "total_emails": total_emails,
                "email_success_rate": round((total_emails / total_businesses * 100), 1) if total_businesses > 0 else 0,
                "total_facebook_pages": total_facebook_pages,
                "total_cost": round(total_cost, 2),
                "cost_per_business": round(total_cost / total_businesses, 4) if total_businesses > 0 else 0,
                "cost_per_email": round(total_cost / total_emails, 2) if total_emails > 0 else 0,
                "duration_minutes": self._calculate_duration(campaign)
            }
            
            logging.info("\n" + "="*70)
            logging.info("📊 CAMPAIGN COMPLETE - SUMMARY")
            logging.info("="*70)
            for key, value in summary.items():
                if key != "campaign_id":
                    logging.info(f"{key}: {value}")
            
            return summary

        except Exception as e:
            logging.error(f"Error executing campaign: {e}")

            # Update campaign status to failed
            self.db.update_campaign(campaign_id, {
                "status": "failed",
                "completed_at": datetime.now().isoformat()
            })

            return {"error": str(e)}
        finally:
            # Stop heartbeat monitoring
            self._stop_heartbeat()

    def _execute_phase_1_google_maps(self, campaign, coverage, max_businesses_per_zip):
        """
        Phase 1: Google Maps Scraping (extracted for timeout wrapping)
        Returns dict with phase results: total_businesses, total_emails, total_facebook_pages, total_cost
        """
        logging.info("\n" + "="*50)
        logging.info("PHASE 1: GOOGLE MAPS SCRAPING")
        logging.info("="*50)

        total_businesses = 0
        total_emails = 0
        total_facebook_pages = 0
        total_cost = 0
        gmaps_verified_emails = 0

        for idx, zip_coverage in enumerate(coverage, 1):
            zip_code = zip_coverage["zip_code"]
            keywords = zip_coverage.get("keywords", campaign.get("keywords", []))

            logging.info(f"\n[{idx}/{len(coverage)}] Scraping ZIP: {zip_code}")

            # Scrape Google Maps for this ZIP
            businesses = self._scrape_zip_code(
                zip_code=zip_code,
                keywords=keywords,
                max_results=max_businesses_per_zip
            )

            if businesses:
                # Save businesses to database
                saved_count = self.db.save_businesses(businesses, self.campaign_id, zip_code)

                # CRITICAL FIX: Query actual count from database instead of trusting return value
                # This ensures we count the real deduplicated businesses saved
                actual_count_result = self.db.client.table("gmaps_businesses")\
                    .select("id", count="exact")\
                    .eq("campaign_id", self.campaign_id)\
                    .eq("zip_code", zip_code)\
                    .execute()
                saved_count = actual_count_result.count if actual_count_result.count is not None else saved_count

                # Get saved business IDs for verification
                saved_businesses = self.db.client.table("gmaps_businesses")\
                    .select("id, email")\
                    .eq("campaign_id", self.campaign_id)\
                    .eq("zip_code", zip_code)\
                    .eq("email_source", "google_maps")\
                    .not_.is_("email", "null")\
                    .execute()

                # Verify Google Maps emails
                if saved_businesses.data:
                    logging.info(f"   🔍 Verifying {len(saved_businesses.data)} Google Maps emails...")
                    for business in saved_businesses.data:
                        try:
                            email = business.get("email")
                            if email:
                                verification = self.email_verifier.verify_email(email)

                                if verification.get("is_safe"):
                                    gmaps_verified_emails += 1

                                # Save verification
                                self.db.update_google_maps_verification(
                                    business_id=business["id"],
                                    verification_data=verification
                                )
                        except Exception as e:
                            logging.warning(f"   Failed to verify email {email}: {e}")
                            continue

                # Count Facebook pages - check multiple fields where Facebook URLs might be
                facebook_count = sum(1 for b in businesses if (
                    b.get("facebooks") or
                    b.get("facebookUrl") or
                    b.get("facebook") or
                    (b.get("website") and "facebook.com" in str(b.get("website", "")).lower())
                ))
                email_count = sum(1 for b in businesses if b.get("email") or b.get("directEmails"))

                # Calculate cost for this ZIP using centralized pricing
                zip_cost = get_service_cost("google_maps", len(businesses))

                # Update coverage status
                self.db.update_coverage_status(
                    campaign_id=self.campaign_id,
                    zip_code=zip_code,
                    businesses_found=saved_count,
                    emails_found=email_count,
                    actual_cost=zip_cost
                )

                # Update totals
                total_businesses += saved_count
                total_emails += email_count
                total_facebook_pages += facebook_count
                total_cost += zip_cost

                logging.info(f"   ✅ Found {saved_count} businesses")
                logging.info(f"   📧 {email_count} have emails")
                logging.info(f"   ✅ {gmaps_verified_emails} verified emails")
                logging.info(f"   📘 {facebook_count} have Facebook pages")

                # Update ZIP code stats
                self.db.update_zip_code_stats(zip_code, saved_count)
            else:
                logging.warning(f"   ❌ No businesses found")

            # Rate limiting between ZIPs
            if idx < len(coverage):
                time.sleep(2)

        logging.info(f"\n📊 Phase 1 Summary:")
        logging.info(f"   Total businesses: {total_businesses}")
        logging.info(f"   Google Maps emails verified: {gmaps_verified_emails}")

        # Track Google Maps costs
        self.db.track_api_cost(
            campaign_id=self.campaign_id,
            service="google_maps",
            items=total_businesses,
            cost_usd=total_cost
        )

        # Save Phase 1 results
        logging.info(f"\n💾 Saving Phase 1 results: {total_businesses} businesses found")
        self.db.update_campaign(self.campaign_id, {
            "total_businesses_found": total_businesses,
            "total_emails_found": total_emails,
            "total_facebook_pages_found": total_facebook_pages,
            "google_maps_cost": total_cost
        })

        return {
            "total_businesses": total_businesses,
            "total_emails": total_emails,
            "total_facebook_pages": total_facebook_pages,
            "total_cost": total_cost
        }

    def _scrape_zip_code(self, zip_code: str, keywords: List[str], max_results: int) -> List[Dict[str, Any]]:
        """Scrape a single ZIP code for all keywords"""
        all_businesses = []
        seen_place_ids = set()
        
        for keyword in keywords:
            try:
                # Create search query for this ZIP
                search_query = f"{keyword} {zip_code}"
                
                logging.info(f"   Searching: {search_query}")
                
                # Use the existing Google Maps scraper
                businesses = self.google_scraper._scrape_google_maps(
                    search_query=keyword,
                    location=zip_code,
                    max_results=max_results // len(keywords)  # Divide limit by number of keywords
                )
                
                # Deduplicate by place_id
                for business in businesses:
                    place_id = business.get("placeId") or business.get("place_id")
                    if place_id and place_id not in seen_place_ids:
                        seen_place_ids.add(place_id)
                        all_businesses.append(business)
                
            except Exception as e:
                logging.error(f"   Error scraping {keyword} in {zip_code}: {e}")
                continue
        
        return all_businesses

    def _scrape_zip_codes_batched(self, zip_codes: List[str], keywords: List[str], max_results: int) -> Dict[str, List[Dict[str, Any]]]:
        """
        Scrape multiple ZIP codes in a single batched call for all keywords.
        Returns dict mapping ZIP codes to their businesses.

        Args:
            zip_codes: List of ZIP codes to scrape (max 10)
            keywords: List of keywords to search
            max_results: Max results per ZIP per keyword

        Returns:
            Dict mapping ZIP code -> list of businesses
        """
        businesses_by_zip = {}
        seen_place_ids_by_zip = {zip_code: set() for zip_code in zip_codes}

        for keyword in keywords:
            try:
                logging.info(f"   Batching {len(zip_codes)} ZIP codes for keyword: {keyword}")

                # Call scraper with list of ZIPs (batched mode)
                businesses = self.google_scraper._scrape_google_maps(
                    search_query=keyword,
                    location=zip_codes,  # List of ZIPs triggers batched mode
                    max_results=max_results // len(keywords)
                )

                # Group businesses by their extracted ZIP code
                for business in businesses:
                    extracted_zip = business.get('extracted_zip', 'UNKNOWN')

                    if extracted_zip == 'UNKNOWN':
                        logging.warning(f"   Business has no extracted ZIP: {business.get('title', 'Unknown')}")
                        continue

                    # Initialize list for this ZIP if needed
                    if extracted_zip not in businesses_by_zip:
                        businesses_by_zip[extracted_zip] = []

                    # Deduplicate by place_id
                    place_id = business.get("placeId") or business.get("place_id")
                    if place_id and place_id not in seen_place_ids_by_zip[extracted_zip]:
                        seen_place_ids_by_zip[extracted_zip].add(place_id)
                        businesses_by_zip[extracted_zip].append(business)

            except Exception as e:
                logging.error(f"   Error in batched scraping for {keyword}: {e}")
                continue

        return businesses_by_zip

    def _calculate_coverage_percentage(self, zip_count: int, profile: str) -> float:
        """Calculate coverage percentage based on profile"""
        profile_percentages = {
            "aggressive": 99.0,
            "balanced": 94.0,
            "budget": 85.0,
            "custom": 0.0  # Will be calculated based on actual selection
        }
        return profile_percentages.get(profile, 0.0)
    
    def _calculate_duration(self, campaign: Dict[str, Any]) -> float:
        """Calculate campaign duration in minutes"""
        try:
            if campaign.get("started_at") and campaign.get("completed_at"):
                start = datetime.fromisoformat(campaign["started_at"].replace("Z", "+00:00"))
                end = datetime.fromisoformat(campaign["completed_at"].replace("Z", "+00:00"))
                duration = (end - start).total_seconds() / 60
                return round(duration, 1)
        except:
            pass
        return 0.0
    
    def get_campaign_status(self, campaign_id: str) -> Dict[str, Any]:
        """Get current status and analytics for a campaign"""
        return self.db.get_campaign_analytics(campaign_id)
    
    def pause_campaign(self, campaign_id: str) -> bool:
        """Pause a running campaign"""
        return self.db.update_campaign(campaign_id, {"status": "paused"})
    
    def resume_campaign(self, campaign_id: str) -> bool:
        """Resume a paused campaign"""
        return self.db.update_campaign(campaign_id, {"status": "running"})

    def _count_businesses_with_emails(self, campaign_id: str) -> int:
        """
        Count the ACTUAL number of businesses with emails from ANY source.

        This queries the database to count unique businesses that have:
        - Direct email in gmaps_businesses table
        - Email from Facebook enrichment
        - Email from LinkedIn enrichment

        Returns:
            int: Count of businesses with at least one email, or None on error
        """
        try:
            # Get all businesses for this campaign
            businesses = self.db.get_all_businesses(campaign_id, limit=10000)

            # Track which business IDs have emails
            business_ids_with_emails = set()

            # Check for direct emails
            for biz in businesses:
                if biz.get('email'):
                    business_ids_with_emails.add(biz['id'])

            # Check Facebook enrichments
            fb_enrichments = self.db.client.table('gmaps_facebook_enrichments')\
                .select('business_id, primary_email')\
                .eq('campaign_id', campaign_id)\
                .execute()

            for fb in fb_enrichments.data:
                if fb.get('primary_email'):
                    business_ids_with_emails.add(fb['business_id'])

            # Check LinkedIn enrichments
            li_enrichments = self.db.client.table('gmaps_linkedin_enrichments')\
                .select('business_id, primary_email')\
                .eq('campaign_id', campaign_id)\
                .execute()

            for li in li_enrichments.data:
                if li.get('primary_email'):
                    business_ids_with_emails.add(li['business_id'])

            return len(business_ids_with_emails)

        except Exception as e:
            logging.error(f"Error counting emails from database: {e}")
            return None

# Example usage
if __name__ == "__main__":
    from config import APIFY_API_KEY, OPENAI_API_KEY
    
    # Initialize manager
    manager = GmapsCampaignManager(
        supabase_url="https://your-project.supabase.co",
        supabase_key="your-key",
        apify_key=APIFY_API_KEY,
        openai_key=OPENAI_API_KEY
    )
    
    # Create a campaign
    campaign = manager.create_campaign(
        name="LA Restaurants Campaign",
        location="Los Angeles, CA",
        keywords=["restaurants", "cafes", "bakeries"],
        coverage_profile="budget"  # Start with budget profile
    )
    
    print(f"Created campaign: {campaign['campaign_id']}")
    print(f"Estimated cost: ${campaign['estimated_cost']}")
    print(f"Estimated emails: {campaign['estimated_emails']}")
    
    # Execute the campaign
    # results = manager.execute_campaign(campaign['campaign_id'])
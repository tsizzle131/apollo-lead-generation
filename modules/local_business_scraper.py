"""
Local Business Scraper Module
Uses Google Maps and LinkedIn to find local businesses and their decision makers
Outputs in the same format as Apollo scraper for pipeline compatibility
"""

import requests
import logging
import time
import os
from typing import List, Dict, Any, Optional
from urllib.parse import quote, urlparse
from config import APIFY_API_KEY, MAX_RETRIES, REQUEST_TIMEOUT

class LocalBusinessScraper:
    def __init__(self, api_key: str = APIFY_API_KEY, ai_processor = None):
        self.api_key = api_key
        self.base_url = "https://api.apify.com/v2"

        # Actor IDs
        self.google_maps_actor = "nwua9Gu5YrADL7ZDj"  # Google Maps Scraper

        # Import web scraper for website enrichment
        from .web_scraper import WebScraper
        self.web_scraper = WebScraper()

        # AI processor for icebreaker generation
        self.ai_processor = ai_processor
        
    def scrape_local_businesses(self, search_query: str, location: str, max_results: int = 1000) -> List[Dict[str, Any]]:
        """
        Scrape local businesses from Google Maps and enrich with LinkedIn
        
        Args:
            search_query: Business type to search (e.g., "restaurants", "hair salons")
            location: Location to search in (e.g., "Austin, TX")
            max_results: Maximum number of businesses to fetch
            
        Returns:
            List of contact dictionaries in Apollo format
        """
        try:
            # Advanced logging - Operation start
            logging.info("="*70)
            logging.info("🚀 LOCAL BUSINESS SCRAPER - OPERATION START")
            logging.info("="*70)
            logging.info(f"📋 Configuration:")
            logging.info(f"  • Search Query: '{search_query}'")
            logging.info(f"  • Location: '{location}'")
            logging.info(f"  • Max Results: {max_results}")
            logging.info(f"  • Apify API Key: {'✅ Present' if self.api_key else '❌ Missing'}")
            logging.info(f"  • Web Scraper: {'✅ Available' if self.web_scraper else '❌ Not initialized'}")
            logging.info("")
            
            # Step 1: Get businesses from Google Maps
            logging.info("📍 STAGE 1: Google Maps Search")
            logging.info(f"  → Searching for '{search_query}' in '{location}'...")
            
            start_time = time.time()
            businesses = self._scrape_google_maps(search_query, location, max_results)
            search_duration = time.time() - start_time
            
            if not businesses:
                logging.error("❌ CRITICAL: No businesses found from Google Maps")
                logging.error(f"  Possible issues:")
                logging.error(f"  • Invalid location: '{location}'")
                logging.error(f"  • No results for: '{search_query}'")
                logging.error(f"  • Apify API issues or insufficient credits")
                return []
            
            logging.info(f"✅ Google Maps search completed in {search_duration:.2f}s")
            logging.info(f"📊 Results: {len(businesses)} businesses found")
            
            # Step 2: Process business data with website enrichment
            logging.info("")
            logging.info("🔄 STAGE 2: Contact Enrichment")
            logging.info(f"  → Processing {len(businesses)} businesses...")
            
            enriched_contacts = []
            stats = {
                'with_website': 0,
                'website_scraped': 0,
                'emails_found': 0,
                'emails_generated': 0,
                'owner_names_found': 0,
                'failed': 0
            }
            
            for idx, business in enumerate(businesses, 1):
                business_name = business.get('title') or business.get('name', 'Unknown')
                logging.info(f"\n  [{idx}/{len(businesses)}] Processing: {business_name}")
                
                # Track if business has website
                if business.get('website') or business.get('url'):
                    stats['with_website'] += 1
                
                # Enrich business data with website scraping
                enriched_contact = self._enrich_business_contact(business)
                
                # Only add contacts with verified emails (not guessed or social media)
                if enriched_contact and enriched_contact.get('email'):
                    email = enriched_contact['email']
                    email_status = enriched_contact.get('email_status', '')
                    
                    # Skip guessed emails to protect email health
                    if email_status == 'guessed':
                        logging.warning(f"    ❌ Skipping guessed email to protect deliverability: {email}")
                        stats['failed'] += 1
                        continue
                    
                    # Skip social media/generic emails
                    if any(domain in email.lower() for domain in [
                        '@google.com', '@facebook.com', '@instagram.com', 
                        '@twitter.com', '@linkedin.com', '@youtube.com',
                        '@uscompany.business', '@usmapic.org'
                    ]):
                        logging.warning(f"    ❌ Skipping contact with invalid email: {email}")
                        stats['failed'] += 1
                        continue
                    
                    # This is a verified email - add it
                    enriched_contacts.append(enriched_contact)
                else:
                    # No email found at all
                    if enriched_contact:
                        business_name = enriched_contact.get('name', 'Unknown')
                        logging.info(f"    ℹ️ No verified email found for {business_name} - not adding to campaign")
                        stats['failed'] += 1
                        
                        # Update statistics
                        if enriched_contact.get('email'):
                            if enriched_contact.get('email_status') == 'guessed':
                                stats['emails_generated'] += 1
                                logging.info(f"    ✅ Generated email: {enriched_contact['email']}")
                            else:
                                stats['emails_found'] += 1
                                logging.info(f"    ✅ Found email: {enriched_contact['email']}")
                        
                        if enriched_contact.get('_website_scraped'):
                            stats['website_scraped'] += 1
                            logging.info(f"    ✅ Website scraped successfully")
                        
                        if enriched_contact.get('_has_owner_name'):
                            stats['owner_names_found'] += 1
                            logging.info(f"    ✅ Owner name found: {enriched_contact.get('first_name')}")
                        
                        logging.info(f"    ✅ Contact created successfully")
                    else:
                        stats['failed'] += 1
                        logging.warning(f"    ⚠️ Failed to create contact for business")

                # Rate limiting handled by token bucket rate limiter
                # Removed redundant time.sleep(2) delay (token bucket handles rate limiting)

            # Final statistics
            logging.info("")
            logging.info("="*70)
            logging.info("📊 OPERATION COMPLETE - STATISTICS")
            logging.info("="*70)
            logging.info(f"✅ Total contacts created: {len(enriched_contacts)}/{len(businesses)}")
            logging.info(f"")
            logging.info(f"📈 Enrichment Statistics:")
            logging.info(f"  • Businesses with websites: {stats['with_website']}")
            logging.info(f"  • Websites successfully scraped: {stats['website_scraped']}")
            logging.info(f"  • Emails found from websites: {stats['emails_found']}")
            logging.info(f"  • Emails generated (smart defaults): {stats['emails_generated']}")
            logging.info(f"  • Owner/manager names found: {stats['owner_names_found']}")
            logging.info(f"  • Failed to create contact: {stats['failed']}")
            
            success_rate = (len(enriched_contacts) / len(businesses) * 100) if businesses else 0
            logging.info(f"")
            logging.info(f"🎯 Success Rate: {success_rate:.1f}%")
            logging.info("="*70)
            
            return enriched_contacts
            
        except Exception as e:
            logging.error(f"❌ Error in local business scraping: {e}")
            return []
    
    def scrape_local_businesses_raw(self, search_query: str, location: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """
        Scrape local businesses from Google Maps WITHOUT enrichment (for fast database storage)
        This is the fixed version that saves data immediately like Apollo flow
        
        Args:
            search_query: Business type to search (e.g., "restaurants", "hair salons")
            location: Location to search in (e.g., "Austin, TX", "23602")
            max_results: Maximum number of businesses to fetch
            
        Returns:
            List of raw contact dictionaries ready for database storage
        """
        try:
            logging.info("="*70)
            logging.info("🚀 LOCAL BUSINESS RAW SCRAPER - FAST MODE")
            logging.info("="*70)
            logging.info(f"📋 Configuration:")
            logging.info(f"  • Search Query: '{search_query}'")
            logging.info(f"  • Location: '{location}'")
            logging.info(f"  • Max Results: {max_results}")
            logging.info("")
            
            # Get businesses from Google Maps API
            logging.info("📍 Fetching businesses from Google Maps...")
            businesses = self._scrape_google_maps(search_query, location, max_results)
            
            if not businesses:
                logging.error("❌ No businesses found from Google Maps")
                return []
            
            logging.info(f"✅ Found {len(businesses)} businesses")
            
            # Convert to database-ready format WITHOUT enrichment
            raw_contacts = []
            for business in businesses:
                # Extract basic info from Google Maps data
                business_name = business.get('title') or business.get('name', '')
                if not business_name:
                    continue
                
                # Get email if directly available from Google Maps
                email = None
                email_status = None
                
                # Check for email in Google Maps data (prioritize 'emails' array which Apify actually returns)
                email_fields = ['emails', 'directEmails', 'email', 'contactEmail', 'businessEmail']
                for field in email_fields:
                    field_value = business.get(field)
                    if field_value:
                        if isinstance(field_value, list) and len(field_value) > 0:
                            email = field_value[0]
                            email_status = 'verified'
                            break
                        elif isinstance(field_value, str) and '@' in field_value:
                            email = field_value
                            email_status = 'verified'
                            break
                
                # Create raw contact in Apollo-like format for database
                raw_contact = {
                    # Use Google place_id as unique identifier
                    'id': business.get('placeId') or business.get('place_id', ''),
                    'apollo_id': business.get('placeId') or business.get('place_id', ''),
                    
                    # Names - using business name as contact
                    'first_name': business_name,
                    'last_name': 'Business Contact',
                    'name': f"{business_name} Business Contact",
                    
                    # Contact info
                    'email': email,
                    'email_status': email_status,
                    
                    # Professional info
                    'title': 'Business Contact',
                    'headline': f"Business Contact at {business_name}",
                    
                    # Organization info - store website for later enrichment
                    'website_url': business.get('website') or business.get('url', ''),
                    
                    # Store full business data for later enrichment
                    'raw_data_json': business,
                    
                    # Mark as needing enrichment
                    'processed': False,
                    '_source': 'google_maps_raw',
                    '_needs_enrichment': True
                }
                
                raw_contacts.append(raw_contact)
            
            logging.info(f"✅ Prepared {len(raw_contacts)} contacts for immediate database storage")
            logging.info("📊 These will be enriched in Stage 2 (after database save)")
            
            return raw_contacts
            
        except Exception as e:
            logging.error(f"❌ Error in raw local business scraping: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _scrape_google_maps(self, search_query: str, location: str, max_results: int) -> List[Dict[str, Any]]:
        """Scrape businesses from Google Maps using Apify
        
        Args:
            search_query: What to search for (e.g., "salons", "dentists")
            location: Can be:
                - Zip code: "23602"
                - City, State: "Newport News, VA"
                - Address: "123 Main St, Newport News, VA"
                - Coordinates: "37.0871,-76.4730"
            max_results: Maximum number of results to return
        """
        try:
            endpoint = f"{self.base_url}/acts/{self.google_maps_actor}/runs"
            
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            # Build payload matching the exact Apify actor schema
            payload = {
                "searchStringsArray": [],  # Will be populated based on location type
                "maxCrawledPlacesPerSearch": max_results,
                "language": "en",
                "exportPlaceUrls": False,
                "saveHtml": False,
                "saveScreenshots": False,
                # Email and contact enrichment
                "scrapeDirectEmails": True,  # Get direct emails if available
                "scrapeWebsiteDetails": True,  # Enable to get social media links from websites
                "skipPlacesWithoutWebsite": False,  # Include all businesses even without websites
                # Filtering options
                "searchMatching": "all",
                "placeMinimumStars": "",
                "website": "allPlaces",
                "skipClosedPlaces": False,
                # Additional data options
                "scrapePlaceDetailPage": True,  # Enable to get more details including social links
                "scrapeTableReservationProvider": False,
                "includeWebResults": False,
                "scrapeDirectories": False,
                "maxQuestions": 0,
                "scrapeContacts": True,  # Enable to get social media contacts
                "maximumLeadsEnrichmentRecords": 0,  # We handle lead enrichment ourselves
                # Reviews settings
                "maxReviews": 0,  # We don't need reviews for lead gen
                "reviewsSort": "newest",
                "reviewsFilterString": "",
                "reviewsOrigin": "all",
                "scrapeReviewsPersonalData": True,
                "scrapeImageAuthors": False,
                "allPlacesNoSearchAction": "",
                # Proxy config
                "proxyConfig": {
                    "useApifyProxy": True
                }
            }
            
            # Build search strings based on input
            if location.lower() in ['usa', 'united states', 'us']:
                # Search entire USA by state
                states = [
                    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
                    'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
                    'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
                    'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 
                    'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
                    'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
                    'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
                    'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
                    'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
                    'West Virginia', 'Wisconsin', 'Wyoming'
                ]
                search_strings = [f"{search_query} {state}" for state in states]
                payload["searchStringsArray"] = search_strings
                logging.info(f"🇺🇸 Searching all 50 states for {search_query}")
            elif any(state in location.lower() for state in ['virginia', 'va', 'california', 'ca', 'texas', 'tx', 'florida', 'fl', 'new york', 'ny']):
                # State-level search
                search_term = f"{search_query} {location}"
                payload["searchStringsArray"] = [search_term]
                logging.info(f"🏛️ State-wide search: {search_term}")
            else:
                # Local search (zip code, city, or specific location)
                # Check if it's a zip code with radius
                import re
                zip_pattern = r'^\d{5}(-\d{4})?$'
                is_zip = re.match(zip_pattern, location.strip())
                
                # Simple search without complex grid logic
                search_term = f"{search_query} {location}"
                payload["searchStringsArray"] = [search_term]
                logging.info(f"📍 Local search: {search_term}")
            
            # Log the search details
            search_count = len(payload["searchStringsArray"])
            if search_count > 1:
                logging.info(f"🚀 Starting Google Maps scrape: {search_count} searches across multiple locations")
            elif search_count == 1:
                logging.info(f"🚀 Starting Google Maps scrape: {payload['searchStringsArray'][0]}")
            else:
                logging.error(f"❌ ERROR: searchStringsArray is EMPTY!")
                logging.error(f"   search_query: '{search_query}'")
                logging.error(f"   location: '{location}'")
                return []

            # Debug: Log full payload before sending
            logging.info(f"📤 Apify Payload Summary:")
            logging.info(f"   Actor: {self.google_maps_actor}")
            logging.info(f"   searchStringsArray: {payload['searchStringsArray']}")
            logging.info(f"   maxCrawledPlacesPerSearch: {payload['maxCrawledPlacesPerSearch']}")

            # CRITICAL DEBUG: Verify searchStringsArray is not empty
            if not payload.get('searchStringsArray'):
                logging.error("🚨 CRITICAL BUG: searchStringsArray is EMPTY!")
                logging.error(f"   search_query='{search_query}', location='{location}'")
                logging.error(f"   Full payload keys: {list(payload.keys())}")
                return []

            # Log FULL payload as JSON for debugging
            import json as json_lib
            logging.info(f"📋 Complete payload being sent:")
            logging.info(json_lib.dumps(payload, indent=2))

            # Start the actor run
            response = self._make_request_with_retry(
                endpoint,
                method="POST",
                headers=headers,
                json=payload
            )
            
            if not response or response.status_code not in [200, 201]:
                logging.error(f"❌ Failed to start Google Maps scrape: Status {response.status_code if response else 'No response'}")
                if response:
                    logging.error(f"Response body: {response.text[:500]}")  # First 500 chars
                    try:
                        error_data = response.json()
                        if 'error' in error_data:
                            logging.error(f"Apify Error: {error_data['error']}")
                    except:
                        pass
                return []
            
            run_data = response.json()
            run_id = run_data.get('data', {}).get('id')
            
            if not run_id:
                logging.error("❌ No run ID returned from Google Maps scraper")
                return []
            
            logging.info(f"⏳ Waiting for Google Maps scrape to complete...")
            
            # Wait for completion and get results
            results = self._wait_for_run_completion(run_id, headers, "Google Maps")
            logging.info(f"📊 Raw Google Maps results: {len(results)} items")
            return results
            
        except Exception as e:
            logging.error(f"❌ Google Maps scraping error: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _enrich_business_contact(self, business: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Enrich business data with website scraping and create a contact"""
        try:
            business_name = business.get('title') or business.get('name', '')
            website = business.get('website') or business.get('url', '')
            
            # Step 1: Try to enrich from website if available
            website_data = {}
            contact_email = None
            contact_name = None
            
            if website:
                logging.info(f"🌐 Scraping website for {business_name}: {website}")
                try:
                    # First, try to extract emails from raw HTML (before markdown conversion)
                    import re
                    import requests
                    from bs4 import BeautifulSoup
                    
                    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                    
                    # Skip social media and maps URLs
                    if not any(skip in website.lower() for skip in ['google.com/maps', 'facebook.com', 'instagram.com', 'twitter.com']):
                        try:
                            headers = {
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                            }
                            response = requests.get(website, headers=headers, timeout=30, allow_redirects=True)
                            if response.status_code == 200:
                                raw_html = response.text
                                soup = BeautifulSoup(raw_html, 'html.parser')
                                
                                # 1. Check mailto links first (most reliable)
                                mailto_links = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
                                for link in mailto_links:
                                    email = link.get('href', '').replace('mailto:', '').split('?')[0].strip()
                                    if '@' in email and '.' in email.split('@')[1]:
                                        # Accept even generic emails from mailto links as they're intentional
                                        contact_email = email
                                        logging.info(f"📧 Found email in mailto link: {contact_email}")
                                        break
                                
                                # 2. Look for emails in contact/footer areas
                                if not contact_email:
                                    # Search in elements likely to contain contact info
                                    contact_selectors = [
                                        soup.find_all(['footer', 'div', 'section'], class_=re.compile(r'contact|email|info|footer', re.I)),
                                        soup.find_all(['div', 'p', 'span'], string=re.compile(r'email|contact|reach|mail', re.I))
                                    ]
                                    
                                    for selector_results in contact_selectors:
                                        for element in selector_results[:5]:  # Check first 5 matches
                                            text = element.get_text() if element else ''
                                            emails_found = re.findall(email_pattern, text)
                                            for email in emails_found:
                                                if '@' in email and '.' in email.split('@')[1]:
                                                    contact_email = email
                                                    logging.info(f"📧 Found email in contact area: {contact_email}")
                                                    break
                                            if contact_email:
                                                break
                                        if contact_email:
                                            break
                                
                                # 3. Search entire page text as last resort
                                if not contact_email:
                                    all_text = soup.get_text()
                                    emails_found = re.findall(email_pattern, all_text)
                                    # Get unique emails
                                    unique_emails = list(set(emails_found))
                                    for email in unique_emails:
                                        if '@' in email and '.' in email.split('@')[1]:
                                            # Filter out common non-contact emails
                                            if not any(skip in email.lower() for skip in ['noreply', 'no-reply', 'donotreply', 'example.com']):
                                                contact_email = email
                                                logging.info(f"📧 Found email in page text: {contact_email}")
                                                break
                                
                        except Exception as e:
                            logging.debug(f"Could not extract email from raw HTML: {e}")
                    
                    # Now scrape website for content (for icebreaker generation)
                    scraped_data = self.web_scraper.scrape_website_content(website)
                    website_data = scraped_data
                    
                    logging.info(f"✅ Website scraped successfully for {business_name}")
                except Exception as e:
                    logging.warning(f"⚠️ Could not scrape website for {business_name}: {e}")
            
            # Step 2: Use Google Maps emails if no website email found
            if not contact_email:
                # Check multiple email fields from Google Maps
                email_fields = [
                    'directEmails',
                    'emails', 
                    'email',
                    'contactEmail',
                    'businessEmail'
                ]
                
                for field in email_fields:
                    field_value = business.get(field)
                    if field_value:
                        if isinstance(field_value, list) and len(field_value) > 0:
                            contact_email = field_value[0]
                            logging.info(f"📧 Found email from Google Maps field '{field}': {contact_email}")
                            break
                        elif isinstance(field_value, str) and '@' in field_value:
                            contact_email = field_value
                            logging.info(f"📧 Found email from Google Maps field '{field}': {contact_email}")
                            break
                
                # Also check additionalInfo field which sometimes has emails
                if not contact_email:
                    additional_info = business.get('additionalInfo', {})
                    if isinstance(additional_info, dict):
                        for key, value in additional_info.items():
                            if isinstance(value, str) and '@' in value and '.' in value.split('@')[1]:
                                contact_email = value
                                logging.info(f"📧 Found email in additionalInfo.{key}: {contact_email}")
                                break
            
            # Step 3: Skip email generation for Google Maps flow
            # We only want verified emails, not guessed ones that could hurt deliverability
            if not contact_email:
                logging.info(f"❌ No verified email found for {business_name} - skipping to protect email health")
                # Don't generate fake emails that will bounce
                # contact_email remains None
            
            # Step 4: Extract owner/manager info from Google Maps data
            # Try to find owner name from reviews or responses
            owner_name = self._extract_owner_from_reviews(business)

            # Step 5: Generate icebreaker if AI processor available and business has email
            icebreaker = None
            subject_line = None
            if self.ai_processor and contact_email:
                try:
                    logging.info(f"🤖 Generating icebreaker for {business_name}...")

                    # Prepare contact info for AI processor
                    contact_info = {
                        'first_name': owner_name if owner_name else business_name,
                        'last_name': 'Business Contact' if not owner_name else '',
                        'name': owner_name or business_name,
                        'email': contact_email,
                        'headline': business.get('category') or business.get('categoryName', ''),
                        'company_name': business_name,
                        'is_business_contact': not owner_name,
                        'organization': {
                            'name': business_name,
                            'category': business.get('category') or business.get('categoryName', ''),
                            'city': business.get('city', ''),
                            'state': business.get('state', ''),
                            'description': business.get('description', ''),
                            'rating': business.get('totalScore') or business.get('rating'),
                            'reviews_count': business.get('reviewsCount') or business.get('totalReviews')
                        }
                    }

                    # Generate icebreaker using website summaries
                    website_summaries = website_data.get('summaries', []) if website_data else []
                    icebreaker_result = self.ai_processor.generate_icebreaker(contact_info, website_summaries)

                    icebreaker = icebreaker_result.get('icebreaker')
                    subject_line = icebreaker_result.get('subject_line')

                    logging.info(f"✅ Generated icebreaker for {business_name}")

                except Exception as e:
                    logging.warning(f"⚠️ Could not generate icebreaker for {business_name}: {e}")
                    # Continue without icebreaker - don't fail the entire enrichment

            # Create the enriched contact
            return self._create_enriched_contact(business, contact_email, owner_name, website_data, icebreaker, subject_line)
            
        except Exception as e:
            logging.error(f"Error enriching business contact: {e}")
            return None
    
    def _extract_owner_from_reviews(self, business: Dict[str, Any]) -> Optional[str]:
        """Try to extract owner/manager name from review responses"""
        try:
            # Check if business has owner responses to reviews
            reviews = business.get('reviews', [])
            if reviews and isinstance(reviews, list):
                for review in reviews[:10]:  # Check first 10 reviews
                    if isinstance(review, dict):
                        response = review.get('response', {}) or review.get('ownerResponse', {})
                        if response:
                            # Owner responses often signed with name
                            response_text = response.get('text', '') if isinstance(response, dict) else str(response)
                            # Look for common sign-offs
                            import re
                            patterns = [
                                r'(?:Thanks|Thank you|Sincerely|Best|Regards),?\s+([A-Z][a-z]+ ?[A-Z]?[a-z]*)',
                                r'- ([A-Z][a-z]+ ?[A-Z]?[a-z]*)$',
                                r'^([A-Z][a-z]+ ?[A-Z]?[a-z]*),? Owner',
                                r'^([A-Z][a-z]+ ?[A-Z]?[a-z]*),? Manager',
                            ]
                            for pattern in patterns:
                                match = re.search(pattern, response_text, re.MULTILINE)
                                if match:
                                    name = match.group(1).strip()
                                    if len(name) > 2 and len(name) < 30:  # Reasonable name length
                                        logging.info(f"👤 Found potential owner name: {name}")
                                        return name
        except Exception as e:
            logging.debug(f"Could not extract owner from reviews: {e}")
        return None
    
    def _generate_smart_email(self, business: Dict[str, Any], website: str) -> str:
        """Generate a smart default email based on business type and domain"""
        try:
            from urllib.parse import urlparse
            domain = urlparse(website).netloc
            if domain:
                domain = domain.replace('www.', '')
                
                # Skip social media and generic domains
                social_media_domains = [
                    'facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com',
                    'youtube.com', 'google.com', 'yelp.com', 'tripadvisor.com',
                    'pinterest.com', 'tiktok.com', 'snapchat.com', 'reddit.com',
                    'm.facebook.com', 'business.facebook.com', 'maps.google.com',
                    'vagaro.com', 'glossgenius.com', 'booking.moego.pet', 
                    'uscompany.business', 'usmapic.org', 'google.co.uk'
                ]
                
                # Don't generate email for social media or generic domains
                if any(social in domain.lower() for social in social_media_domains):
                    logging.debug(f"❌ Skipping email generation for social media domain: {domain}")
                    return None
                
                # Get business category
                category = (business.get('category') or business.get('categoryName') or '').lower()
                
                # Smart email prefixes based on business type
                if any(word in category for word in ['restaurant', 'cafe', 'coffee', 'food', 'bar']):
                    prefix = 'manager'
                elif any(word in category for word in ['salon', 'spa', 'beauty', 'hair', 'nail']):
                    prefix = 'owner'
                elif any(word in category for word in ['medical', 'dental', 'clinic', 'doctor']):
                    prefix = 'office'
                elif any(word in category for word in ['store', 'shop', 'retail', 'boutique']):
                    prefix = 'sales'
                elif any(word in category for word in ['gym', 'fitness', 'yoga', 'pilates']):
                    prefix = 'info'
                else:
                    prefix = 'contact'
                
                email = f"{prefix}@{domain}"
                logging.info(f"📧 Generated smart email for real domain: {email}")
                return email
        except Exception as e:
            logging.debug(f"Could not generate smart email: {e}")
        return None
    
    def _create_enriched_contact(self, business: Dict[str, Any], email: str = None,
                                owner_name: str = None, website_data: Dict = None,
                                icebreaker: str = None, subject_line: str = None) -> Optional[Dict[str, Any]]:
        """Create an enriched contact from all available data sources"""
        try:
            # Handle different field names from the Google Maps actor
            business_name = business.get('title') or business.get('name', '')
            if not business_name:
                return None
            
            website = business.get('website') or business.get('url', '')
            
            # Extract location parts
            full_address = business.get('address') or business.get('fullAddress', '')
            city = business.get('city', '')
            state = business.get('state', '')
            
            # Parse city and state from address if not provided separately
            if not city and full_address:
                # Try to extract city and state from full address
                parts = full_address.split(',')
                if len(parts) >= 2:
                    city = parts[-2].strip()
                    state_zip = parts[-1].strip().split(' ')
                    if len(state_zip) >= 1:
                        state = state_zip[0]
            
            # Determine contact name
            if owner_name:
                name_parts = owner_name.split(' ', 1)
                first_name = name_parts[0] if len(name_parts) > 0 else owner_name
                last_name = name_parts[1] if len(name_parts) > 1 else ''
                full_name = owner_name
                title = "Owner" if "owner" in owner_name.lower() else "Manager"
            else:
                # Use business name with role
                first_name = business_name
                last_name = "(Business Contact)"
                full_name = f"{business_name} Business Contact"
                title = "Business Contact"
            
            # Determine email status - all found emails are verified, generated would be guessed
            if email:
                # All emails we actually found (not generated) are considered verified
                email_status = 'verified'
            else:
                email_status = ''
            
            # Extract business description and services
            description = business.get('description', '')
            category = business.get('category') or business.get('categoryName') or business.get('type', '')
            rating = business.get('totalScore') or business.get('rating') or business.get('averageRating')
            reviews_count = business.get('reviewsCount') or business.get('totalReviews')
            
            # Build enriched contact
            contact = {
                # Identification
                'id': business.get('placeId') or business.get('place_id', ''),
                'first_name': first_name,
                'last_name': last_name,
                'name': full_name,

                # Contact info
                'email': email or '',
                'email_status': email_status,

                # Professional info
                'title': title,
                'headline': f"{title} at {business_name}",
                'company_name': business_name,  # Add company name for better AI processing
                'is_business_contact': not owner_name,  # Flag for AI processor

                # Organization data - enhanced with more details
                'organization': {
                    'name': business_name,
                    'website_url': website,
                    'phone': business.get('phone') or business.get('phoneNumber', ''),
                    'address': full_address,
                    'city': city,
                    'state': state,
                    'category': category,
                    'description': description,
                    'rating': rating,
                    'reviews_count': reviews_count,
                    'price_level': business.get('price', ''),
                },

                # Website data for AI processing
                'website_url': website,
                'website_summaries': website_data.get('summaries', []) if website_data else [],

                # Icebreaker fields
                'icebreaker': icebreaker,
                'subject_line': subject_line,

                # Source tracking
                '_source': 'local_business_enriched',
                '_google_maps_data': business,
                '_website_scraped': bool(website_data),
                '_has_owner_name': bool(owner_name)
            }
            
            # Only return if we have some way to contact them
            if email or website:
                logging.info(f"✅ Created enriched contact for {business_name}")
                return contact
            else:
                logging.warning(f"⚠️ No contact method for {business_name}")
                return None
            
        except Exception as e:
            logging.warning(f"Error creating enriched contact: {e}")
            return None
    
    def _make_request_with_retry(self, url: str, method: str = "GET", **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                if method.upper() == "POST":
                    response = requests.post(url, timeout=REQUEST_TIMEOUT, **kwargs)
                else:
                    response = requests.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
                
                if response.status_code in [200, 201]:
                    return response
                elif response.status_code == 429:
                    wait_time = 2 ** attempt
                    logging.warning(f"Rate limited, waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.warning(f"Request failed with status {response.status_code}")
                    
            except requests.exceptions.Timeout:
                logging.warning(f"Request timeout, attempt {attempt + 1}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Request error: {e}")
            
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
        
        return None
    
    def _wait_for_run_completion(self, run_id: str, headers: dict, source: str) -> List[Dict[str, Any]]:
        """Wait for Apify run to complete and return results"""
        max_wait_time = 600  # 10 minutes max
        check_interval = 5   # Check every 5 seconds
        elapsed_time = 0
        
        # Use Google Maps actor (only actor we have now)
        actor_id = self.google_maps_actor
        
        while elapsed_time < max_wait_time:
            try:
                status_url = f"{self.base_url}/acts/{actor_id}/runs/{run_id}"
                status_response = self._make_request_with_retry(status_url, headers=headers)
                
                if not status_response:
                    logging.warning(f"⚠️ Failed to get {source} run status")
                    time.sleep(check_interval)
                    elapsed_time += check_interval
                    continue
                
                run_data = status_response.json()
                run_status = run_data.get('data', {}).get('status', 'UNKNOWN')
                
                # Use info logging for better visibility during execution
                if elapsed_time % 10 == 0 or run_status != 'RUNNING':
                    logging.info(f"🔄 {source} status: {run_status} ({elapsed_time}s elapsed)")
                
                if run_status == 'SUCCEEDED':
                    logging.info(f"✅ {source} scrape completed!")
                    
                    # Get dataset results
                    dataset_id = run_data.get('data', {}).get('defaultDatasetId')
                    if not dataset_id:
                        logging.error(f"❌ No dataset ID found for {source}")
                        return []
                    
                    dataset_url = f"{self.base_url}/datasets/{dataset_id}/items"
                    dataset_response = self._make_request_with_retry(dataset_url, headers=headers)
                    
                    if not dataset_response:
                        logging.error(f"❌ Failed to fetch {source} results")
                        return []
                    
                    results = dataset_response.json()
                    return results if isinstance(results, list) else []
                
                elif run_status == 'FAILED':
                    logging.error(f"❌ {source} scrape failed")
                    return []
                
                elif run_status in ['RUNNING', 'READY']:
                    time.sleep(check_interval)
                    elapsed_time += check_interval
                    
                    if elapsed_time % 30 == 0:
                        logging.info(f"⏳ Still waiting for {source}... ({elapsed_time}s elapsed)")
                
            except Exception as e:
                logging.error(f"❌ Error checking {source} status: {e}")
                return []
        
        logging.error(f"❌ {source} scrape timed out")
        return []
    
    def test_connection(self) -> bool:
        """Test if Apify API connection is working"""
        try:
            test_url = f"{self.base_url}/acts"
            headers = {"Authorization": f"Bearer {self.api_key}"}
            
            response = requests.get(test_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                logging.info("✅ Local Business Scraper API connection successful")
                return True
            else:
                logging.error(f"❌ API test failed: {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"❌ API test error: {e}")
            return False
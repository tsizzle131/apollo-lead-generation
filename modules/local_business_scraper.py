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
    def __init__(self, api_key: str = APIFY_API_KEY):
        self.api_key = api_key
        self.base_url = "https://api.apify.com/v2"
        
        # Actor IDs
        self.google_maps_actor = "nwua9Gu5YrADL7ZDj"  # Google Maps Scraper
        
        # Import web scraper for website enrichment
        from web_scraper import WebScraper
        self.web_scraper = WebScraper()
        
    def scrape_local_businesses(self, search_query: str, location: str, max_results: int = 100) -> List[Dict[str, Any]]:
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
            logging.info(f"🗺️ Starting local business search: {search_query} in {location}")
            
            # Step 1: Get businesses from Google Maps
            businesses = self._scrape_google_maps(search_query, location, max_results)
            if not businesses:
                logging.error("❌ No businesses found from Google Maps")
                return []
            
            logging.info(f"✅ Found {len(businesses)} businesses from Google Maps")
            
            # Step 2: Process business data with website enrichment
            enriched_contacts = []
            for idx, business in enumerate(businesses, 1):
                business_name = business.get('title') or business.get('name', 'Unknown')
                logging.info(f"🔍 Processing business {idx}/{len(businesses)}: {business_name}")
                
                # Enrich business data with website scraping
                enriched_contact = self._enrich_business_contact(business)
                if enriched_contact:
                    enriched_contacts.append(enriched_contact)
                else:
                    logging.warning(f"⚠️ Could not create contact for {business_name}")
                
                # Rate limiting between enrichments
                if idx < len(businesses):
                    time.sleep(2)  # Be respectful to APIs
            
            logging.info(f"✅ Created {len(enriched_contacts)} contacts from {len(businesses)} businesses")
            return enriched_contacts
            
        except Exception as e:
            logging.error(f"❌ Error in local business scraping: {e}")
            return []
    
    def _scrape_google_maps(self, search_query: str, location: str, max_results: int) -> List[Dict[str, Any]]:
        """Scrape businesses from Google Maps using Apify"""
        try:
            endpoint = f"{self.base_url}/acts/{self.google_maps_actor}/runs"
            
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            # Construct search term
            search_term = f"{search_query} {location}"
            
            # Payload format for nwua9Gu5YrADL7ZDj actor with lead enrichment
            payload = {
                "searchStringsArray": [search_term],  # Required field name
                "maxCrawledPlacesPerSearch": max_results,
                "language": "en",
                "exportPlaceUrls": False,
                "saveHtml": False,
                "saveScreenshots": False,
                # Get business data and emails
                "scrapeDirectEmails": True,  # Get direct emails if available
                "scrapeWebsiteDetails": False,  # We'll scrape websites ourselves for better control
                "skipPlacesWithoutWebsite": False,  # Include all businesses
                "proxyConfig": {
                    "useApifyProxy": True
                }
            }
            
            logging.info(f"🚀 Starting Google Maps scrape: {search_term}")
            
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
                    logging.error(f"Response: {response.text}")
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
                    # Scrape website for contact information
                    scraped_data = self.web_scraper.scrape_website_content(website)
                    website_data = scraped_data
                    
                    # Extract emails from website content
                    if scraped_data and 'summaries' in scraped_data:
                        import re
                        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                        
                        for summary in scraped_data['summaries']:
                            if isinstance(summary, dict):
                                content = summary.get('content', '')
                            else:
                                content = str(summary)
                            
                            emails_found = re.findall(email_pattern, content)
                            for email in emails_found:
                                # Prefer non-generic emails
                                if not any(prefix in email.lower() for prefix in ['info@', 'contact@', 'hello@']):
                                    contact_email = email
                                    break
                            if contact_email:
                                break
                    
                    logging.info(f"✅ Website scraped successfully for {business_name}")
                except Exception as e:
                    logging.warning(f"⚠️ Could not scrape website for {business_name}: {e}")
            
            # Step 2: Use Google Maps emails if no website email found
            if not contact_email:
                # Check for direct emails from Google Maps
                gm_emails = business.get('directEmails', []) or business.get('emails', [])
                if gm_emails and isinstance(gm_emails, list) and len(gm_emails) > 0:
                    contact_email = gm_emails[0]
                    logging.info(f"📧 Found email from Google Maps: {contact_email}")
            
            # Step 3: Generate smart default email if still none
            if not contact_email and website:
                contact_email = self._generate_smart_email(business, website)
            
            # Step 4: Extract owner/manager info from Google Maps data
            # Try to find owner name from reviews or responses
            owner_name = self._extract_owner_from_reviews(business)
            
            # Create the enriched contact
            return self._create_enriched_contact(business, contact_email, owner_name, website_data)
            
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
                logging.info(f"📧 Generated smart email: {email}")
                return email
        except Exception as e:
            logging.debug(f"Could not generate smart email: {e}")
        return None
    
    def _create_enriched_contact(self, business: Dict[str, Any], email: str = None, 
                                owner_name: str = None, website_data: Dict = None) -> Optional[Dict[str, Any]]:
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
            
            # Determine email status
            if email:
                if '@' in email and not any(prefix in email.lower() for prefix in ['info@', 'contact@', 'hello@']):
                    email_status = 'likely_valid'
                else:
                    email_status = 'business_email'
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
                
                logging.debug(f"🔄 Run status check: {run_status}")
                
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
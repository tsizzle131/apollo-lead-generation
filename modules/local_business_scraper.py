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
        # LinkedIn scrapers all require payment - we'll rely on Google Maps data
        # and create business-focused icebreakers instead of person-focused ones
        
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
            
            # Step 2: Process business data with lead enrichment
            enriched_contacts = []
            for idx, business in enumerate(businesses, 1):
                business_name = business.get('title') or business.get('name', 'Unknown')
                logging.info(f"🔍 Processing business {idx}/{len(businesses)}: {business_name}")
                
                # Check if Google Maps provided lead enrichment data
                leads_data = business.get('leadsEnrichment', [])
                if leads_data:
                    logging.info(f"✅ Found {len(leads_data)} leads from Google Maps enrichment")
                    for lead in leads_data:
                        contact = self._create_contact_from_lead(lead, business)
                        if contact:
                            enriched_contacts.append(contact)
                else:
                    # No leads found - LinkedIn scrapers require payment
                    # Create business contact from Google Maps data instead
                    fallback_contact = self._create_fallback_contact(business)
                    if fallback_contact:
                        enriched_contacts.append(fallback_contact)
                
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
                # Enable all enrichment features
                "scrapeDirectEmails": True,  # Get direct emails
                "enrichLeads": True,  # Enable lead enrichment (key field!)
                "scrapeWebsiteDetails": True,  # Extract contact info from websites
                "skipPlacesWithoutWebsite": False,  # Don't skip places without websites yet
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
    
    def _create_contact_from_lead(self, lead: Dict[str, Any], business: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a contact from Google Maps lead enrichment data"""
        try:
            # Extract name parts
            full_name = lead.get('name', '')
            if not full_name:
                full_name = lead.get('fullName', '')
            
            name_parts = full_name.split(' ', 1) if full_name else ['', '']
            first_name = name_parts[0] if len(name_parts) > 0 else ''
            last_name = name_parts[1] if len(name_parts) > 1 else ''
            
            # Get email - this is the most important field
            email = lead.get('email', '') or lead.get('workEmail', '')
            
            if not email and not full_name:
                return None  # Skip if no useful data
            
            # Build contact in Apollo format
            contact = {
                # Identification
                'id': f"{business.get('placeId', '')}_{lead.get('id', '')}",
                'first_name': first_name or 'Contact',
                'last_name': last_name or f"at {business.get('title', '')}",
                'name': full_name or f"Contact at {business.get('title', '')}",
                
                # Contact info
                'email': email,
                'email_status': 'verified' if email else '',
                
                # Professional info
                'title': lead.get('jobTitle', '') or lead.get('title', 'Business Owner'),
                'headline': lead.get('headline', '') or f"{lead.get('jobTitle', 'Owner')} at {business.get('title', '')}",
                'linkedin_url': lead.get('linkedinUrl', '') or lead.get('linkedInProfile', ''),
                
                # Organization from Google Maps
                'organization': {
                    'name': business.get('title') or business.get('name', ''),
                    'website_url': business.get('website') or business.get('url', ''),
                    'phone': business.get('phone') or business.get('phoneNumber', ''),
                    'address': business.get('address') or business.get('fullAddress', ''),
                    'city': business.get('city', ''),
                    'state': business.get('state', ''),
                    'category': business.get('category') or business.get('type', ''),
                },
                
                # Website for AI processing
                'website_url': business.get('website') or business.get('url', ''),
                
                # Source tracking
                '_source': 'google_maps_enriched',
                '_google_maps_data': business,
                '_lead_enrichment': lead
            }
            
            return contact
            
        except Exception as e:
            logging.warning(f"Error creating contact from lead: {e}")
            return None
    
    def _enrich_with_linkedin(self, business: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Find and enrich business owner/decision makers using LinkedIn
        """
        try:
            business_name = business.get('title') or business.get('name', '')
            location = business.get('city', '')
            
            if not business_name:
                return []
            
            # Construct LinkedIn search query
            # Search for owners, CEOs, managers at this business
            search_queries = [
                f'"{business_name}" owner',
                f'"{business_name}" CEO',
                f'"{business_name}" manager'
            ]
            
            contacts = []
            for query in search_queries:
                linkedin_url = f"https://www.linkedin.com/search/results/people/?keywords={quote(query)}&origin=GLOBAL_SEARCH_HEADER"
                
                logging.info(f"🔗 Searching LinkedIn: {query}")
                
                # Use bebity actor for LinkedIn scraping
                endpoint = f"{self.base_url}/acts/{self.linkedin_actor}/runs"
                
                headers = {
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self.api_key}"
                }
                
                payload = {
                    "urls": [linkedin_url],
                    "scrapeCompany": False,
                    "scrapeEmail": True,  # Most important - get emails
                    "scrapePhone": True,
                    "maxResults": 3  # Get top 3 matches per query
                }
                
                response = self._make_request_with_retry(
                    endpoint,
                    method="POST",
                    headers=headers,
                    json=payload
                )
                
                if response and response.status_code in [200, 201]:
                    run_data = response.json()
                    run_id = run_data.get('data', {}).get('id')
                    
                    if run_id:
                        results = self._wait_for_run_completion(run_id, headers, "LinkedIn")
                        
                        # Process LinkedIn results
                        for person in results:
                            contact = self._format_linkedin_contact(person, business)
                            if contact and contact.get('email'):
                                contacts.append(contact)
                                break  # Found a good contact, move to next business
                
                if contacts:
                    break  # Found contacts, no need to try other queries
            
            return contacts
            
        except Exception as e:
            logging.warning(f"⚠️ LinkedIn enrichment error: {e}")
            return []
    
    def _format_linkedin_contact(self, person: Dict[str, Any], business: Dict[str, Any]) -> Dict[str, Any]:
        """Format LinkedIn person data into Apollo-compatible contact format"""
        try:
            # Extract name parts
            full_name = person.get('name', '')
            name_parts = full_name.split(' ', 1) if full_name else ['', '']
            first_name = name_parts[0] if len(name_parts) > 0 else ''
            last_name = name_parts[1] if len(name_parts) > 1 else ''
            
            # Build contact in Apollo format
            contact = {
                # Identification
                'id': person.get('profileId', ''),
                'first_name': first_name,
                'last_name': last_name,
                'name': full_name,
                
                # Contact info
                'email': person.get('email', ''),
                'email_status': 'verified' if person.get('email') else '',
                
                # Professional info
                'title': person.get('headline', ''),
                'headline': person.get('headline', ''),
                'linkedin_url': person.get('profileUrl', ''),
                
                # Organization from Google Maps
                'organization': {
                    'name': business.get('title') or business.get('name', ''),
                    'website_url': business.get('website') or business.get('url', ''),
                    'phone': business.get('phone') or business.get('phoneNumber', ''),
                    'address': business.get('address') or business.get('fullAddress', ''),
                    'city': business.get('city', ''),
                    'state': business.get('state', ''),
                    'category': business.get('category') or business.get('type', ''),
                },
                
                # Website for AI processing
                'website_url': business.get('website') or business.get('url', ''),
                
                # Source tracking
                '_source': 'local_business',
                '_google_maps_data': business,
                '_linkedin_data': person
            }
            
            return contact
            
        except Exception as e:
            logging.warning(f"Error formatting LinkedIn contact: {e}")
            return None
    
    def _create_fallback_contact(self, business: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a contact from Google Maps data when LinkedIn enrichment fails"""
        try:
            # Handle different field names from the Google Maps actor
            business_name = business.get('title') or business.get('name', '')
            if not business_name:
                return None
            
            # Try to extract email from Google Maps data
            email = business.get('email', '')
            website = business.get('website') or business.get('url', '')
            
            if not email and website:
                # Generate a generic contact email
                domain = urlparse(website).netloc
                if domain:
                    domain = domain.replace('www.', '')
                    email = f"info@{domain}"
            
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
            
            contact = {
                # Use business name as contact name
                'id': business.get('placeId') or business.get('place_id', ''),
                'first_name': business_name,
                'last_name': '(Business)',
                'name': business_name,
                
                # Contact info
                'email': email,
                'email_status': 'business_email' if email else '',  # Mark as business email
                
                # Professional info
                'title': 'Business Contact',
                'headline': f"{business_name} - {business.get('category') or business.get('type', 'Local Business')}",
                'is_business_contact': True,  # Flag for AI processor
                
                # Organization data
                'organization': {
                    'name': business_name,
                    'website_url': website,
                    'phone': business.get('phone') or business.get('phoneNumber', ''),
                    'address': full_address,
                    'city': city,
                    'state': state,
                    'category': business.get('category') or business.get('type', ''),
                    'rating': business.get('rating') or business.get('averageRating'),
                    'reviews_count': business.get('reviewsCount') or business.get('totalReviews'),
                },
                
                # Website for AI processing
                'website_url': website,
                
                # Source tracking
                '_source': 'google_maps_only',
                '_google_maps_data': business
            }
            
            return contact if (email or business.get('website')) else None
            
        except Exception as e:
            logging.warning(f"Error creating fallback contact: {e}")
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
        
        # Determine actor ID based on source
        actor_id = self.google_maps_actor if source == "Google Maps" else self.linkedin_actor
        
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
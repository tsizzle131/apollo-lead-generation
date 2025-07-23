import requests
import logging
import time
import os
from typing import List, Dict, Any, Optional
from config import APIFY_API_KEY, MAX_RETRIES, REQUEST_TIMEOUT

class ApifyScraper:
    def __init__(self, api_key: str = APIFY_API_KEY):
        self.api_key = api_key
        self.base_url = "https://api.apify.com/v2"
        
    def scrape_contacts(self, search_url: str, total_records: int = None) -> List[Dict[str, Any]]:
        """
        Scrape contacts using Apify LinkedIn scraper
        
        Args:
            search_url: LinkedIn search URL to scrape
            total_records: Maximum number of records to fetch
            
        Returns:
            List of contact dictionaries
        """
        try:
            # Get record count from environment or use parameter or default
            if total_records is None:
                total_records = int(os.getenv('RECORD_COUNT', '500'))
            
            # Apify actor endpoint - using async approach for large Apollo scrapes
            actor_id = "jljBwyyQakqrL1wae"
            # Use regular run endpoint and poll for results
            endpoint = f"{self.base_url}/acts/{actor_id}/runs"
            
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            payload = {
                "getPersonalEmails": True,
                "getWorkEmails": True,
                "totalRecords": total_records,
                "url": search_url
            }
            
            logging.info(f"🚀 Starting Apify Apollo scrape for URL: {search_url}")
            logging.info(f"📊 Requesting {total_records} records from Apollo (this may take several minutes)")
            
            # Step 1: Start the actor run
            start_response = self._make_request_with_retry(
                endpoint, 
                method="POST", 
                headers=headers, 
                json=payload
            )
            
            if not start_response or start_response.status_code not in [200, 201]:
                logging.error(f"❌ Failed to start Apify run: {start_response.status_code if start_response else 'No response'}")
                return []
            
            run_data = start_response.json()
            run_id = run_data.get('data', {}).get('id')
            
            if not run_id:
                logging.error("❌ No run ID returned from Apify")
                return []
            
            logging.info(f"✅ Apify run started with ID: {run_id}")
            logging.info(f"⏳ Waiting for Apollo scrape to complete...")
            
            # Step 2: Poll for completion and get results
            return self._wait_for_run_completion(run_id, headers)
                
        except Exception as e:
            logging.error(f"❌ Error in Apify Apollo scraping: {e}")
            logging.info("💡 Troubleshooting tips:")
            logging.info("   • Check if your Apify API key is correct")
            logging.info("   • Verify the Apollo URL format is valid")  
            logging.info("   • Try a smaller record count first (e.g., 50-100)")
            logging.info("   • Check Apify dashboard for actor run details")
            return []
    
    def _make_request_with_retry(self, url: str, method: str = "GET", **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                logging.info(f"🌐 Making request to Apify (attempt {attempt + 1}/{MAX_RETRIES})...")
                if method.upper() == "POST":
                    response = requests.post(url, timeout=REQUEST_TIMEOUT, **kwargs)
                else:
                    response = requests.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
                
                if response.status_code in [200, 201]:
                    return response
                elif response.status_code == 429:  # Rate limited
                    wait_time = 2 ** attempt  # Exponential backoff
                    logging.warning(f"Rate limited, waiting {wait_time}s before retry {attempt + 1}")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.warning(f"Request failed with status {response.status_code}, attempt {attempt + 1}")
                    
            except requests.exceptions.Timeout:
                logging.warning(f"Request timeout, attempt {attempt + 1}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Request error: {e}, attempt {attempt + 1}")
            
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        logging.error(f"All {MAX_RETRIES} attempts failed for {url}")
        return None
    
    def _wait_for_run_completion(self, run_id: str, headers: dict) -> List[Dict[str, Any]]:
        """Wait for Apify run to complete and return the results"""
        max_wait_time = 1800  # 30 minutes max wait time
        check_interval = 15   # Check every 15 seconds
        elapsed_time = 0
        consecutive_failures = 0
        max_consecutive_failures = 3  # Allow 3 consecutive connection failures before giving up
        
        while elapsed_time < max_wait_time:
            try:
                # Check run status
                status_url = f"{self.base_url}/acts/jljBwyyQakqrL1wae/runs/{run_id}"
                status_response = self._make_request_with_retry(status_url, headers=headers)
                
                if not status_response:
                    consecutive_failures += 1
                    logging.warning(f"⚠️ Failed to get run status (attempt {consecutive_failures}/{max_consecutive_failures})")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        logging.error("❌ Too many consecutive connection failures - giving up")
                        return []
                    
                    # Wait longer before retry when having connection issues
                    time.sleep(check_interval * 2)
                    elapsed_time += check_interval * 2
                    continue
                
                # Reset consecutive failures on successful response
                consecutive_failures = 0
                
                run_data = status_response.json()
                run_status = run_data.get('data', {}).get('status', 'UNKNOWN')
                
                logging.info(f"🔄 Apollo scrape status: {run_status} (elapsed: {elapsed_time}s)")
                
                if run_status == 'SUCCEEDED':
                    logging.info("✅ Apollo scrape completed successfully!")
                    
                    # Get the dataset items
                    dataset_id = run_data.get('data', {}).get('defaultDatasetId')
                    if not dataset_id:
                        logging.error("❌ No dataset ID found in completed run")
                        return []
                    
                    # Fetch results from dataset
                    dataset_url = f"{self.base_url}/datasets/{dataset_id}/items"
                    dataset_response = self._make_request_with_retry(dataset_url, headers=headers)
                    
                    if not dataset_response:
                        logging.error("❌ Failed to fetch dataset results")
                        return []
                    
                    results = dataset_response.json()
                    logging.info(f"📊 Retrieved {len(results)} contacts from Apollo")
                    
                    return self._process_apify_response(results)
                
                elif run_status == 'FAILED':
                    logging.error(f"❌ Apollo scrape failed with status: {run_status}")
                    return []
                
                elif run_status in ['RUNNING', 'READY']:
                    # Still running, wait and check again
                    time.sleep(check_interval)
                    elapsed_time += check_interval
                    
                    # Log progress every 2 minutes
                    if elapsed_time % 120 == 0:
                        logging.info(f"⏳ Still waiting for Apollo scrape... ({elapsed_time//60} minutes elapsed)")
                
                else:
                    logging.warning(f"⚠️ Unknown run status: {run_status}, continuing to wait...")
                    time.sleep(check_interval)
                    elapsed_time += check_interval
                
            except Exception as e:
                consecutive_failures += 1
                logging.error(f"❌ Error checking run status (attempt {consecutive_failures}/{max_consecutive_failures}): {e}")
                
                if consecutive_failures >= max_consecutive_failures:
                    logging.error("❌ Too many consecutive errors - giving up")
                    return []
                
                # Wait longer before retry when having connection issues
                time.sleep(check_interval * 2)
                elapsed_time += check_interval * 2
        
        logging.error(f"❌ Apollo scrape timed out after {max_wait_time} seconds")
        return []
    
    def _process_apify_response(self, data) -> List[Dict[str, Any]]:
        """Process the response from Apify and return ALL contact data for storage"""
        contacts = []
        
        # Handle different response formats
        items = data if isinstance(data, list) else data.get('items', [])
        
        for item in items:
            try:
                # Return the complete raw contact data - let Supabase handle the storage
                # This preserves all fields from Apollo/Apify for future use
                contact = {
                    # Core identification fields
                    'id': item.get('id'),  # Apollo ID
                    'first_name': item.get('first_name', ''),
                    'last_name': item.get('last_name', ''),
                    'name': item.get('name', ''),  # Sometimes used instead of first_name
                    
                    # Contact information
                    'email': item.get('email', ''),
                    'email_status': item.get('email_status', ''),
                    'extrapolated_email_confidence': item.get('extrapolated_email_confidence'),
                    
                    # Professional information
                    'title': item.get('title', ''),
                    'headline': item.get('headline', ''),
                    'linkedin_url': item.get('linkedin_url', ''),
                    
                    # Social media profiles
                    'photo_url': item.get('photo_url', ''),
                    'twitter_url': item.get('twitter_url', ''),
                    'github_url': item.get('github_url', ''),
                    'facebook_url': item.get('facebook_url', ''),
                    
                    # Education
                    'degree': item.get('degree', ''),
                    'grade_level': item.get('grade_level', ''),
                    
                    # Organization information
                    'organization_id': item.get('organization_id', ''),
                    'organization': item.get('organization', {}),
                    
                    # Location (legacy support)
                    'city': item.get('city', ''),
                    'country': item.get('country', ''),
                    
                    # Website URL extraction
                    'website_url': self._extract_website_url_from_contact(item),
                    
                    # Store original data for future use
                    '_raw_item': item
                }
                
                # Always add contact - let the database manager decide what to process
                contacts.append(contact)
                    
            except Exception as e:
                logging.warning(f"Error processing contact item: {e}")
                continue
        
        return contacts
    
    def _extract_website_url_from_contact(self, item: Dict[str, Any]) -> str:
        """Extract website URL from various possible fields in the contact data"""
        # Try direct website_url field first
        website = item.get('website_url')
        if website:
            return website
            
        # Try organization data
        org = item.get('organization', {})
        if isinstance(org, dict) and org.get('website_url'):
            return org['website_url']
            
        # Try other possible fields
        for field in ['company_website', 'website', 'web_url']:
            if item.get(field):
                return item[field]
                
        return ""
    
    def test_connection(self) -> bool:
        """Test if Apify API connection is working"""
        try:
            # Test with a simple API call
            test_url = f"{self.base_url}/acts"
            headers = {
                "Authorization": f"Bearer {self.api_key}"
            }
            
            response = requests.get(test_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                logging.info("Apify API connection successful")
                return True
            else:
                logging.error(f"Apify API test failed: {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"Apify API test error: {e}")
            return False
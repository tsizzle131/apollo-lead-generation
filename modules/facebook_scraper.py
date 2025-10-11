"""
Facebook Pages Scraper Module
Extracts emails and contact information from Facebook business pages
Uses Apify's Facebook Pages Scraper actor
"""

import requests
import logging
import time
from typing import List, Dict, Any, Optional
from config import APIFY_API_KEY, MAX_RETRIES, REQUEST_TIMEOUT

class FacebookScraper:
    def __init__(self, api_key: str = APIFY_API_KEY):
        """Initialize Facebook scraper with Apify API"""
        self.api_key = api_key
        self.base_url = "https://api.apify.com/v2"
        
        # Facebook Pages Scraper actor ID
        self.facebook_actor = "4Hv5RhChiaDk6iwad"
        
    def enrich_with_facebook(self, facebook_urls: List[str], max_pages: int = 100) -> List[Dict[str, Any]]:
        """
        Extract emails and contact info from Facebook pages
        
        Args:
            facebook_urls: List of Facebook page URLs to scrape
            max_pages: Maximum number of pages to process
            
        Returns:
            List of enrichment results with emails and contact info
        """
        try:
            if not facebook_urls:
                logging.warning("No Facebook URLs provided for enrichment")
                return []
            
            logging.info(f"🔍 Starting Facebook enrichment for {len(facebook_urls)} pages")
            
            # Run Facebook Pages Scraper
            results = self._scrape_facebook_pages(facebook_urls[:max_pages])
            
            # Process results to extract emails
            enriched_data = []
            for result in results:
                enrichment = self._extract_contact_info(result)
                if enrichment:
                    enriched_data.append(enrichment)
            
            logging.info(f"✅ Enriched {len(enriched_data)} Facebook pages")
            return enriched_data
            
        except Exception as e:
            logging.error(f"Error in Facebook enrichment: {e}")
            return []
    
    def _scrape_facebook_pages(self, facebook_urls: List[str]) -> List[Dict[str, Any]]:
        """Run Apify Facebook Pages Scraper"""
        try:
            endpoint = f"{self.base_url}/acts/{self.facebook_actor}/runs"
            
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            # Prepare Facebook Pages Scraper input
            payload = {
                "startUrls": [{"url": url} for url in facebook_urls],
                "maxPagesPerQuery": len(facebook_urls),
                "proxyConfiguration": {
                    "useApifyProxy": True
                },
                "scrapeAbout": True,  # Important for contact info
                "scrapeReviews": False,  # We don't need reviews
                "scrapePosts": False,  # We don't need posts
                "scrapeServices": True,  # May contain contact info
                "reviewLimit": 0,
                "postLimit": 0,
                "commentsLimit": 0
            }
            
            logging.info(f"🚀 Starting Facebook Pages scrape for {len(facebook_urls)} URLs")
            
            # Start the actor run
            response = self._make_request_with_retry(
                endpoint,
                method="POST",
                headers=headers,
                json=payload
            )
            
            if not response or response.status_code not in [200, 201]:
                logging.error(f"Failed to start Facebook scraper: Status {response.status_code if response else 'No response'}")
                return []
            
            run_data = response.json()
            run_id = run_data.get('data', {}).get('id')
            
            if not run_id:
                logging.error("No run ID returned from Facebook scraper")
                return []
            
            logging.info(f"⏳ Waiting for Facebook scrape to complete (Run ID: {run_id})")
            
            # Wait for completion and get results
            results = self._wait_for_run_completion(run_id, headers)
            logging.info(f"📊 Facebook scraper returned {len(results)} results")
            
            return results
            
        except Exception as e:
            logging.error(f"Error in Facebook scraping: {e}")
            return []
    
    def _extract_contact_info(self, page_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract emails and contact information from Facebook page data"""
        try:
            # Initialize enrichment result
            enrichment = {
                "facebook_url": page_data.get("url") or page_data.get("facebookUrl") or page_data.get("pageUrl"),
                "page_name": page_data.get("pageName") or page_data.get("name") or page_data.get("title"),
                "page_likes": page_data.get("likes"),
                "page_followers": page_data.get("followers"),
                "emails": [],
                "primary_email": None,
                "email_sources": [],
                "phone_numbers": [],
                "addresses": [],
                "success": False,
                "raw_data": page_data
            }
            
            # Extract emails from various fields
            emails_found = set()
            email_sources = []

            # CRITICAL FIX: Check root-level 'email' field FIRST
            # The Apify actor returns email directly at root level
            if page_data.get("email"):
                emails_found.add(page_data["email"])
                email_sources.append("root.email")

            # Also check 'phone' at root level
            if page_data.get("phone"):
                enrichment["phone_numbers"].append(page_data["phone"])

            # Check 'about' section
            about = page_data.get("about", {})
            if isinstance(about, dict):
                # Check email field directly
                if about.get("email"):
                    emails_found.add(about["email"])
                    email_sources.append("about.email")
                
                # Check contact info
                contact_info = about.get("contactInfo", {})
                if isinstance(contact_info, dict) and contact_info.get("email"):
                    emails_found.add(contact_info["email"])
                    email_sources.append("about.contactInfo")
                
                # Check description for emails
                description = about.get("description", "")
                if description:
                    import re
                    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                    found_emails = re.findall(email_pattern, description)
                    for email in found_emails:
                        emails_found.add(email)
                        email_sources.append("about.description")
            
            # Check 'info' section
            info = page_data.get("info", {})
            if isinstance(info, dict):
                if info.get("email"):
                    emails_found.add(info["email"])
                    email_sources.append("info.email")
                
                # Check for phone
                if info.get("phone"):
                    enrichment["phone_numbers"].append(info["phone"])
                
                # Check for address
                if info.get("address"):
                    enrichment["addresses"].append(info["address"])
            
            # Check 'contactInfo' at root level
            contact = page_data.get("contactInfo", {})
            if isinstance(contact, dict):
                if contact.get("email"):
                    emails_found.add(contact["email"])
                    email_sources.append("contactInfo")
                if contact.get("phone"):
                    enrichment["phone_numbers"].append(contact["phone"])
            
            # Check services section for contact info
            services = page_data.get("services", [])
            if isinstance(services, list):
                for service in services:
                    if isinstance(service, dict):
                        service_desc = service.get("description", "")
                        if service_desc:
                            import re
                            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                            found_emails = re.findall(email_pattern, service_desc)
                            for email in found_emails:
                                emails_found.add(email)
                                email_sources.append("services")
            
            # Convert to list and filter out invalid emails
            valid_emails = []
            for email in emails_found:
                # Filter out common non-contact emails
                if not any(skip in email.lower() for skip in [
                    'noreply', 'no-reply', 'donotreply', 'example.com',
                    '@facebook.com', '@instagram.com', '@twitter.com'
                ]):
                    valid_emails.append(email)
            
            # Set results
            enrichment["emails"] = valid_emails
            enrichment["email_sources"] = list(set(email_sources))
            
            # Select primary email (prefer info@ or contact@ emails)
            if valid_emails:
                primary = None
                for email in valid_emails:
                    if any(prefix in email.lower() for prefix in ['info@', 'contact@', 'hello@', 'support@']):
                        primary = email
                        break
                enrichment["primary_email"] = primary or valid_emails[0]
                enrichment["success"] = True
            
            # Remove duplicate phone numbers
            enrichment["phone_numbers"] = list(set(enrichment["phone_numbers"]))
            
            if enrichment["primary_email"]:
                logging.info(f"✅ Found email for {enrichment['page_name']}: {enrichment['primary_email']}")
            else:
                logging.debug(f"❌ No email found for {enrichment['page_name']}")
            
            return enrichment
            
        except Exception as e:
            logging.error(f"Error extracting contact info: {e}")
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
    
    def _wait_for_run_completion(self, run_id: str, headers: dict) -> List[Dict[str, Any]]:
        """Wait for Apify run to complete and return results"""
        max_wait_time = 600  # 10 minutes max
        check_interval = 5   # Check every 5 seconds
        elapsed_time = 0
        
        while elapsed_time < max_wait_time:
            try:
                status_url = f"{self.base_url}/acts/{self.facebook_actor}/runs/{run_id}"
                status_response = self._make_request_with_retry(status_url, headers=headers)
                
                if not status_response:
                    logging.warning("Failed to get Facebook run status")
                    time.sleep(check_interval)
                    elapsed_time += check_interval
                    continue
                
                run_data = status_response.json()
                run_status = run_data.get('data', {}).get('status', 'UNKNOWN')
                
                # Use info logging for better visibility
                if elapsed_time % 10 == 0 or run_status != 'RUNNING':
                    logging.info(f"🔄 Facebook status: {run_status} ({elapsed_time}s elapsed)")
                
                if run_status == 'SUCCEEDED':
                    logging.info("✅ Facebook scrape completed!")
                    
                    # Get dataset results
                    dataset_id = run_data.get('data', {}).get('defaultDatasetId')
                    if not dataset_id:
                        logging.error("No dataset ID found")
                        return []
                    
                    dataset_url = f"{self.base_url}/datasets/{dataset_id}/items"
                    dataset_response = self._make_request_with_retry(dataset_url, headers=headers)
                    
                    if not dataset_response:
                        logging.error("Failed to fetch results")
                        return []
                    
                    results = dataset_response.json()
                    return results if isinstance(results, list) else []
                
                elif run_status == 'FAILED':
                    logging.error("Facebook scrape failed")
                    return []
                
                elif run_status in ['RUNNING', 'READY']:
                    time.sleep(check_interval)
                    elapsed_time += check_interval
                    
                    if elapsed_time % 30 == 0:
                        logging.info(f"⏳ Still waiting... ({elapsed_time}s elapsed)")
                
            except Exception as e:
                logging.error(f"Error checking status: {e}")
                return []
        
        logging.error("Facebook scrape timed out")
        return []
    
    def test_connection(self) -> bool:
        """Test if Apify API connection is working"""
        try:
            test_url = f"{self.base_url}/acts"
            headers = {"Authorization": f"Bearer {self.api_key}"}
            
            response = requests.get(test_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                logging.info("✅ Facebook Scraper API connection successful")
                return True
            else:
                logging.error(f"❌ API test failed: {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"❌ API test error: {e}")
            return False

# Example usage
if __name__ == "__main__":
    scraper = FacebookScraper()
    
    # Test with sample Facebook URLs
    test_urls = [
        "https://www.facebook.com/example1",
        "https://www.facebook.com/example2"
    ]
    
    results = scraper.enrich_with_facebook(test_urls)
    for result in results:
        print(f"Page: {result['page_name']}")
        print(f"Primary Email: {result['primary_email']}")
        print(f"All Emails: {result['emails']}")
        print("-" * 50)
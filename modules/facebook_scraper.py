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
        """Run Apify Facebook Pages Scraper with comprehensive error handling"""
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
            logging.info(f"   Actor ID: {self.facebook_actor}")

            # Start the actor run
            response = self._make_request_with_retry(
                endpoint,
                method="POST",
                headers=headers,
                json=payload
            )

            if not response:
                logging.error(f"❌ Failed to start Facebook scraper - No response from Apify")
                logging.error(f"   Actor ID: {self.facebook_actor}")
                logging.error(f"   Check API key and actor ID validity")
                return []

            if response.status_code not in [200, 201]:
                logging.error(f"❌ Failed to start Facebook scraper")
                logging.error(f"   Status code: {response.status_code}")
                logging.error(f"   Actor ID: {self.facebook_actor}")
                try:
                    error_data = response.json()
                    logging.error(f"   Error details: {error_data}")
                except:
                    logging.error(f"   Response text: {response.text[:200]}")
                return []

            try:
                run_data = response.json()
            except ValueError as e:
                logging.error(f"❌ Invalid JSON response when starting Facebook scraper")
                logging.error(f"   Error: {e}")
                logging.error(f"   Response text: {response.text[:200]}")
                return []

            run_id = run_data.get('data', {}).get('id')

            if not run_id:
                logging.error(f"❌ No run ID returned from Facebook scraper")
                logging.error(f"   Actor ID: {self.facebook_actor}")
                logging.error(f"   Response data: {run_data}")
                return []

            logging.info(f"⏳ Waiting for Facebook scrape to complete (Run ID: {run_id})")

            # Wait for completion and get results
            results = self._wait_for_run_completion(run_id, headers)

            if not results:
                logging.warning(f"⚠️  Facebook scraper returned no results")
                logging.warning(f"   Run ID: {run_id}")
                logging.warning(f"   This may be normal if no pages were accessible")
            else:
                logging.info(f"📊 Facebook scraper returned {len(results)} results")

            return results

        except KeyboardInterrupt:
            logging.error(f"❌ Facebook scraping interrupted by user")
            raise
        except Exception as e:
            logging.error(f"❌ Unexpected error in Facebook scraping")
            logging.error(f"   Actor ID: {self.facebook_actor}")
            logging.error(f"   Error type: {type(e).__name__}")
            logging.error(f"   Error: {e}")
            import traceback
            logging.error(f"   Traceback: {traceback.format_exc()}")
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
        """Make HTTP request with retry logic and exponential backoff"""
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
                    logging.warning(f"⚠️  Rate limited by Apify (429), waiting {wait_time}s before retry {attempt + 1}/{MAX_RETRIES}")
                    logging.warning(f"   URL: {url}")
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 401:
                    logging.error(f"❌ Authentication failed (401) - Invalid or expired API key")
                    logging.error(f"   URL: {url}")
                    logging.error(f"   Actor ID: {self.facebook_actor}")
                    return None
                elif response.status_code == 404:
                    logging.error(f"❌ Resource not found (404)")
                    logging.error(f"   URL: {url}")
                    logging.error(f"   Actor ID may be invalid: {self.facebook_actor}")
                    return None
                elif response.status_code >= 500:
                    wait_time = 2 ** attempt
                    logging.warning(f"⚠️  Server error ({response.status_code}), retrying in {wait_time}s (attempt {attempt + 1}/{MAX_RETRIES})")
                    logging.warning(f"   URL: {url}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        return None
                else:
                    logging.warning(f"⚠️  Request failed with status {response.status_code} (attempt {attempt + 1}/{MAX_RETRIES})")
                    logging.warning(f"   URL: {url}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        return None

            except requests.exceptions.Timeout as e:
                wait_time = 2 ** attempt
                logging.warning(f"⚠️  Request timeout after {REQUEST_TIMEOUT}s (attempt {attempt + 1}/{MAX_RETRIES})")
                logging.warning(f"   URL: {url}")
                logging.warning(f"   Error: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(wait_time)
                    continue
            except requests.exceptions.ConnectionError as e:
                wait_time = 2 ** attempt
                logging.warning(f"⚠️  Connection error (attempt {attempt + 1}/{MAX_RETRIES})")
                logging.warning(f"   URL: {url}")
                logging.warning(f"   Error: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(wait_time)
                    continue
            except requests.exceptions.RequestException as e:
                logging.warning(f"⚠️  Request error (attempt {attempt + 1}/{MAX_RETRIES})")
                logging.warning(f"   URL: {url}")
                logging.warning(f"   Error type: {type(e).__name__}")
                logging.warning(f"   Error: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue

        logging.error(f"❌ All {MAX_RETRIES} retry attempts failed")
        logging.error(f"   URL: {url}")
        return None
    
    def _wait_for_run_completion(self, run_id: str, headers: dict) -> List[Dict[str, Any]]:
        """Wait for Apify run to complete and return results with fail-fast error handling"""
        max_wait_time = 300  # 5 minutes max (reduced from 10 to fail faster)
        check_interval = 5   # Check every 5 seconds
        elapsed_time = 0
        consecutive_running = 0
        max_consecutive_running = 36  # 3 minutes of stuck RUNNING state (36 * 5s)
        last_status = None

        while elapsed_time < max_wait_time:
            try:
                status_url = f"{self.base_url}/acts/{self.facebook_actor}/runs/{run_id}"
                status_response = self._make_request_with_retry(status_url, headers=headers)

                if not status_response:
                    logging.warning(f"⚠️  Failed to get Facebook run status (attempt {elapsed_time // check_interval})")
                    logging.warning(f"   Run ID: {run_id}")
                    logging.warning(f"   Actor ID: {self.facebook_actor}")
                    time.sleep(check_interval)
                    elapsed_time += check_interval
                    continue

                try:
                    run_data = status_response.json()
                except ValueError as e:
                    logging.error(f"❌ Invalid JSON response from Apify API")
                    logging.error(f"   Run ID: {run_id}")
                    logging.error(f"   Error: {e}")
                    return []

                run_status = run_data.get('data', {}).get('status', 'UNKNOWN')

                # Use info logging for better visibility
                if elapsed_time % 10 == 0 or run_status != last_status:
                    logging.info(f"🔄 Facebook status: {run_status} ({elapsed_time}s elapsed)")

                if run_status == 'SUCCEEDED':
                    logging.info("✅ Facebook scrape completed!")

                    # Get dataset results
                    dataset_id = run_data.get('data', {}).get('defaultDatasetId')
                    if not dataset_id:
                        logging.error("❌ No dataset ID found in successful run")
                        logging.error(f"   Run ID: {run_id}")
                        logging.error(f"   This may indicate an API response format change")
                        return []

                    dataset_url = f"{self.base_url}/datasets/{dataset_id}/items"
                    dataset_response = self._make_request_with_retry(dataset_url, headers=headers)

                    if not dataset_response:
                        logging.error("❌ Failed to fetch dataset results")
                        logging.error(f"   Dataset ID: {dataset_id}")
                        logging.error(f"   Run ID: {run_id}")
                        return []

                    try:
                        results = dataset_response.json()
                    except ValueError as e:
                        logging.error(f"❌ Invalid JSON in dataset response")
                        logging.error(f"   Dataset ID: {dataset_id}")
                        logging.error(f"   Error: {e}")
                        return []

                    return results if isinstance(results, list) else []

                elif run_status == 'FAILED':
                    error_message = run_data.get('data', {}).get('statusMessage', 'No error message')
                    logging.error(f"❌ Facebook scrape failed")
                    logging.error(f"   Run ID: {run_id}")
                    logging.error(f"   Actor ID: {self.facebook_actor}")
                    logging.error(f"   Error: {error_message}")
                    return []

                elif run_status == 'ABORTED':
                    logging.error(f"❌ Facebook scrape was aborted")
                    logging.error(f"   Run ID: {run_id}")
                    logging.error(f"   This may indicate the actor was manually stopped or exceeded limits")
                    return []

                elif run_status == 'TIMED-OUT':
                    logging.error(f"❌ Facebook scrape timed out on Apify's side")
                    logging.error(f"   Run ID: {run_id}")
                    logging.error(f"   The actor exceeded its execution time limit")
                    return []

                elif run_status in ['RUNNING', 'READY']:
                    # Track consecutive RUNNING states to detect stuck actors
                    if run_status == 'RUNNING':
                        consecutive_running += 1

                        # Fail fast if stuck in RUNNING for too long
                        if consecutive_running >= max_consecutive_running:
                            logging.error(f"❌ Facebook actor stuck in RUNNING state for {consecutive_running * check_interval}s")
                            logging.error(f"   Run ID: {run_id}")
                            logging.error(f"   Actor ID: {self.facebook_actor}")
                            logging.error(f"   Aborting to prevent indefinite hang")
                            logging.error(f"   This usually indicates the actor is stalled or encountering rate limits")
                            return []
                    else:
                        consecutive_running = 0  # Reset counter if status changes

                    time.sleep(check_interval)
                    elapsed_time += check_interval

                    if elapsed_time % 30 == 0:
                        logging.info(f"⏳ Still waiting... ({elapsed_time}s elapsed, status: {run_status})")

                else:
                    # Unknown status - log and continue
                    logging.warning(f"⚠️  Unknown Facebook run status: {run_status}")
                    logging.warning(f"   Run ID: {run_id}")
                    time.sleep(check_interval)
                    elapsed_time += check_interval

                last_status = run_status

            except requests.exceptions.Timeout as e:
                logging.error(f"❌ Timeout while checking Facebook run status")
                logging.error(f"   Run ID: {run_id}")
                logging.error(f"   Error: {e}")
                return []
            except requests.exceptions.ConnectionError as e:
                logging.error(f"❌ Connection error while checking Facebook run status")
                logging.error(f"   Run ID: {run_id}")
                logging.error(f"   Error: {e}")
                return []
            except Exception as e:
                logging.error(f"❌ Unexpected error checking Facebook status")
                logging.error(f"   Run ID: {run_id}")
                logging.error(f"   Actor ID: {self.facebook_actor}")
                logging.error(f"   Error type: {type(e).__name__}")
                logging.error(f"   Error: {e}")
                return []

        logging.error(f"❌ Facebook scrape timed out after {max_wait_time}s")
        logging.error(f"   Run ID: {run_id}")
        logging.error(f"   Actor ID: {self.facebook_actor}")
        logging.error(f"   Max wait time reduced to fail faster - consider increasing if legitimate runs need more time")
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
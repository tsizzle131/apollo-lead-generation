"""
Facebook Pages Scraper Module (Version 2)
Extracts emails and contact information from Facebook business pages
Uses a more reliable approach with better contact info extraction
"""

import requests
import logging
import time
import re
from typing import List, Dict, Any, Optional
from config import APIFY_API_KEY, MAX_RETRIES, REQUEST_TIMEOUT

class FacebookScraper:
    def __init__(self, api_key: str = APIFY_API_KEY):
        """Initialize Facebook scraper with Apify API"""
        self.api_key = api_key
        self.base_url = "https://api.apify.com/v2"

        # Use Web Scraper to extract contact info from Facebook pages
        # This is more reliable than specialized Facebook actors
        self.scraper_actor = "apify/web-scraper"

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

            # Process in smaller batches to avoid timeouts
            batch_size = 20
            all_enrichments = []

            for i in range(0, min(len(facebook_urls), max_pages), batch_size):
                batch = facebook_urls[i:i + batch_size]
                logging.info(f"Processing batch {i//batch_size + 1} ({len(batch)} URLs)")

                # Scrape this batch
                results = self._scrape_facebook_pages(batch)

                # Process results to extract emails
                for result in results:
                    enrichment = self._extract_contact_info(result)
                    if enrichment:
                        all_enrichments.append(enrichment)

                # Rate limiting between batches
                if i + batch_size < min(len(facebook_urls), max_pages):
                    time.sleep(3)

            logging.info(f"✅ Enriched {len(all_enrichments)} Facebook pages")
            return all_enrichments

        except Exception as e:
            logging.error(f"Error in Facebook enrichment: {e}")
            return []

    def _scrape_facebook_pages(self, facebook_urls: List[str]) -> List[Dict[str, Any]]:
        """Scrape Facebook pages using Web Scraper with custom page function"""
        try:
            endpoint = f"{self.base_url}/acts/{self.scraper_actor}/runs"

            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }

            # Custom page function to extract contact info from Facebook pages
            page_function = """
            async function pageFunction(context) {
                const { request, page, log } = context;

                // Wait for page to load
                await page.waitForTimeout(2000);

                // Get page title
                const title = await page.title();

                // Get all text content
                const bodyText = await page.evaluate(() => document.body.innerText);

                // Extract emails using regex
                const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g;
                const emails = bodyText.match(emailRegex) || [];

                // Extract phone numbers (US format)
                const phoneRegex = /\\(?\\d{3}\\)?[\\s.-]?\\d{3}[\\s.-]?\\d{4}/g;
                const phones = bodyText.match(phoneRegex) || [];

                // Try to get page name from meta tags
                let pageName = title;
                const metaTitle = await page.$eval('meta[property="og:title"]', el => el.content).catch(() => null);
                if (metaTitle) pageName = metaTitle;

                // Get website link if available
                let website = null;
                const links = await page.evaluate(() => {
                    const anchors = Array.from(document.querySelectorAll('a'));
                    return anchors
                        .map(a => a.href)
                        .filter(href => href && !href.includes('facebook.com'));
                });
                if (links.length > 0) website = links[0];

                return {
                    url: request.url,
                    pageName: pageName,
                    emails: [...new Set(emails)],
                    phones: [...new Set(phones)],
                    website: website,
                    bodyText: bodyText.substring(0, 5000) // First 5000 chars for debugging
                };
            }
            """

            # Prepare Web Scraper input
            payload = {
                "startUrls": [{"url": url} for url in facebook_urls],
                "pageFunction": page_function,
                "maxRequestsPerCrawl": len(facebook_urls),
                "maxConcurrency": 5,
                "proxyConfiguration": {
                    "useApifyProxy": True,
                    "apifyProxyGroups": ["RESIDENTIAL"]  # Use residential proxies for Facebook
                },
                "waitForPageIdleSecs": 3,
                "maxRequestRetries": 3
            }

            logging.info(f"🚀 Starting Web Scraper for {len(facebook_urls)} URLs")

            # Start the actor run
            response = self._make_request_with_retry(
                endpoint,
                method="POST",
                headers=headers,
                json=payload
            )

            if not response or response.status_code not in [200, 201]:
                logging.error(f"Failed to start Web Scraper: Status {response.status_code if response else 'No response'}")
                return []

            run_data = response.json()
            run_id = run_data.get('data', {}).get('id')

            if not run_id:
                logging.error("No run ID returned from Web Scraper")
                return []

            logging.info(f"⏳ Waiting for scrape to complete (Run ID: {run_id})")

            # Wait for completion and get results
            results = self._wait_for_run_completion(run_id, headers)
            logging.info(f"📊 Web Scraper returned {len(results)} results")

            return results

        except Exception as e:
            logging.error(f"Error in Facebook scraping: {e}")
            return []

    def _extract_contact_info(self, page_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract and validate contact information from scraped page data"""
        try:
            # Initialize enrichment result
            enrichment = {
                "facebook_url": page_data.get("url"),
                "page_name": page_data.get("pageName", "Unknown"),
                "emails": [],
                "primary_email": None,
                "email_sources": [],
                "phone_numbers": [],
                "website": page_data.get("website"),
                "success": False,
                "raw_data": page_data
            }

            # Get emails and filter out invalid ones
            raw_emails = page_data.get("emails", [])
            valid_emails = []

            for email in raw_emails:
                email = email.lower().strip()

                # Filter out common non-contact emails
                if any(skip in email for skip in [
                    'noreply', 'no-reply', 'donotreply', 'example.com',
                    '@facebook.com', '@instagram.com', '@twitter.com',
                    '@fb.com', '@meta.com', 'privacy@', 'legal@',
                    'test@', 'sample@', 'user@', 'admin@localhost'
                ]):
                    continue

                # Validate email format
                if self._is_valid_email(email):
                    valid_emails.append(email)

            # Remove duplicates while preserving order
            valid_emails = list(dict.fromkeys(valid_emails))

            enrichment["emails"] = valid_emails
            enrichment["email_sources"] = ["facebook_page_text"] if valid_emails else []

            # Select primary email (prefer business-related emails)
            if valid_emails:
                primary = None
                for email in valid_emails:
                    if any(prefix in email for prefix in ['info@', 'contact@', 'hello@', 'support@', 'sales@']):
                        primary = email
                        break
                enrichment["primary_email"] = primary or valid_emails[0]
                enrichment["success"] = True

            # Get phone numbers
            raw_phones = page_data.get("phones", [])
            enrichment["phone_numbers"] = list(set(raw_phones))

            if enrichment["primary_email"]:
                logging.info(f"✅ Found email for {enrichment['page_name']}: {enrichment['primary_email']}")
            else:
                logging.debug(f"❌ No email found for {enrichment['page_name']}")

            return enrichment

        except Exception as e:
            logging.error(f"Error extracting contact info: {e}")
            return None

    def _is_valid_email(self, email: str) -> bool:
        """Validate email format"""
        # Basic email validation
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))

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
                status_url = f"{self.base_url}/acts/{self.scraper_actor}/runs/{run_id}"
                status_response = self._make_request_with_retry(status_url, headers=headers)

                if not status_response:
                    logging.warning("Failed to get run status")
                    time.sleep(check_interval)
                    elapsed_time += check_interval
                    continue

                run_data = status_response.json()
                run_status = run_data.get('data', {}).get('status', 'UNKNOWN')

                # Log status periodically
                if elapsed_time % 10 == 0 or run_status != 'RUNNING':
                    logging.info(f"🔄 Status: {run_status} ({elapsed_time}s elapsed)")

                if run_status == 'SUCCEEDED':
                    logging.info("✅ Scrape completed!")

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
                    logging.error("Scrape failed")
                    error_msg = run_data.get('data', {}).get('statusMessage', 'Unknown error')
                    logging.error(f"Error: {error_msg}")
                    return []

                elif run_status in ['RUNNING', 'READY']:
                    time.sleep(check_interval)
                    elapsed_time += check_interval

                    if elapsed_time % 30 == 0:
                        logging.info(f"⏳ Still waiting... ({elapsed_time}s elapsed)")

            except Exception as e:
                logging.error(f"Error checking status: {e}")
                return []

        logging.error("Scrape timed out")
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
        "https://www.facebook.com/starbucks",
        "https://www.facebook.com/cocacola"
    ]

    results = scraper.enrich_with_facebook(test_urls)
    for result in results:
        print(f"Page: {result['page_name']}")
        print(f"Primary Email: {result['primary_email']}")
        print(f"All Emails: {result['emails']}")
        print("-" * 50)

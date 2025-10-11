"""
LinkedIn Email Extractor Module
Extracts publicly visible emails from LinkedIn profiles using Apify actor 2SyF0bVxmgGr8IVCZ
Provides verified email addresses when available on LinkedIn profiles
"""

import requests
import logging
import time
from typing import List, Dict, Any, Optional

class LinkedInEmailExtractor:
    """
    Extracts publicly visible emails from LinkedIn profiles

    This actor searches for emails that LinkedIn users have made public on their profiles.
    Success rate: ~8% (most profiles don't expose emails publicly)

    Returns verified emails when found, which are higher quality than generated patterns.
    """

    MAX_RETRIES = 3
    REQUEST_TIMEOUT = 30

    def __init__(self, apify_key: str, actor_id: str = "2SyF0bVxmgGr8IVCZ"):
        """
        Initialize LinkedIn Email Extractor

        Args:
            apify_key: Apify API key
            actor_id: Email extraction actor ID (default: 2SyF0bVxmgGr8IVCZ)
        """
        self.api_key = apify_key
        self.base_url = "https://api.apify.com/v2"
        self.actor_id = actor_id

    def extract_emails_batch(self, linkedin_urls: List[str]) -> List[Dict[str, Any]]:
        """
        Extract emails from multiple LinkedIn profiles in batch

        Args:
            linkedin_urls: List of LinkedIn profile URLs (personal or company)

        Returns:
            List of dicts with extracted data:
            {
                "linkedinUrl": "https://linkedin.com/in/...",
                "email": "person@company.com" or None,
                "mobileNumber": "+1..." or None,
                "firstName": "John",
                "lastName": "Doe",
                "fullName": "John Doe",
                "headline": "Job Title at Company",
                "extraction_success": True/False
            }
        """
        if not linkedin_urls:
            logging.warning("No LinkedIn URLs provided for email extraction")
            return []

        logging.info(f"📧 Starting LinkedIn email extraction for {len(linkedin_urls)} profiles")

        try:
            # Run the email extraction actor
            results = self._run_email_extraction(linkedin_urls)

            # Process and normalize results
            processed_results = []
            emails_found = 0
            phones_found = 0

            for result in results:
                processed = self._process_extraction_result(result)
                processed_results.append(processed)

                if processed.get('email'):
                    emails_found += 1
                if processed.get('mobileNumber'):
                    phones_found += 1

            logging.info(f"✅ Email extraction complete:")
            logging.info(f"   Profiles processed: {len(processed_results)}")
            logging.info(f"   Emails found: {emails_found} ({emails_found/len(processed_results)*100:.1f}%)")
            logging.info(f"   Phone numbers found: {phones_found}")

            return processed_results

        except Exception as e:
            logging.error(f"Error in email extraction: {e}")
            return []

    def _run_email_extraction(self, linkedin_urls: List[str]) -> List[Dict[str, Any]]:
        """Run the Apify email extraction actor"""
        try:
            endpoint = f"{self.base_url}/acts/{self.actor_id}/runs"

            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }

            # Prepare actor input
            payload = {
                "profileUrls": linkedin_urls,
                "proxyConfiguration": {
                    "useApifyProxy": True
                }
            }

            logging.info(f"🚀 Starting email extraction actor (Run with {len(linkedin_urls)} URLs)")

            # Start the actor run
            response = self._make_request_with_retry(
                endpoint,
                method="POST",
                headers=headers,
                json=payload
            )

            if not response or response.status_code not in [200, 201]:
                logging.error(f"Failed to start email extraction actor: Status {response.status_code if response else 'No response'}")
                return []

            run_data = response.json()
            run_id = run_data.get('data', {}).get('id')

            if not run_id:
                logging.error("No run ID returned from email extraction actor")
                return []

            logging.info(f"⏳ Waiting for email extraction to complete (Run ID: {run_id})")

            # Wait for completion and get results
            results = self._wait_for_run_completion(run_id, headers)
            logging.info(f"📊 Email extraction actor returned {len(results)} results")

            return results

        except Exception as e:
            logging.error(f"Error running email extraction: {e}")
            return []

    def _process_extraction_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process and normalize extraction result

        Normalizes different field names and adds extraction metadata
        """
        processed = {
            "linkedinUrl": result.get("linkedinUrl") or result.get("url"),
            "email": result.get("email"),
            "mobileNumber": result.get("mobileNumber") or result.get("phone"),
            "firstName": result.get("firstName"),
            "lastName": result.get("lastName"),
            "fullName": result.get("fullName"),
            "headline": result.get("headline"),
            "jobTitle": result.get("jobTitle"),
            "companyName": result.get("companyName"),
            "companyWebsite": result.get("companyWebsite"),
            "location": result.get("addressWithCountry") or result.get("location"),
            "connections": result.get("connections"),
            "followers": result.get("followers"),
            "extraction_success": bool(result.get("email") or result.get("mobileNumber")),
            "raw_data": result
        }

        return processed

    def _wait_for_run_completion(self, run_id: str, headers: dict) -> List[Dict[str, Any]]:
        """Wait for Apify run to complete and return results"""
        max_wait_time = 300  # 5 minutes max
        check_interval = 10   # Check every 10 seconds
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            try:
                status_url = f"{self.base_url}/acts/{self.actor_id}/runs/{run_id}"
                status_response = self._make_request_with_retry(status_url, headers=headers)

                if not status_response:
                    logging.warning("Failed to get email extraction run status")
                    time.sleep(check_interval)
                    elapsed_time += check_interval
                    continue

                run_data = status_response.json()
                run_status = run_data.get('data', {}).get('status', 'UNKNOWN')

                # Log status periodically
                if elapsed_time % 30 == 0 or run_status != 'RUNNING':
                    logging.info(f"🔄 Email extraction status: {run_status} ({elapsed_time}s elapsed)")

                if run_status == 'SUCCEEDED':
                    logging.info("✅ Email extraction completed!")

                    # Get dataset results
                    dataset_id = run_data.get('data', {}).get('defaultDatasetId')
                    if not dataset_id:
                        logging.error("No dataset ID found")
                        return []

                    dataset_url = f"{self.base_url}/datasets/{dataset_id}/items"
                    dataset_response = self._make_request_with_retry(dataset_url, headers=headers)

                    if not dataset_response:
                        logging.error("Failed to fetch email extraction results")
                        return []

                    results = dataset_response.json()
                    return results if isinstance(results, list) else []

                elif run_status == 'FAILED':
                    logging.error("Email extraction run failed")
                    return []

                elif run_status in ['RUNNING', 'READY']:
                    time.sleep(check_interval)
                    elapsed_time += check_interval

            except Exception as e:
                logging.error(f"Error checking email extraction status: {e}")
                return []

        logging.error("Email extraction timed out")
        return []

    def _make_request_with_retry(self, url: str, method: str = "GET", **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        for attempt in range(self.MAX_RETRIES):
            try:
                if method.upper() == "POST":
                    response = requests.post(url, timeout=self.REQUEST_TIMEOUT, **kwargs)
                else:
                    response = requests.get(url, timeout=self.REQUEST_TIMEOUT, **kwargs)

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

            if attempt < self.MAX_RETRIES - 1:
                time.sleep(2 ** attempt)

        return None

    def test_connection(self) -> bool:
        """Test if Apify API connection is working"""
        try:
            test_url = f"{self.base_url}/acts/{self.actor_id}"
            headers = {"Authorization": f"Bearer {self.api_key}"}

            response = requests.get(test_url, headers=headers, timeout=10)

            if response.status_code == 200:
                actor_data = response.json().get('data', {})
                actor_name = actor_data.get('name', 'Unknown')
                logging.info(f"✅ Email Extraction Actor API connection successful")
                logging.info(f"   Actor: {actor_name} (ID: {self.actor_id})")
                return True
            else:
                logging.error(f"❌ API test failed: {response.status_code}")
                return False

        except Exception as e:
            logging.error(f"❌ API test error: {e}")
            return False


# Example usage
if __name__ == "__main__":
    import sys
    import json
    from pathlib import Path

    # Load app state
    app_state_path = Path(__file__).parent.parent.parent / '.app-state.json'
    with open(app_state_path, 'r') as f:
        app_state = json.load(f)

    apify_key = app_state['apiKeys']['apify_api_key']

    extractor = LinkedInEmailExtractor(apify_key=apify_key)

    # Test connection
    if not extractor.test_connection():
        print("Failed to connect to Apify API")
        sys.exit(1)

    # Test with sample LinkedIn URLs
    test_urls = [
        "https://www.linkedin.com/in/williamhgates",  # Bill Gates (known to have email)
        "https://www.linkedin.com/in/test-profile"      # Test profile
    ]

    results = extractor.extract_emails_batch(test_urls)

    print("\n" + "=" * 80)
    print("EMAIL EXTRACTION TEST RESULTS")
    print("=" * 80)

    for result in results:
        print(f"\nProfile: {result['fullName']} ({result['linkedinUrl']})")
        print(f"  Email: {result['email'] or 'Not found'}")
        print(f"  Phone: {result['mobileNumber'] or 'Not found'}")
        print(f"  Headline: {result['headline']}")
        print(f"  Success: {result['extraction_success']}")

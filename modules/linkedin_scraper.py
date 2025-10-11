"""
LinkedIn Enrichment Module
Finds and extracts decision maker information from LinkedIn profiles
Uses Apify for both Google search and LinkedIn scraping
"""

import requests
import logging
import time
import re
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote, urlparse

# Try to import config, but provide defaults if not available (for testing)
try:
    from ..config import APIFY_API_KEY as DEFAULT_APIFY_KEY, MAX_RETRIES as DEFAULT_MAX_RETRIES, REQUEST_TIMEOUT as DEFAULT_REQUEST_TIMEOUT
except (ImportError, ValueError):
    DEFAULT_APIFY_KEY = None
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_REQUEST_TIMEOUT = 30

class LinkedInScraper:
    # Class-level defaults
    MAX_RETRIES = DEFAULT_MAX_RETRIES
    REQUEST_TIMEOUT = DEFAULT_REQUEST_TIMEOUT

    def __init__(self, apify_key: str = None, actor_id: str = None):
        """
        Initialize LinkedIn scraper with Apify API

        Args:
            apify_key: Apify API key (required for actual usage)
            actor_id: ID of the LinkedIn scraper actor on Apify
        """
        self.api_key = apify_key or DEFAULT_APIFY_KEY
        self.base_url = "https://api.apify.com/v2"

        # Actor IDs
        # OLD: self.google_search_actor = "nwua9Gu5YrADL7ZDj"  # Google Search Scraper (DEPRECATED)
        # FIXED: Use working Google Search actor
        self.google_search_actor = "apify~google-search-scraper"  # Official Google Search Scraper
        self.linkedin_actor = actor_id or "bebity~linkedin-premium-actor"  # Default to bebity actor

    def enrich_with_linkedin(self, businesses: List[Dict[str, Any]],
                            max_businesses: int = 25) -> List[Dict[str, Any]]:
        """
        Enrich multiple businesses with LinkedIn data

        Args:
            businesses: List of business dictionaries with name, city, website
            max_businesses: Maximum number of businesses to process (default 25 to avoid rate limits)

        Returns:
            List of LinkedIn enrichment results
        """
        enriched_results = []
        businesses_to_process = businesses[:max_businesses]

        logging.info(f"🔗 Starting LinkedIn enrichment for {len(businesses_to_process)} businesses")

        for idx, business in enumerate(businesses_to_process, 1):
            business_name = business.get('name') or business.get('title', '')
            city = business.get('city', '')

            logging.info(f"\n[{idx}/{len(businesses_to_process)}] Processing: {business_name}")

            # Step 1: Find LinkedIn presence
            linkedin_url = self.find_linkedin_url(business_name, city)

            if not linkedin_url:
                logging.warning(f"  ❌ No LinkedIn found for {business_name}")
                enriched_results.append({
                    'business_id': business.get('id'),
                    'business_name': business_name,
                    'linkedin_found': False,
                    'error': 'No LinkedIn profile found'
                })
                continue

            # Step 2: Determine profile type
            profile_type = self._determine_profile_type(linkedin_url)
            logging.info(f"  📄 Found {profile_type} page: {linkedin_url}")

            # Step 3: Scrape LinkedIn profile(s)
            profiles = self.scrape_linkedin_profiles(linkedin_url, profile_type)

            if not profiles:
                logging.warning(f"  ⚠️ Could not scrape LinkedIn for {business_name}")
                enriched_results.append({
                    'business_id': business.get('id'),
                    'business_name': business_name,
                    'linkedin_url': linkedin_url,
                    'linkedin_found': True,
                    'profile_type': profile_type,
                    'error': 'Failed to scrape LinkedIn profile'
                })
                continue

            # Step 4: Process profiles and extract contact info
            for profile in profiles:
                enrichment = self._process_linkedin_profile(
                    profile,
                    business,
                    linkedin_url,
                    profile_type
                )
                enriched_results.append(enrichment)
                logging.info(f"  ✅ Extracted: {enrichment.get('person_name', 'Unknown')} - {enrichment.get('person_title', 'N/A')}")

            # Rate limiting between businesses (7s to avoid 403 errors)
            if idx < len(businesses_to_process):
                time.sleep(7)
                logging.info(f"  ⏳ Rate limiting: waiting 7s before next business...")

        logging.info(f"\n📊 LinkedIn enrichment complete: {len(enriched_results)} profiles found")
        return enriched_results

    def find_linkedin_url(self, business_name: str, city: str) -> Optional[str]:
        """
        Search Google for LinkedIn presence of a business

        Args:
            business_name: Name of the business
            city: City location

        Returns:
            LinkedIn URL if found, None otherwise
        """
        try:
            # Build search query
            search_query = f'"{business_name}" site:linkedin.com {city}'
            logging.info(f"  🔍 Searching: {search_query}")

            # Use Apify Google Search - Fixed API format
            endpoint = f"{self.base_url}/acts/{self.google_search_actor}/runs"

            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }

            # Fixed: Use correct input format for apify~google-search-scraper
            payload = {
                "queries": search_query,
                "maxPagesPerQuery": 1,
                "resultsPerPage": 10,
                "languageCode": "en",
                "countryCode": "us"
            }

            # Start the search
            response = self._make_request_with_retry(
                endpoint,
                method="POST",
                headers=headers,
                json=payload
            )

            if not response or response.status_code not in [200, 201]:
                logging.error(f"  ❌ Failed to start Google search")
                return None

            run_data = response.json()
            run_id = run_data.get('data', {}).get('id')

            if not run_id:
                return None

            # Wait for completion
            results = self._wait_for_run_completion(run_id, headers, "Google Search")

            # Brief delay after Google Search to avoid rate limits
            time.sleep(2)

            # Extract LinkedIn URL from results
            for result in results:
                # Handle both old format (organicResults) and new format (searchResults)
                search_results = result.get('organicResults', []) or result.get('searchResults', {}).get('results', [])

                for item in search_results:
                    url = item.get('url', '')
                    if 'linkedin.com' in url and ('/in/' in url or '/company/' in url):
                        return self._clean_linkedin_url(url)

            return None

        except Exception as e:
            logging.error(f"  ❌ Error searching for LinkedIn: {e}")
            return None

    def scrape_linkedin_profiles(self, linkedin_url: str, profile_type: str) -> List[Dict[str, Any]]:
        """
        Scrape LinkedIn profile(s) using Apify

        Args:
            linkedin_url: LinkedIn URL to scrape
            profile_type: 'company' or 'personal'

        Returns:
            List of profile data dictionaries
        """
        try:
            endpoint = f"{self.base_url}/acts/{self.linkedin_actor}/runs"

            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }

            # Configure based on profile type
            # Fixed: bebity actor requires 'action' parameter - different for companies vs profiles
            if profile_type == 'company':
                # For company pages - use 'get-companies' action
                payload = {
                    "action": "get-companies",  # Returns company info
                    "keywords": [linkedin_url],
                    "isUrl": True,
                    "isName": False,
                    "limit": 1
                }
            else:
                # For personal profiles - use 'get-profiles' action
                payload = {
                    "action": "get-profiles",  # Returns personal profile info
                    "keywords": [linkedin_url],
                    "isUrl": True,
                    "isName": False,
                    "limit": 1
                }

            logging.info(f"  🚀 Starting LinkedIn scrape...")

            # Start the scraper
            response = self._make_request_with_retry(
                endpoint,
                method="POST",
                headers=headers,
                json=payload
            )

            if not response or response.status_code not in [200, 201]:
                logging.error(f"  ❌ Failed to start LinkedIn scraper")
                return []

            run_data = response.json()
            run_id = run_data.get('data', {}).get('id')

            if not run_id:
                return []

            # Wait for completion
            results = self._wait_for_run_completion(run_id, headers, "LinkedIn")

            # Filter for relevant profiles (owners, managers, directors)
            if profile_type == 'company' and results:
                filtered_results = self._filter_key_people(results)
                return filtered_results if filtered_results else results[:3]  # Return top 3 if no key people found

            return results

        except Exception as e:
            logging.error(f"  ❌ Error scraping LinkedIn: {e}")
            return []

    def _process_linkedin_profile(self, profile: Dict[str, Any],
                                 business: Dict[str, Any],
                                 linkedin_url: str,
                                 profile_type: str) -> Dict[str, Any]:
        """
        Process LinkedIn profile data and extract contact information
        Handles both company and personal profile formats
        """
        # Extract person details - different fields for companies vs profiles
        if profile_type == 'company':
            # Company format: {name, industry, websiteUrl, employeeCount, etc}
            person_name = profile.get('name', '')
            person_title = profile.get('industry', '') or 'Company'
        else:
            # Personal profile format: {firstName, lastName, headline, EXPERIENCE, etc}
            first_name = profile.get('firstName', '')
            last_name = profile.get('lastName', '')
            person_name = f"{first_name} {last_name}".strip() or profile.get('name', '')
            person_title = profile.get('headline', '') or profile.get('title', '')

        # Extract emails
        emails_found = []

        # Check various email fields
        email_fields = ['email', 'personalEmail', 'workEmail', 'contactEmail']
        for field in email_fields:
            email = profile.get(field)
            if email and email not in emails_found:
                emails_found.append(email)

        # Generate email patterns if we have domain but no direct email
        emails_generated = []
        website = business.get('website') or business.get('website_url', '')

        if not emails_found and website and person_name:
            emails_generated = self._generate_email_patterns(person_name, website)

        # Determine primary email (prefer found over generated)
        primary_email = emails_found[0] if emails_found else (emails_generated[0] if emails_generated else None)
        email_source = 'linkedin_direct' if emails_found else ('generated' if emails_generated else None)

        # Extract profile URL - different fields for companies vs profiles
        if profile_type == 'company':
            person_profile_url = profile.get('url', '') or linkedin_url
        else:
            person_profile_url = profile.get('url', '') or linkedin_url

        return {
            'business_id': business.get('id'),
            'business_name': business.get('name') or business.get('title', ''),
            'linkedin_url': linkedin_url,
            'profile_type': profile_type,
            'linkedin_found': True,
            'person_name': person_name,
            'person_title': person_title,
            'person_profile_url': person_profile_url,
            'emails_found': emails_found,
            'emails_generated': emails_generated,
            'primary_email': primary_email,
            'email_source': email_source,
            'phone': profile.get('phone'),
            'company': profile.get('company', {}).get('name', '') if isinstance(profile.get('company'), dict) else '',
            'location': profile.get('location') or profile.get('headquarter', {}).get('city', '') if isinstance(profile.get('headquarter'), dict) else '',
            'connections': profile.get('connections') or profile.get('followerCount'),
            'raw_profile_data': profile
        }

    def _determine_profile_type(self, linkedin_url: str) -> str:
        """Determine if LinkedIn URL is a company page or personal profile"""
        if '/company/' in linkedin_url:
            return 'company'
        elif '/in/' in linkedin_url:
            return 'personal'
        else:
            return 'unknown'

    def _clean_linkedin_url(self, url: str) -> str:
        """Clean and standardize LinkedIn URL"""
        # Remove tracking parameters
        url = url.split('?')[0]
        # Ensure https
        if not url.startswith('http'):
            url = 'https://' + url
        return url

    def _filter_key_people(self, profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter LinkedIn profiles for key decision makers"""
        key_titles = [
            'owner', 'founder', 'ceo', 'president', 'director',
            'manager', 'chief', 'head', 'principal', 'partner'
        ]

        key_people = []
        for profile in profiles:
            title = (profile.get('headline', '') or profile.get('title', '')).lower()
            if any(key_title in title for key_title in key_titles):
                key_people.append(profile)

        # Sort by importance (owner/founder > CEO > director > manager)
        def title_priority(profile):
            title = (profile.get('headline', '') or profile.get('title', '')).lower()
            if 'owner' in title or 'founder' in title:
                return 1
            elif 'ceo' in title or 'chief executive' in title:
                return 2
            elif 'president' in title:
                return 3
            elif 'director' in title:
                return 4
            elif 'manager' in title:
                return 5
            return 6

        key_people.sort(key=title_priority)
        return key_people

    def _generate_email_patterns(self, full_name: str, website: str) -> List[str]:
        """
        Generate common email patterns based on name and domain

        Args:
            full_name: Person's full name
            website: Business website URL

        Returns:
            List of potential email addresses
        """
        try:
            # Extract domain from website
            domain = urlparse(website).netloc
            if not domain:
                return []
            domain = domain.replace('www.', '')

            # Skip social media domains
            social_domains = ['facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com']
            if any(social in domain for social in social_domains):
                return []

            # Parse name
            name_parts = full_name.strip().split()
            if not name_parts:
                return []

            first_name = name_parts[0].lower()
            last_name = name_parts[-1].lower() if len(name_parts) > 1 else ''

            # Generate common patterns
            patterns = []

            if last_name:
                patterns.extend([
                    f"{first_name}@{domain}",                    # john@example.com
                    f"{first_name}.{last_name}@{domain}",       # john.doe@example.com
                    f"{first_name[0]}{last_name}@{domain}",     # jdoe@example.com
                    f"{first_name}{last_name}@{domain}",        # johndoe@example.com
                    f"{last_name}@{domain}",                    # doe@example.com
                    f"{first_name[0]}.{last_name}@{domain}",    # j.doe@example.com
                ])
            else:
                patterns.append(f"{first_name}@{domain}")

            # Add common role-based emails as fallback
            patterns.extend([
                f"contact@{domain}",
                f"info@{domain}"
            ])

            return patterns[:5]  # Return top 5 patterns

        except Exception as e:
            logging.debug(f"Error generating email patterns: {e}")
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

    def _wait_for_run_completion(self, run_id: str, headers: dict, source: str) -> List[Dict[str, Any]]:
        """Wait for Apify run to complete and return results"""
        max_wait_time = 300  # 5 minutes max
        check_interval = 5   # Check every 5 seconds
        elapsed_time = 0

        # Determine actor ID based on source
        if source == "Google Search":
            actor_id = self.google_search_actor
        else:  # LinkedIn
            actor_id = self.linkedin_actor

        while elapsed_time < max_wait_time:
            try:
                status_url = f"{self.base_url}/acts/{actor_id}/runs/{run_id}"
                status_response = self._make_request_with_retry(status_url, headers=headers)

                if not status_response:
                    logging.warning(f"  ⚠️ Failed to get {source} run status")
                    time.sleep(check_interval)
                    elapsed_time += check_interval
                    continue

                run_data = status_response.json()
                run_status = run_data.get('data', {}).get('status', 'UNKNOWN')

                if run_status == 'SUCCEEDED':
                    # Get dataset results
                    dataset_id = run_data.get('data', {}).get('defaultDatasetId')
                    if not dataset_id:
                        logging.error(f"  ❌ No dataset ID found for {source}")
                        return []

                    dataset_url = f"{self.base_url}/datasets/{dataset_id}/items"
                    dataset_response = self._make_request_with_retry(dataset_url, headers=headers)

                    if not dataset_response:
                        logging.error(f"  ❌ Failed to fetch {source} results")
                        return []

                    results = dataset_response.json()
                    return results if isinstance(results, list) else []

                elif run_status == 'FAILED':
                    logging.error(f"  ❌ {source} scrape failed")
                    return []

                elif run_status in ['RUNNING', 'READY']:
                    time.sleep(check_interval)
                    elapsed_time += check_interval

            except Exception as e:
                logging.error(f"  ❌ Error checking {source} status: {e}")
                return []

        logging.error(f"  ❌ {source} scrape timed out")
        return []

    def test_connection(self) -> bool:
        """Test if Apify API connection is working"""
        try:
            test_url = f"{self.base_url}/acts"
            headers = {"Authorization": f"Bearer {self.api_key}"}

            response = requests.get(test_url, headers=headers, timeout=10)

            if response.status_code == 200:
                logging.info("✅ LinkedIn Scraper API connection successful")
                return True
            else:
                logging.error(f"❌ API test failed: {response.status_code}")
                return False

        except Exception as e:
            logging.error(f"❌ API test error: {e}")
            return False
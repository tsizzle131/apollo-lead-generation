"""
PARALLEL BATCH LinkedIn Enrichment Module
Processes multiple batches simultaneously for maximum speed
Estimated: 103-172x faster than sequential (3-5 minutes for 321 businesses)

HYBRID MODE: Combines verified email extraction + pattern generation
"""

import requests
import logging
import time
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Import the LinkedIn email extractor and Bouncer verifier for hybrid mode
from .linkedin_email_extractor import LinkedInEmailExtractor
from .bouncer_verifier import BouncerVerifier

class LinkedInScraperParallel:
    """Parallel batch LinkedIn scraper - processes multiple batches at once"""

    MAX_RETRIES = 3
    REQUEST_TIMEOUT = 30
    MAX_PARALLEL_BATCHES = 3  # Process 3 batches simultaneously

    def __init__(self, apify_key: str = None, actor_id: str = None, bouncer_key: str = None):
        self.api_key = apify_key
        self.base_url = "https://api.apify.com/v2"
        self.google_search_actor = "apify~google-search-scraper"
        self.linkedin_actor = actor_id or "bebity~linkedin-premium-actor"
        self.lock = threading.Lock()  # Thread-safe logging

        # Initialize email extractor for hybrid mode
        self.email_extractor = LinkedInEmailExtractor(apify_key=apify_key) if apify_key else None

        # Initialize Bouncer verifier for email validation
        self.bouncer_verifier = BouncerVerifier(api_key=bouncer_key) if bouncer_key else None

    def enrich_with_linkedin_parallel(self, businesses: List[Dict[str, Any]],
                                      max_businesses: int = 500,
                                      batch_size: int = 15,
                                      max_parallel: int = 3) -> List[Dict[str, Any]]:
        """
        PARALLEL: Process multiple batches of businesses simultaneously

        Performance: ~3-5 minutes for 321 businesses (vs 15 minutes batch, vs 8.6 hours sequential)

        Args:
            businesses: List of business dictionaries
            max_businesses: Maximum businesses to process (default 500)
            batch_size: Businesses per batch (default 15)
            max_parallel: Number of batches to process simultaneously (default 3)

        Returns:
            List of LinkedIn enrichment results
        """
        businesses_to_process = businesses[:max_businesses]
        all_results = []

        logging.info(f"🚀 PARALLEL PROCESSING: {len(businesses_to_process)} businesses")
        logging.info(f"   Batch size: {batch_size}")
        logging.info(f"   Parallel batches: {max_parallel}")

        # Split into batches
        batches = []
        for i in range(0, len(businesses_to_process), batch_size):
            batch = businesses_to_process[i:i + batch_size]
            batches.append((i // batch_size + 1, batch))

        total_batches = len(batches)
        logging.info(f"   Total batches: {total_batches}")

        start_time = time.time()

        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            # Submit all batch jobs
            future_to_batch = {
                executor.submit(self._process_single_batch, batch_num, batch): (batch_num, batch)
                for batch_num, batch in batches
            }

            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_batch):
                batch_num, batch = future_to_batch[future]
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                    completed += 1

                    elapsed = time.time() - start_time
                    avg_per_batch = elapsed / completed
                    remaining_batches = total_batches - completed
                    eta_seconds = avg_per_batch * remaining_batches

                    with self.lock:
                        logging.info(f"✅ Batch {batch_num}/{total_batches} complete ({completed}/{total_batches})")
                        logging.info(f"   ⏱️  ETA: {eta_seconds:.0f}s ({eta_seconds/60:.1f} min)")

                except Exception as e:
                    with self.lock:
                        logging.error(f"❌ Batch {batch_num} failed: {e}")

        total_time = time.time() - start_time
        logging.info(f"\n🎉 PARALLEL PROCESSING COMPLETE")
        logging.info(f"   Total time: {total_time:.1f}s ({total_time/60:.1f} min)")
        logging.info(f"   Results: {len(all_results)}")

        return all_results

    def enrich_with_linkedin_hybrid(self, businesses: List[Dict[str, Any]],
                                    max_businesses: int = 500,
                                    batch_size: int = 15,
                                    max_parallel: int = 3) -> List[Dict[str, Any]]:
        """
        HYBRID MODE: Three-tier email enrichment strategy

        Tier 2: LinkedIn verified emails (8% success) - Extracted from public profiles
        Tier 3: LinkedIn pattern generation (40% success) - Generated from name + domain

        (Tier 1 Facebook emails handled separately in facebook_scraper.py)

        Performance: ~5-8 minutes for 321 businesses (includes email extraction step)

        Args:
            businesses: List of business dictionaries
            max_businesses: Maximum businesses to process (default 500)
            batch_size: Businesses per batch (default 15)
            max_parallel: Number of batches to process simultaneously (default 3)

        Returns:
            List of LinkedIn enrichment results with verified + generated emails
        """
        if not self.email_extractor:
            logging.warning("⚠️  Email extractor not initialized - falling back to pattern generation only")
            return self.enrich_with_linkedin_parallel(businesses, max_businesses, batch_size, max_parallel)

        logging.info(f"\n🎯 HYBRID ENRICHMENT MODE")
        logging.info(f"   Strategy: Verified emails (Tier 2) → Pattern generation (Tier 3)")

        # STEP 1: Get LinkedIn profiles using parallel processing
        profile_results = self.enrich_with_linkedin_parallel(
            businesses, max_businesses, batch_size, max_parallel
        )

        # STEP 2: Extract verified emails from profiles with LinkedIn URLs
        logging.info(f"\n📧 EXTRACTING VERIFIED EMAILS FROM LINKEDIN PROFILES")

        profiles_with_linkedin = [r for r in profile_results if r.get('linkedin_url')]
        logging.info(f"   Profiles with LinkedIn URLs: {len(profiles_with_linkedin)}")

        if not profiles_with_linkedin:
            logging.info("   No LinkedIn profiles to extract emails from")
            return profile_results

        # Batch extract emails (process in chunks to avoid overwhelming the actor)
        linkedin_urls = [r['linkedin_url'] for r in profiles_with_linkedin]

        try:
            email_extraction_results = self.email_extractor.extract_emails_batch(linkedin_urls)

            # Map extraction results back to profiles
            extraction_map = {
                self._clean_linkedin_url(r['linkedinUrl']): r
                for r in email_extraction_results
                if r.get('linkedinUrl')
            }

            # STEP 3: Merge extraction results with profile data
            verified_count = 0
            generated_count = 0
            phone_count = 0

            for result in profile_results:
                linkedin_url = result.get('linkedin_url')
                if not linkedin_url:
                    # No LinkedIn profile - mark as Tier 5 (not found)
                    result['email_quality_tier'] = 5
                    result['email_extraction_attempted'] = False
                    continue

                # Mark that we attempted extraction
                result['email_extraction_attempted'] = True

                # Look up extracted email
                normalized_url = self._clean_linkedin_url(linkedin_url)
                extracted_data = extraction_map.get(normalized_url, {})

                verified_email = extracted_data.get('email')
                verified_phone = extracted_data.get('mobileNumber')

                if verified_email:
                    # Tier 2: Verified email from LinkedIn
                    result['primary_email'] = verified_email
                    result['email_source'] = 'linkedin_verified'
                    result['email_verified_source'] = 'linkedin_public'
                    result['email_quality_tier'] = 2
                    result['emails_found'] = [verified_email]
                    verified_count += 1

                    if verified_phone:
                        result['phone_number'] = verified_phone
                        result['phone_numbers'] = [verified_phone]
                        phone_count += 1

                    logging.info(f"   ✅ Verified email found: {verified_email[:20]}...")

                elif result.get('emails_generated') and len(result.get('emails_generated', [])) > 0:
                    # Tier 3: Pattern generated email (from existing logic)
                    result['email_quality_tier'] = 4
                    result['email_verified_source'] = 'pattern_generated'
                    generated_count += 1

                else:
                    # Tier 5: No email found
                    result['email_quality_tier'] = 5
                    result['email_verified_source'] = 'not_found'

            # Summary before Bouncer verification
            logging.info(f"\n📊 HYBRID ENRICHMENT SUMMARY (Pre-Verification):")
            logging.info(f"   Total profiles processed: {len(profile_results)}")
            logging.info(f"   Verified emails (Tier 2): {verified_count} ({verified_count/len(profiles_with_linkedin)*100:.1f}%)")
            logging.info(f"   Generated emails (Tier 4): {generated_count} ({generated_count/len(profiles_with_linkedin)*100:.1f}%)")
            logging.info(f"   Phone numbers found: {phone_count}")
            logging.info(f"   Total with emails: {verified_count + generated_count}")

            # STEP 4: Bouncer email verification
            if self.bouncer_verifier:
                logging.info(f"\n🔍 BOUNCER EMAIL VERIFICATION")

                # Collect all emails to verify
                emails_to_verify = []
                email_to_result_map = {}

                for result in profile_results:
                    email = result.get('primary_email')
                    if email:
                        emails_to_verify.append(email)
                        email_to_result_map[email] = result

                logging.info(f"   Verifying {len(emails_to_verify)} emails...")

                try:
                    # Batch verify all emails
                    verification_results = self.bouncer_verifier.verify_batch(emails_to_verify)

                    # Map verification results back to profile results
                    for verification in verification_results:
                        email = verification.get('email')
                        result = email_to_result_map.get(email)

                        if result:
                            # Add Bouncer verification data
                            result['bouncer_status'] = verification.get('status')
                            result['bouncer_score'] = verification.get('score')
                            result['bouncer_verified'] = verification.get('verified')
                            result['bouncer_is_safe'] = verification.get('is_safe')
                            result['bouncer_is_deliverable'] = verification.get('is_deliverable')
                            result['bouncer_is_risky'] = verification.get('is_risky')
                            result['bouncer_reason'] = verification.get('reason')
                            result['bouncer_verified_at'] = verification.get('verified_at')
                            result['bouncer_raw_response'] = verification.get('raw_response')

                    # Count verification results
                    deliverable_count = sum(1 for v in verification_results if v.get('status') == 'deliverable')
                    risky_count = sum(1 for v in verification_results if v.get('status') == 'risky')
                    undeliverable_count = sum(1 for v in verification_results if v.get('status') == 'undeliverable')

                    logging.info(f"\n📊 BOUNCER VERIFICATION SUMMARY:")
                    logging.info(f"   ✅ Deliverable: {deliverable_count}")
                    logging.info(f"   ⚠️  Risky: {risky_count}")
                    logging.info(f"   ❌ Undeliverable: {undeliverable_count}")

                except Exception as e:
                    logging.error(f"❌ Bouncer verification error: {e}")
                    logging.warning("   Continuing without verification data")
            else:
                logging.warning(f"\n⚠️  Bouncer verifier not initialized - skipping email verification")
                logging.warning(f"   Add bouncer_api_key to enable verification")

            return profile_results

        except Exception as e:
            logging.error(f"❌ Error in email extraction: {e}")
            logging.warning("   Falling back to pattern generation only")

            # Mark extraction as attempted but failed
            for result in profile_results:
                if result.get('linkedin_url'):
                    result['email_extraction_attempted'] = True
                    if result.get('emails_generated'):
                        result['email_quality_tier'] = 4
                    else:
                        result['email_quality_tier'] = 5

            return profile_results

    def _process_single_batch(self, batch_num: int, businesses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a single batch of businesses (called from thread pool)

        This is the same logic as the optimized batch scraper, but designed to run in parallel
        """
        batch_results = []

        with self.lock:
            logging.info(f"\n📦 Batch {batch_num}: Processing {len(businesses)} businesses")

        # STEP 1: Batch Google Search
        linkedin_urls_map = self._batch_find_linkedin_urls(businesses, batch_num)

        found_count = sum(1 for url in linkedin_urls_map.values() if url)
        with self.lock:
            logging.info(f"  Batch {batch_num}: Found {found_count}/{len(businesses)} LinkedIn URLs")

        # STEP 2: Batch LinkedIn Scraping
        profiles_map = self._batch_scrape_linkedin_profiles(linkedin_urls_map, batch_num)

        scraped_count = sum(1 for profiles in profiles_map.values() if profiles)
        with self.lock:
            logging.info(f"  Batch {batch_num}: Scraped {scraped_count} profiles")

        # STEP 3: Process results
        for business in businesses:
            business_id = business.get('id')
            business_name = business.get('name') or business.get('title', '')

            linkedin_url = linkedin_urls_map.get(business_id)

            if not linkedin_url:
                batch_results.append({
                    'business_id': business_id,
                    'business_name': business_name,
                    'linkedin_found': False,
                    'error': 'No LinkedIn profile found'
                })
                continue

            profiles = profiles_map.get(business_id, [])

            if not profiles:
                batch_results.append({
                    'business_id': business_id,
                    'business_name': business_name,
                    'linkedin_url': linkedin_url,
                    'linkedin_found': True,
                    'error': 'Failed to scrape LinkedIn profile'
                })
                continue

            # Process each profile found
            for profile in profiles:
                profile_type = self._determine_profile_type(linkedin_url)
                enrichment = self._process_linkedin_profile(
                    profile, business, linkedin_url, profile_type
                )
                batch_results.append(enrichment)

        return batch_results

    def _batch_find_linkedin_urls(self, businesses: List[Dict[str, Any]], batch_num: int) -> Dict[str, Optional[str]]:
        """
        Batch find LinkedIn URLs for multiple businesses (thread-safe version)
        """
        business_map = {b.get('id'): b for b in businesses}

        # Build batch search queries
        search_queries = []
        business_ids = []

        for business in businesses:
            business_name = business.get('name') or business.get('title', '')
            city = business.get('city', '')
            query = f'"{business_name}" site:linkedin.com {city}'
            search_queries.append(query)
            business_ids.append(business.get('id'))

        # Join with newlines for batch processing
        batch_query = "\n".join(search_queries)

        try:
            endpoint = f"{self.base_url}/acts/{self.google_search_actor}/runs"
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }

            payload = {
                "queries": batch_query,
                "maxPagesPerQuery": 1,
                "resultsPerPage": 10,
                "languageCode": "en",
                "countryCode": "us"
            }

            # Start batch search
            response = self._make_request_with_retry(
                endpoint, method="POST", headers=headers, json=payload
            )

            if not response or response.status_code not in [200, 201]:
                with self.lock:
                    logging.error(f"  Batch {batch_num}: Failed to start Google search")
                return {bid: None for bid in business_ids}

            run_id = response.json().get('data', {}).get('id')
            if not run_id:
                return {bid: None for bid in business_ids}

            # Wait for completion
            results = self._wait_for_run_completion(run_id, headers, "Google Search", batch_num)

            # Map results back to businesses
            linkedin_urls = {}
            for idx, business_id in enumerate(business_ids):
                linkedin_url = None

                if idx < len(results):
                    result = results[idx]
                    search_results = result.get('organicResults', []) or \
                                   result.get('searchResults', {}).get('results', [])

                    for item in search_results:
                        url = item.get('url', '')
                        if 'linkedin.com' in url and ('/in/' in url or '/company/' in url):
                            linkedin_url = self._clean_linkedin_url(url)
                            break

                linkedin_urls[business_id] = linkedin_url

            return linkedin_urls

        except Exception as e:
            with self.lock:
                logging.error(f"  Batch {batch_num}: Google search error: {e}")
            return {bid: None for bid in business_ids}

    def _batch_scrape_linkedin_profiles(self, linkedin_urls_map: Dict[str, Optional[str]],
                                       batch_num: int) -> Dict[str, List[Dict]]:
        """
        Batch scrape multiple LinkedIn profiles (thread-safe version)
        """
        # Filter out None URLs and group by type
        company_urls = {}
        personal_urls = {}

        for business_id, url in linkedin_urls_map.items():
            if not url:
                continue

            if '/company/' in url:
                company_urls[business_id] = url
            elif '/in/' in url:
                personal_urls[business_id] = url

        results = {}

        # Batch scrape company pages
        if company_urls:
            company_results = self._batch_scrape_by_type(
                company_urls, profile_type='company', batch_num=batch_num
            )
            results.update(company_results)

        # Batch scrape personal profiles
        if personal_urls:
            personal_results = self._batch_scrape_by_type(
                personal_urls, profile_type='personal', batch_num=batch_num
            )
            results.update(personal_results)

        return results

    def _batch_scrape_by_type(self, url_map: Dict[str, str],
                             profile_type: str, batch_num: int) -> Dict[str, List[Dict]]:
        """Batch scrape LinkedIn profiles of the same type (thread-safe)"""

        try:
            endpoint = f"{self.base_url}/acts/{self.linkedin_actor}/runs"
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }

            # Batch payload with multiple URLs
            urls = list(url_map.values())

            if profile_type == 'company':
                payload = {
                    "action": "get-companies",
                    "keywords": urls,
                    "isUrl": True,
                    "isName": False,
                    "limit": len(urls)
                }
            else:
                payload = {
                    "action": "get-profiles",
                    "keywords": urls,
                    "isUrl": True,
                    "isName": False,
                    "limit": len(urls)
                }

            # Start batch scraper
            response = self._make_request_with_retry(
                endpoint, method="POST", headers=headers, json=payload
            )

            if not response or response.status_code not in [200, 201]:
                with self.lock:
                    logging.error(f"  Batch {batch_num}: Failed to start LinkedIn scraper ({profile_type})")
                return {bid: [] for bid in url_map.keys()}

            run_id = response.json().get('data', {}).get('id')
            if not run_id:
                return {bid: [] for bid in url_map.keys()}

            # Wait for completion
            results = self._wait_for_run_completion(run_id, headers, "LinkedIn", batch_num)

            # Map results back to business IDs
            profiles_map = {}
            url_to_business = {url: bid for bid, url in url_map.items()}

            for profile in results:
                profile_url = profile.get('url', '') or profile.get('publicIdentifier', '')

                # Normalize profile URL for matching
                normalized_profile_url = self._clean_linkedin_url(profile_url) if profile_url else ''

                # Find matching business by normalized URL
                for url, business_id in url_to_business.items():
                    normalized_search_url = self._clean_linkedin_url(url)

                    # Match on normalized URLs
                    if normalized_search_url == normalized_profile_url or \
                       normalized_search_url in normalized_profile_url or \
                       normalized_profile_url in normalized_search_url:
                        if business_id not in profiles_map:
                            profiles_map[business_id] = []
                        profiles_map[business_id].append(profile)
                        break

            return profiles_map

        except Exception as e:
            with self.lock:
                logging.error(f"  Batch {batch_num}: LinkedIn scrape error ({profile_type}): {e}")
            return {bid: [] for bid in url_map.keys()}

    def _process_linkedin_profile(self, profile: Dict[str, Any], business: Dict[str, Any],
                                  linkedin_url: str, profile_type: str) -> Dict[str, Any]:
        """Extract contact information from LinkedIn profile"""

        # Extract person details
        if profile_type == 'company':
            person_name = profile.get('name', '')
            person_title = profile.get('industry', '') or 'Company'
        else:
            first_name = profile.get('firstName', '')
            last_name = profile.get('lastName', '')
            person_name = f"{first_name} {last_name}".strip() or profile.get('name', '')
            person_title = profile.get('headline', '') or profile.get('title', '')

        # Extract emails
        emails_found = []
        email_fields = ['email', 'personalEmail', 'workEmail', 'contactEmail']
        for field in email_fields:
            email = profile.get(field)
            if email and email not in emails_found:
                emails_found.append(email)

        # Generate email patterns if needed
        emails_generated = []
        website = business.get('website') or business.get('website_url', '')

        if not emails_found and website and person_name:
            emails_generated = self._generate_email_patterns(person_name, website)

        primary_email = emails_found[0] if emails_found else (emails_generated[0] if emails_generated else None)
        email_source = 'linkedin_direct' if emails_found else ('generated' if emails_generated else None)

        return {
            'business_id': business.get('id'),
            'business_name': business.get('name') or business.get('title', ''),
            'linkedin_url': linkedin_url,
            'profile_type': profile_type,
            'linkedin_found': True,
            'person_name': person_name,
            'person_title': person_title,
            'person_profile_url': profile.get('url', '') or linkedin_url,
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
        return 'unknown'

    def _clean_linkedin_url(self, url: str) -> str:
        """Clean and standardize LinkedIn URL"""
        url = url.split('?')[0]  # Remove query params
        url = url.rstrip('/')     # Remove trailing slash
        if not url.startswith('http'):
            url = 'https://' + url
        # Normalize www vs non-www
        url = url.replace('https://www.linkedin.com/', 'https://linkedin.com/')
        url = url.replace('http://www.linkedin.com/', 'https://linkedin.com/')
        return url

    def _generate_email_patterns(self, full_name: str, website: str) -> List[str]:
        """Generate common email patterns based on name and domain"""
        try:
            # Skip Google Maps URLs - these are not real business websites
            if 'google.com/maps' in website:
                return []

            domain = urlparse(website).netloc
            if not domain:
                return []
            domain = domain.replace('www.', '')

            # Skip social media and other non-business domains
            invalid_domains = ['facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'google.com', 'youtube.com']
            if any(invalid in domain for invalid in invalid_domains):
                return []

            name_parts = full_name.strip().split()
            if not name_parts:
                return []

            first_name = name_parts[0].lower()
            last_name = name_parts[-1].lower() if len(name_parts) > 1 else ''

            patterns = []
            if last_name:
                patterns.extend([
                    f"{first_name}@{domain}",
                    f"{first_name}.{last_name}@{domain}",
                    f"{first_name[0]}{last_name}@{domain}",
                    f"{first_name}{last_name}@{domain}",
                    f"{last_name}@{domain}",
                    f"{first_name[0]}.{last_name}@{domain}",
                ])
            else:
                patterns.append(f"{first_name}@{domain}")

            patterns.extend([f"contact@{domain}", f"info@{domain}"])
            return patterns[:5]

        except Exception as e:
            logging.debug(f"Error generating email patterns: {e}")
            return []

    def _make_request_with_retry(self, url: str, method: str = "GET", **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retry logic and exponential backoff (thread-safe logging)"""
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
                    with self.lock:
                        logging.warning(f"⚠️  Rate limited by Apify (429), waiting {wait_time}s before retry {attempt + 1}/{self.MAX_RETRIES}")
                        logging.warning(f"   URL: {url}")
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 401:
                    with self.lock:
                        logging.error(f"❌ Authentication failed (401) - Invalid or expired API key")
                        logging.error(f"   URL: {url}")
                    return None
                elif response.status_code == 404:
                    with self.lock:
                        logging.error(f"❌ Resource not found (404)")
                        logging.error(f"   URL: {url}")
                        logging.error(f"   Actor ID may be invalid: {self.linkedin_actor or self.google_search_actor}")
                    return None
                elif response.status_code >= 500:
                    wait_time = 2 ** attempt
                    with self.lock:
                        logging.warning(f"⚠️  Server error ({response.status_code}), retrying in {wait_time}s (attempt {attempt + 1}/{self.MAX_RETRIES})")
                        logging.warning(f"   URL: {url}")
                    if attempt < self.MAX_RETRIES - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        return None
                else:
                    with self.lock:
                        logging.warning(f"⚠️  Request failed with status {response.status_code} (attempt {attempt + 1}/{self.MAX_RETRIES})")
                        logging.warning(f"   URL: {url}")
                    if attempt < self.MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        return None

            except requests.exceptions.Timeout as e:
                wait_time = 2 ** attempt
                with self.lock:
                    logging.warning(f"⚠️  Request timeout after {self.REQUEST_TIMEOUT}s (attempt {attempt + 1}/{self.MAX_RETRIES})")
                    logging.warning(f"   URL: {url}")
                    logging.warning(f"   Error: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(wait_time)
                    continue
            except requests.exceptions.ConnectionError as e:
                wait_time = 2 ** attempt
                with self.lock:
                    logging.warning(f"⚠️  Connection error (attempt {attempt + 1}/{self.MAX_RETRIES})")
                    logging.warning(f"   URL: {url}")
                    logging.warning(f"   Error: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(wait_time)
                    continue
            except requests.exceptions.RequestException as e:
                with self.lock:
                    logging.warning(f"⚠️  Request error (attempt {attempt + 1}/{self.MAX_RETRIES})")
                    logging.warning(f"   URL: {url}")
                    logging.warning(f"   Error type: {type(e).__name__}")
                    logging.warning(f"   Error: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue

        with self.lock:
            logging.error(f"❌ All {self.MAX_RETRIES} retry attempts failed")
            logging.error(f"   URL: {url}")
        return None

    def _wait_for_run_completion(self, run_id: str, headers: dict, source: str,
                                 batch_num: int) -> List[Dict[str, Any]]:
        """Wait for Apify run to complete and return results with fail-fast error handling (thread-safe)"""
        max_wait_time = 120  # 2 minutes (already reasonable for batch operations)
        check_interval = 5
        elapsed_time = 0
        consecutive_running = 0
        max_consecutive_running = 24  # 2 minutes of stuck RUNNING state (24 * 5s)
        last_status = None

        actor_id = self.google_search_actor if source == "Google Search" else self.linkedin_actor

        while elapsed_time < max_wait_time:
            try:
                status_url = f"{self.base_url}/acts/{actor_id}/runs/{run_id}"
                status_response = self._make_request_with_retry(status_url, headers=headers)

                if not status_response:
                    with self.lock:
                        logging.warning(f"  Batch {batch_num}: Failed to get {source} run status (attempt {elapsed_time // check_interval})")
                        logging.warning(f"    Run ID: {run_id}")
                        logging.warning(f"    Actor ID: {actor_id}")
                    time.sleep(check_interval)
                    elapsed_time += check_interval
                    continue

                try:
                    run_data = status_response.json()
                except ValueError as e:
                    with self.lock:
                        logging.error(f"  Batch {batch_num}: Invalid JSON response from Apify API")
                        logging.error(f"    Source: {source}")
                        logging.error(f"    Run ID: {run_id}")
                        logging.error(f"    Error: {e}")
                    return []

                run_status = run_data.get('data', {}).get('status', 'UNKNOWN')

                # Log status changes (thread-safe)
                if run_status != last_status and run_status not in ['RUNNING', 'READY']:
                    with self.lock:
                        logging.info(f"  Batch {batch_num}: {source} status: {run_status}")

                if run_status == 'SUCCEEDED':
                    dataset_id = run_data.get('data', {}).get('defaultDatasetId')
                    if not dataset_id:
                        with self.lock:
                            logging.error(f"  Batch {batch_num}: No dataset ID found in successful {source} run")
                            logging.error(f"    Run ID: {run_id}")
                        return []

                    dataset_url = f"{self.base_url}/datasets/{dataset_id}/items"
                    dataset_response = self._make_request_with_retry(dataset_url, headers=headers)

                    if not dataset_response:
                        with self.lock:
                            logging.error(f"  Batch {batch_num}: Failed to fetch {source} dataset results")
                            logging.error(f"    Dataset ID: {dataset_id}")
                            logging.error(f"    Run ID: {run_id}")
                        return []

                    try:
                        results = dataset_response.json()
                    except ValueError as e:
                        with self.lock:
                            logging.error(f"  Batch {batch_num}: Invalid JSON in {source} dataset response")
                            logging.error(f"    Dataset ID: {dataset_id}")
                            logging.error(f"    Error: {e}")
                        return []

                    return results if isinstance(results, list) else []

                elif run_status == 'FAILED':
                    error_message = run_data.get('data', {}).get('statusMessage', 'No error message')
                    with self.lock:
                        logging.error(f"  Batch {batch_num}: {source} scrape failed")
                        logging.error(f"    Run ID: {run_id}")
                        logging.error(f"    Actor ID: {actor_id}")
                        logging.error(f"    Error: {error_message}")
                    return []

                elif run_status == 'ABORTED':
                    with self.lock:
                        logging.error(f"  Batch {batch_num}: {source} scrape was aborted")
                        logging.error(f"    Run ID: {run_id}")
                        logging.error(f"    Actor may have been manually stopped or exceeded limits")
                    return []

                elif run_status == 'TIMED-OUT':
                    with self.lock:
                        logging.error(f"  Batch {batch_num}: {source} scrape timed out on Apify's side")
                        logging.error(f"    Run ID: {run_id}")
                        logging.error(f"    Actor exceeded its execution time limit")
                    return []

                elif run_status in ['RUNNING', 'READY']:
                    # Track consecutive RUNNING states to detect stuck actors
                    if run_status == 'RUNNING':
                        consecutive_running += 1

                        # Fail fast if stuck in RUNNING for too long
                        if consecutive_running >= max_consecutive_running:
                            with self.lock:
                                logging.error(f"  Batch {batch_num}: {source} actor stuck in RUNNING state for {consecutive_running * check_interval}s")
                                logging.error(f"    Run ID: {run_id}")
                                logging.error(f"    Actor ID: {actor_id}")
                                logging.error(f"    Aborting to prevent indefinite hang")
                                logging.error(f"    This usually indicates the actor is stalled or encountering rate limits")
                            return []
                    else:
                        consecutive_running = 0  # Reset counter if status changes

                    time.sleep(check_interval)
                    elapsed_time += check_interval

                else:
                    # Unknown status - log and continue
                    with self.lock:
                        logging.warning(f"  Batch {batch_num}: Unknown {source} run status: {run_status}")
                        logging.warning(f"    Run ID: {run_id}")
                    time.sleep(check_interval)
                    elapsed_time += check_interval

                last_status = run_status

            except requests.exceptions.Timeout as e:
                with self.lock:
                    logging.error(f"  Batch {batch_num}: Timeout while checking {source} run status")
                    logging.error(f"    Run ID: {run_id}")
                    logging.error(f"    Error: {e}")
                return []
            except requests.exceptions.ConnectionError as e:
                with self.lock:
                    logging.error(f"  Batch {batch_num}: Connection error while checking {source} run status")
                    logging.error(f"    Run ID: {run_id}")
                    logging.error(f"    Error: {e}")
                return []
            except Exception as e:
                with self.lock:
                    logging.error(f"  Batch {batch_num}: Unexpected error checking {source} status")
                    logging.error(f"    Run ID: {run_id}")
                    logging.error(f"    Actor ID: {actor_id}")
                    logging.error(f"    Error type: {type(e).__name__}")
                    logging.error(f"    Error: {e}")
                return []

        with self.lock:
            logging.error(f"  Batch {batch_num}: {source} scrape timed out after {max_wait_time}s")
            logging.error(f"    Run ID: {run_id}")
            logging.error(f"    Actor ID: {actor_id}")
        return []

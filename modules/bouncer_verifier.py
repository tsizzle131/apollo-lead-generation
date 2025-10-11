"""
Bouncer Email Verification Module
Verifies email deliverability using UseBouncer API
Protects sender reputation by validating emails before use
"""

import requests
import logging
import time
from typing import List, Dict, Any, Optional
from datetime import datetime

class BouncerVerifier:
    def __init__(self, api_key: str = None):
        """
        Initialize Bouncer email verifier

        Args:
            api_key: Bouncer API key (get from https://app.usebouncer.com)
        """
        self.api_key = api_key
        self.base_url = "https://api.usebouncer.com/v1.1"  # Fixed: v1.1 endpoint
        self.session = requests.Session()

        if self.api_key:
            self.session.headers.update({
                'x-api-key': self.api_key,
                'Content-Type': 'application/json'
            })

    def verify_email(self, email: str) -> Dict[str, Any]:
        """
        Verify a single email address

        Args:
            email: Email address to verify

        Returns:
            Verification result with status, score, and details
        """
        try:
            if not self.api_key:
                logging.warning("⚠️ Bouncer API key not configured, skipping verification")
                return {
                    'email': email,
                    'status': 'unknown',
                    'reason': 'API key not configured',
                    'verified': False
                }

            # Single email verification endpoint - use GET with query params
            url = f"{self.base_url}/email/verify"

            params = {
                'email': email
            }

            response = self.session.get(url, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()

                # Process Bouncer response
                return self._process_verification_result(email, data)

            elif response.status_code == 401:
                logging.error("❌ Invalid Bouncer API key")
                return {
                    'email': email,
                    'status': 'error',
                    'reason': 'Invalid API key',
                    'verified': False
                }

            elif response.status_code == 429:
                logging.warning("⚠️ Bouncer rate limit reached")
                return {
                    'email': email,
                    'status': 'error',
                    'reason': 'Rate limit exceeded',
                    'verified': False
                }

            else:
                logging.error(f"❌ Bouncer API error: {response.status_code}")
                return {
                    'email': email,
                    'status': 'error',
                    'reason': f'API error: {response.status_code}',
                    'verified': False
                }

        except Exception as e:
            logging.error(f"❌ Error verifying email {email}: {e}")
            return {
                'email': email,
                'status': 'error',
                'reason': str(e),
                'verified': False
            }

    def verify_batch(self, emails: List[str], max_batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        Verify multiple emails using single-email API (synchronous)

        Note: Bouncer's batch API is async and requires polling.
        For real-time verification, we use the synchronous single-email endpoint.

        Args:
            emails: List of email addresses to verify
            max_batch_size: Maximum emails per batch request (rate limiting)

        Returns:
            List of verification results
        """
        if not emails:
            return []

        results = []
        unique_emails = list(set(emails))  # Remove duplicates

        logging.info(f"🔍 Verifying {len(unique_emails)} unique emails with Bouncer (real-time)")

        # Verify each email individually for immediate results
        for i, email in enumerate(unique_emails, 1):
            logging.info(f"   [{i}/{len(unique_emails)}] Verifying {email}...")
            result = self.verify_email(email)
            results.append(result)

            # Rate limiting: 10 requests/second max
            if i < len(unique_emails):
                time.sleep(0.1)

        # Summary statistics
        deliverable = sum(1 for r in results if r.get('status') == 'deliverable')
        undeliverable = sum(1 for r in results if r.get('status') == 'undeliverable')
        risky = sum(1 for r in results if r.get('status') == 'risky')
        unknown = sum(1 for r in results if r.get('status') == 'unknown')

        logging.info(f"📊 Verification complete:")
        logging.info(f"   ✅ Deliverable: {deliverable}")
        logging.info(f"   ❌ Undeliverable: {undeliverable}")
        logging.info(f"   ⚠️ Risky: {risky}")
        logging.info(f"   ❓ Unknown: {unknown}")

        return results

    def _verify_batch_request(self, emails: List[str]) -> List[Dict[str, Any]]:
        """Send batch verification request to Bouncer"""
        try:
            if not self.api_key:
                return [
                    {
                        'email': email,
                        'status': 'unknown',
                        'reason': 'API key not configured',
                        'verified': False
                    }
                    for email in emails
                ]

            # Batch verification endpoint - use POST with JSON body
            url = f"{self.base_url}/email/verify/batch"

            # Bouncer expects array of objects with email field
            payload = [{"email": email} for email in emails]

            response = self.session.post(url, json=payload, timeout=60)

            if response.status_code == 200:
                data = response.json()

                # Debug: Log the response structure
                logging.info(f"🔍 Bouncer API response keys: {list(data.keys())}")
                logging.info(f"🔍 Response type: {type(data)}")

                results = []

                # Check if response is a list (direct array of results)
                if isinstance(data, list):
                    logging.info(f"🔍 Processing {len(data)} results from array response")
                    for email_result in data:
                        processed = self._process_verification_result(
                            email_result.get('email'),
                            email_result
                        )
                        results.append(processed)
                # Check if response has 'results' key
                elif 'results' in data:
                    logging.info(f"🔍 Processing {len(data['results'])} results from results key")
                    for email_result in data.get('results', []):
                        processed = self._process_verification_result(
                            email_result.get('email'),
                            email_result
                        )
                        results.append(processed)
                else:
                    logging.warning(f"⚠️ Unexpected Bouncer response format: {data}")

                return results

            else:
                logging.error(f"❌ Batch verification failed: {response.status_code}")
                return [
                    {
                        'email': email,
                        'status': 'error',
                        'reason': f'Batch API error: {response.status_code}',
                        'verified': False
                    }
                    for email in emails
                ]

        except Exception as e:
            logging.error(f"❌ Batch verification error: {e}")
            return [
                {
                    'email': email,
                    'status': 'error',
                    'reason': str(e),
                    'verified': False
                }
                for email in emails
            ]

    def _process_verification_result(self, email: str, bouncer_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process Bouncer API response into standardized format

        Bouncer status codes:
        - deliverable: Email is valid and deliverable
        - undeliverable: Email is invalid or undeliverable
        - risky: Email might be deliverable but risky (disposable, role-based, etc.)
        - unknown: Could not determine deliverability
        """
        status = bouncer_data.get('status', 'unknown')
        score = bouncer_data.get('score', 0)

        # Determine if email should be used
        is_safe = status == 'deliverable' and score >= 70

        result = {
            'email': email,
            'status': status,
            'score': score,
            'verified': True,  # Verification was performed
            'is_safe': is_safe,  # Safe to use for outreach
            'is_deliverable': status == 'deliverable',
            'is_risky': status == 'risky',

            # Detailed flags
            'is_disposable': bouncer_data.get('is_disposable', False),
            'is_role_based': bouncer_data.get('is_role', False),
            'is_free_email': bouncer_data.get('is_free', False),
            'is_gibberish': bouncer_data.get('is_gibberish', False),

            # Additional info
            'domain': bouncer_data.get('domain', ''),
            'provider': bouncer_data.get('provider', ''),
            'mx_records': bouncer_data.get('mx_records', False),
            'smtp_check': bouncer_data.get('smtp_check', False),

            # Error details if any
            'reason': bouncer_data.get('reason', ''),
            'suggestion': bouncer_data.get('did_you_mean', ''),

            # Metadata
            'verified_at': datetime.now().isoformat(),
            'raw_response': bouncer_data
        }

        # Log verification result
        if is_safe:
            logging.info(f"  ✅ {email} - Deliverable (Score: {score})")
        elif status == 'undeliverable':
            logging.warning(f"  ❌ {email} - Undeliverable: {result['reason']}")
        elif status == 'risky':
            logging.warning(f"  ⚠️ {email} - Risky: {self._get_risk_reasons(result)}")
        else:
            logging.warning(f"  ❓ {email} - Unknown status")

        return result

    def _get_risk_reasons(self, result: Dict[str, Any]) -> str:
        """Get human-readable risk reasons"""
        risks = []
        if result.get('is_disposable'):
            risks.append('disposable')
        if result.get('is_role_based'):
            risks.append('role-based')
        if result.get('is_gibberish'):
            risks.append('gibberish')
        if not result.get('mx_records'):
            risks.append('no MX records')

        return ', '.join(risks) if risks else 'unknown risk'

    def filter_safe_emails(self, verification_results: List[Dict[str, Any]]) -> List[str]:
        """
        Filter only safe emails from verification results

        Args:
            verification_results: List of verification result dictionaries

        Returns:
            List of safe email addresses
        """
        safe_emails = []

        for result in verification_results:
            if result.get('is_safe', False):
                safe_emails.append(result['email'])

        return safe_emails

    def get_best_email(self, verification_results: List[Dict[str, Any]]) -> Optional[str]:
        """
        Get the best email from a list of verification results

        Prioritizes:
        1. Deliverable emails with highest score
        2. Risky emails if no deliverable ones
        3. None if all are undeliverable

        Args:
            verification_results: List of verification result dictionaries

        Returns:
            Best email address or None
        """
        if not verification_results:
            return None

        # Sort by status priority and score
        def email_priority(result):
            status_priority = {
                'deliverable': 0,
                'risky': 1,
                'unknown': 2,
                'undeliverable': 3,
                'error': 4
            }
            return (
                status_priority.get(result.get('status', 'error'), 5),
                -result.get('score', 0)  # Negative for descending order
            )

        sorted_results = sorted(verification_results, key=email_priority)

        best_result = sorted_results[0]

        # Only return if it's at least risky or better
        if best_result.get('status') in ['deliverable', 'risky']:
            return best_result.get('email')

        return None

    def test_connection(self) -> bool:
        """Test if Bouncer API connection is working"""
        try:
            if not self.api_key:
                logging.error("❌ Bouncer API key not configured")
                return False

            # Test with a known good email format
            test_result = self.verify_email('test@example.com')

            if test_result.get('verified'):
                logging.info("✅ Bouncer API connection successful")
                return True
            else:
                logging.error(f"❌ Bouncer API test failed: {test_result.get('reason')}")
                return False

        except Exception as e:
            logging.error(f"❌ Bouncer API test error: {e}")
            return False

    def get_usage_stats(self) -> Dict[str, Any]:
        """Get API usage statistics from Bouncer"""
        try:
            if not self.api_key:
                return {'error': 'API key not configured'}

            url = f"{self.base_url}/account"
            response = self.session.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                return {
                    'credits_remaining': data.get('credits', 0),
                    'credits_used': data.get('credits_used', 0),
                    'plan': data.get('plan', 'unknown'),
                    'status': 'active'
                }
            else:
                return {'error': f'API error: {response.status_code}'}

        except Exception as e:
            return {'error': str(e)}
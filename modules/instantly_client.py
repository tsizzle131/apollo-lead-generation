"""
Instantly.ai API Client
Simple export of leads to Instantly campaigns
"""

import requests
import logging
from typing import List, Dict, Any, Optional

class InstantlyClient:
    """Client for Instantly.ai API v2 - Campaign Export"""

    BASE_URL = "https://api.instantly.ai/api/v2"

    def __init__(self, api_key: str):
        """
        Initialize Instantly.ai client

        Args:
            api_key: Instantly.ai API key (Bearer token) - Use as-is (base64 encoded)
        """
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })
        logging.info("✅ Instantly.ai client initialized")

    def get_accounts(self) -> List[Dict[str, Any]]:
        """
        Get all sending email accounts from Instantly workspace

        Returns:
            List of email account objects with IDs
        """
        try:
            response = self.session.get(
                f"{self.BASE_URL}/accounts",
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            # Log the raw response for debugging (using ERROR to ensure visibility)
            logging.error(f"🔍 DEBUG: Raw API response type: {type(data)}")
            logging.error(f"🔍 DEBUG: Raw API response: {data}")

            # Handle different response formats
            if isinstance(data, dict):
                # Response might be wrapped in 'items', 'data', or 'accounts' field
                accounts = data.get('items', data.get('data', data.get('accounts', [])))
                logging.error(f"🔍 DEBUG: Extracted from dict - accounts: {len(accounts)} found")
            elif isinstance(data, list):
                accounts = data
                logging.error(f"🔍 DEBUG: Response is already a list: {accounts}")
            else:
                logging.error(f"❌ Unexpected response format: {type(data)}")
                logging.error(f"❌ Response: {data}")
                raise ValueError(f"Unexpected response format from Instantly API: {type(data)}")

            logging.error(f"🔍 DEBUG: Found {len(accounts)} email accounts total")
            if accounts:
                logging.error(f"🔍 DEBUG: First account structure: {accounts[0]}")
            else:
                logging.warning(f"⚠️ No accounts found in response")

            return accounts

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Failed to get accounts: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"❌ Response: {e.response.text}")
            raise

    def create_lead_list(self, name: str, organization_id: str) -> str:
        """
        Create a new lead list in Instantly

        Args:
            name: Name for the lead list
            organization_id: Organization ID from your Instantly account

        Returns:
            Lead list ID
        """
        try:
            payload = {
                "name": name,
                "organization_id": organization_id
            }

            response = self.session.post(
                f"{self.BASE_URL}/leadlist",
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            result = response.json()
            list_id = result.get("id")

            logging.info(f"✅ Created lead list: {list_id}")
            return list_id

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Failed to create lead list: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"❌ Response: {e.response.text}")
            raise

    def create_campaign(self, name: str,
                       account_ids: List[str],
                       timezone: str = "America/Chicago",
                       hours_from: str = "09:00",
                       hours_to: str = "17:00") -> Dict[str, Any]:
        """
        Create a new campaign in Instantly.ai with all required fields

        Args:
            name: Campaign name
            account_ids: List of sending account IDs
            timezone: IANA timezone (default: America/Chicago)
            hours_from: Start time (default: 09:00)
            hours_to: End time (default: 17:00)

        Returns:
            Campaign details with ID
        """
        # Default email sequence template using custom variables
        default_sequence = [
            {
                "steps": [
                    {
                        "type": "email",
                        "position": 1,
                        "delay": 0,
                        "variants": [],
                        "subject": "{{subject_line}}",
                        "body": """Hi {{first_name}},

{{icebreaker}}

I noticed {{business_name}} in {{business_location}} and wanted to reach out.

Would you be open to a quick chat about how we might be able to help?

Best regards"""
                    }
                ]
            }
        ]

        # Build complete payload with all required fields
        payload = {
            "name": name,
            "accounts": account_ids,
            "sequences": default_sequence,
            "status": 0,  # 0 = Draft, 1 = Active
            "campaign_schedule": {
                "schedules": [
                    {
                        "name": "Business Hours",
                        "timing": {
                            "from": hours_from,
                            "to": hours_to
                        },
                        "days": {
                            "monday": True,
                            "tuesday": True,
                            "wednesday": True,
                            "thursday": True,
                            "friday": True
                        },
                        "timezone": timezone
                    }
                ]
            }
        }

        try:
            logging.info(f"📋 Creating campaign: {name}")
            logging.info(f"   Accounts: {len(account_ids)} sending accounts")

            response = self.session.post(
                f"{self.BASE_URL}/campaigns",
                json=payload,
                timeout=30
            )

            logging.info(f"🔍 Response Status: {response.status_code}")

            response.raise_for_status()

            result = response.json()
            campaign_id = result.get('id')
            logging.info(f"✅ Created Instantly campaign: {campaign_id}")
            return result

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Failed to create campaign: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"❌ Response Status: {e.response.status_code}")
                logging.error(f"❌ Response Body: {e.response.text}")
            raise

    def bulk_add_leads(self, campaign_id: str,
                       leads: List[Dict[str, Any]],
                       batch_size: int = 500) -> Dict[str, Any]:
        """
        Add leads individually to a campaign (Instantly API requires individual POST requests)

        Uses /api/v2/leads endpoint - one request per lead with campaign_id in payload.

        Args:
            campaign_id: Instantly campaign ID
            leads: List of lead objects with email and custom variables
            batch_size: Ignored - kept for backward compatibility

        Returns:
            Import results with counts
        """
        import time

        total_leads = len(leads)
        logging.info(f"📤 Exporting {total_leads} leads to Instantly campaign {campaign_id}")

        successful = 0
        failed = 0
        failed_leads = []

        # Process leads individually (Instantly API has no batch endpoint)
        for idx, lead in enumerate(leads, start=1):
            # Add campaign ID to lead payload (API uses "campaign" not "campaign_id")
            payload = {
                "campaign": campaign_id,
                **lead  # Merge lead data (email, first_name, last_name, etc.)
            }

            try:
                response = self.session.post(
                    f"{self.BASE_URL}/leads",
                    json=payload,
                    timeout=30
                )
                response.raise_for_status()

                successful += 1

                # Log progress every 10 leads
                if idx % 10 == 0 or idx == total_leads:
                    logging.info(f"  ✅ Progress: {idx}/{total_leads} leads added ({successful} successful, {failed} failed)")

                # Small delay to avoid rate limiting (100ms between requests)
                if idx < total_leads:
                    time.sleep(0.1)

            except requests.exceptions.RequestException as e:
                failed += 1

                error_detail = {
                    "email": lead.get("email", "unknown"),
                    "error": str(e)
                }

                if hasattr(e, 'response') and e.response is not None:
                    error_detail["status_code"] = e.response.status_code
                    try:
                        error_detail["response"] = e.response.json()
                    except:
                        error_detail["response"] = e.response.text[:200]

                failed_leads.append(error_detail)

                logging.error(f"  ❌ Lead {idx} failed ({lead.get('email', 'unknown')}): {error_detail}")
                continue

        # Summary logging
        logging.info(f"📊 Import complete: {successful} successful, {failed} failed")

        if failed > 0:
            logging.error(f"  ❌ {failed} leads failed total")
            logging.error(f"  ❌ Sample failed emails: {[l['email'] for l in failed_leads[:5]]}")

        return {
            "total_leads": total_leads,
            "successful": successful,
            "failed": failed,
            "failed_leads": failed_leads
        }

    def format_lead_for_instantly(self, business: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format a business record for Instantly lead import

        Args:
            business: Business record from gmaps_businesses table

        Returns:
            Formatted lead object with custom variables
        """
        # Extract business info
        business_name = business.get("name", "Business")
        email = business.get("email", "")
        city = business.get("city", "")
        state = business.get("state", "")

        # Build location string
        location = ", ".join(filter(None, [city, state]))

        # Format lead with custom variables for personalization
        lead = {
            "email": email,
            "first_name": business_name,
            "last_name": "Team",
            "company_name": business_name,
            "custom_variables": {
                # AI-generated personalization
                "icebreaker": business.get("icebreaker", ""),
                "subject_line": business.get("subject_line", f"Quick question for {business_name}"),

                # Business details for template variables
                "business_name": business_name,
                "business_category": business.get("category", ""),
                "business_location": location,
                "business_city": city,
                "business_state": state,
                "business_rating": str(business.get("rating", "")),
                "business_reviews": str(business.get("reviews_count", "")),
                "business_phone": business.get("phone", ""),
                "business_website": business.get("website", ""),
                "business_address": business.get("address", "")
            }
        }

        return lead

    def export_campaign(self, campaign_name: str,
                       businesses: List[Dict[str, Any]],
                       timezone: str = "America/Chicago",
                       hours_from: str = "09:00",
                       hours_to: str = "17:00") -> Dict[str, Any]:
        """
        Complete export workflow: Create campaign + Add leads automatically

        This method handles the full export process:
        1. Get sending accounts from Instantly
        2. Create a lead list
        3. Create campaign with default sequence
        4. Add all leads with custom variables

        Args:
            campaign_name: Name for the Instantly campaign
            businesses: List of business records to export
            timezone: IANA timezone (default: America/Chicago)
            hours_from: Start time for sending
            hours_to: End time for sending

        Returns:
            Export summary with campaign ID and stats
        """
        logging.info("="*60)
        logging.info(f"🚀 EXPORTING TO INSTANTLY.AI: {campaign_name}")
        logging.info("="*60)

        # Step 1: Get sending accounts
        logging.info("\n📧 Step 1: Getting sending accounts...")
        accounts = self.get_accounts()

        if not accounts:
            raise ValueError("No sending email accounts found in Instantly. Please add email accounts first.")

        # Use all available accounts - handle different response formats
        account_ids = []
        organization_id = None

        for acc in accounts:
            if isinstance(acc, dict):
                # Extract organization_id from first account
                if organization_id is None and 'organization' in acc:
                    organization_id = acc['organization']

                # Account is a dictionary - extract email or id
                account_id = acc.get('email') or acc.get('id')
                if account_id:
                    account_ids.append(account_id)
            elif isinstance(acc, str):
                # Account is already a string (email address)
                account_ids.append(acc)
            else:
                logging.warning(f"⚠️ Unexpected account format: {type(acc)} - {acc}")

        if not account_ids:
            raise ValueError("Could not extract any valid account IDs from Instantly accounts")

        if not organization_id:
            raise ValueError("Could not extract organization ID from Instantly accounts")

        logging.info(f"   ✅ Using {len(account_ids)} sending accounts: {account_ids}")
        logging.info(f"   ✅ Organization ID: {organization_id}")

        # Step 2: Create campaign
        logging.info("\n🎯 Step 2: Creating campaign...")
        campaign = self.create_campaign(
            name=campaign_name,
            account_ids=account_ids,
            timezone=timezone,
            hours_from=hours_from,
            hours_to=hours_to
        )
        campaign_id = campaign.get("id")

        # Step 3: Format leads
        logging.info(f"\n📝 Step 3: Formatting {len(businesses)} leads...")
        leads = []
        skipped = 0

        for business in businesses:
            # Only export businesses with emails
            if business.get("email"):
                lead = self.format_lead_for_instantly(business)
                leads.append(lead)
            else:
                skipped += 1

        logging.info(f"  ✅ {len(leads)} leads formatted ({skipped} skipped - no email)")

        # Step 4: Bulk import
        logging.info(f"\n📤 Step 4: Importing leads to campaign...")
        import_result = self.bulk_add_leads(campaign_id, leads)

        # Summary
        summary = {
            "success": True,
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "campaign_url": f"https://app.instantly.ai/app/campaigns/{campaign_id}",
            "total_businesses": len(businesses),
            "leads_exported": import_result["successful"],
            "leads_failed": import_result["failed"],
            "leads_skipped": skipped
        }

        logging.info("\n" + "="*60)
        logging.info("✅ EXPORT COMPLETE")
        logging.info("="*60)
        logging.info(f"Campaign ID: {campaign_id}")
        logging.info(f"Campaign URL: {summary['campaign_url']}")
        logging.info(f"Leads exported: {summary['leads_exported']}/{len(businesses)}")
        logging.info("="*60)

        return summary


# Example usage
if __name__ == "__main__":
    # Test with sample data
    client = InstantlyClient(api_key="your-api-key")

    sample_businesses = [
        {
            "name": "Acme Cafe",
            "email": "contact@acmecafe.com",
            "category": "cafe",
            "city": "Los Angeles",
            "state": "CA",
            "rating": 4.8,
            "reviews_count": 127,
            "icebreaker": "Hey - saw you're a cafe in LA with 4.8 stars...",
            "subject_line": "Quick Q for Acme Cafe"
        }
    ]

    result = client.export_campaign(
        campaign_name="Test Campaign",
        businesses=sample_businesses
    )

    print(f"Export result: {result}")

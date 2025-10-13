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
            api_key: Instantly.ai API key (Bearer token)
        """
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })
        logging.info("✅ Instantly.ai client initialized")

    def create_campaign(self, name: str,
                       timezone: str = "America/Los_Angeles",
                       hours_from: str = "09:00",
                       hours_to: str = "17:00") -> Dict[str, Any]:
        """
        Create a new campaign in Instantly.ai

        Args:
            name: Campaign name
            timezone: Timezone for sending (default: America/Los_Angeles)
            hours_from: Start time (default: 09:00)
            hours_to: End time (default: 17:00)

        Returns:
            Campaign details with ID
        """
        payload = {
            "name": name,
            "campaign_schedule": {
                "schedules": [
                    {
                        "name": "Business Hours",
                        "timing": {
                            "from": hours_from,
                            "to": hours_to
                        },
                        "days": {},
                        "timezone": timezone
                    }
                ]
            }
        }

        try:
            response = self.session.post(
                f"{self.BASE_URL}/campaigns",
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            result = response.json()
            logging.info(f"✅ Created Instantly campaign: {result.get('id')}")
            return result

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Failed to create campaign: {e}")
            raise

    def bulk_add_leads(self, campaign_id: str,
                       leads: List[Dict[str, Any]],
                       batch_size: int = 100) -> Dict[str, Any]:
        """
        Add leads in bulk to a campaign

        Args:
            campaign_id: Instantly campaign ID
            leads: List of lead objects with email and custom variables
            batch_size: Number of leads per batch (default: 100)

        Returns:
            Import results with counts
        """
        total_leads = len(leads)
        logging.info(f"📤 Exporting {total_leads} leads to Instantly campaign {campaign_id}")

        # Process in batches to avoid API limits
        results = []
        successful = 0
        failed = 0

        for i in range(0, total_leads, batch_size):
            batch = leads[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_leads + batch_size - 1) // batch_size

            logging.info(f"  Batch {batch_num}/{total_batches}: {len(batch)} leads")

            payload = {
                "campaign_id": campaign_id,
                "leads": batch
            }

            try:
                response = self.session.post(
                    f"{self.BASE_URL}/leads",
                    json=payload,
                    timeout=60
                )
                response.raise_for_status()

                result = response.json()
                results.append(result)
                successful += len(batch)
                logging.info(f"  ✅ Batch {batch_num} imported successfully")

            except requests.exceptions.RequestException as e:
                logging.error(f"  ❌ Batch {batch_num} failed: {e}")
                failed += len(batch)
                continue

        logging.info(f"📊 Import complete: {successful} successful, {failed} failed")

        return {
            "total_leads": total_leads,
            "successful": successful,
            "failed": failed,
            "batches_processed": len(results),
            "results": results
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
            "variables": {
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
                       timezone: str = "America/Los_Angeles",
                       hours_from: str = "09:00",
                       hours_to: str = "17:00") -> Dict[str, Any]:
        """
        Complete export workflow: Create campaign + Add leads

        Args:
            campaign_name: Name for the Instantly campaign
            businesses: List of business records to export
            timezone: Timezone for sending
            hours_from: Start time for sending
            hours_to: End time for sending

        Returns:
            Export summary with campaign ID and stats
        """
        logging.info("="*60)
        logging.info(f"🚀 EXPORTING TO INSTANTLY.AI: {campaign_name}")
        logging.info("="*60)

        # Step 1: Create campaign
        logging.info("\n📋 Step 1: Creating campaign...")
        campaign = self.create_campaign(
            name=campaign_name,
            timezone=timezone,
            hours_from=hours_from,
            hours_to=hours_to
        )
        campaign_id = campaign.get("id")

        # Step 2: Format leads
        logging.info(f"\n📝 Step 2: Formatting {len(businesses)} leads...")
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

        # Step 3: Bulk import
        logging.info(f"\n📤 Step 3: Importing leads to campaign...")
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

#!/usr/bin/env python3
"""
Export campaign to Instantly.ai
Called from Node.js Express API
"""

import sys
import json
import logging
import argparse
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.instantly_client import InstantlyClient
from modules.gmaps_supabase_manager import GmapsSupabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    """Export campaign to Instantly.ai with automatic campaign creation"""
    parser = argparse.ArgumentParser(description='Export campaign to Instantly.ai')
    parser.add_argument('--campaign-id', required=True, help='Campaign ID to export')
    parser.add_argument('--campaign-name', required=True, help='Name for Instantly campaign')
    parser.add_argument('--timezone', default='America/Chicago', help='Timezone (IANA format)')
    parser.add_argument('--hours-from', default='09:00', help='Start time')
    parser.add_argument('--hours-to', default='17:00', help='End time')
    parser.add_argument('--api-key', required=True, help='Instantly.ai API key')
    parser.add_argument('--supabase-url', required=True, help='Supabase URL')
    parser.add_argument('--supabase-key', required=True, help='Supabase key')
    parser.add_argument('--organization-id', help='Organization ID')

    args = parser.parse_args()

    try:
        # Initialize clients
        instantly = InstantlyClient(api_key=args.api_key)
        db = GmapsSupabaseManager(
            supabase_url=args.supabase_url,
            supabase_key=args.supabase_key,
            organization_id=args.organization_id
        )

        # Get businesses with emails from campaign
        logging.info(f"📥 Fetching businesses from campaign {args.campaign_id}")

        # Query all businesses with emails
        businesses_result = db.client.table("gmaps_businesses")\
            .select("*")\
            .eq("campaign_id", args.campaign_id)\
            .not_.is_("email", "null")\
            .execute()

        businesses = businesses_result.data

        if not businesses:
            logging.error("❌ No businesses with emails found in campaign")
            result = {
                "success": False,
                "error": "No businesses with emails to export"
            }
            print(json.dumps(result))
            sys.exit(1)

        logging.info(f"✅ Found {len(businesses)} businesses with emails")

        # Export to Instantly (automatic campaign creation)
        export_result = instantly.export_campaign(
            campaign_name=args.campaign_name,
            businesses=businesses,
            timezone=args.timezone,
            hours_from=args.hours_from,
            hours_to=args.hours_to
        )

        # Save Instantly campaign ID to database (for tracking)
        try:
            db.client.table("gmaps_campaigns")\
                .update({
                    "exported_to_instantly": True,
                    "instantly_export_date": "now()",
                    "instantly_campaign_id": export_result["campaign_id"]
                })\
                .eq("id", args.campaign_id)\
                .execute()
            logging.info("✅ Updated campaign record with export info")
        except Exception as e:
            logging.warning(f"⚠️ Could not update campaign record: {e}")

        # Output result as JSON for Node.js
        print(json.dumps(export_result))
        sys.exit(0)

    except Exception as e:
        logging.error(f"❌ Export failed: {e}")
        result = {
            "success": False,
            "error": str(e)
        }
        print(json.dumps(result))
        sys.exit(1)


if __name__ == "__main__":
    main()

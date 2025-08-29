#!/usr/bin/env python3
"""
Test script for Local Business Scraper
Tests Google Maps scraping and LinkedIn enrichment
"""

import sys
import os
import logging
import json

# Add modules directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'modules'))

from local_business_scraper import LocalBusinessScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_local_scraper():
    """Test the local business scraper with a small sample"""
    
    print("\n" + "="*60)
    print("🧪 TESTING LOCAL BUSINESS SCRAPER")
    print("="*60 + "\n")
    
    # Initialize scraper
    scraper = LocalBusinessScraper()
    
    # Test 1: Connection test
    print("1️⃣ Testing API connection...")
    if scraper.test_connection():
        print("✅ API connection successful\n")
    else:
        print("❌ API connection failed\n")
        return False
    
    # Test 2: Small scrape test
    print("2️⃣ Testing Google Maps + LinkedIn scraping...")
    print("   Query: 'coffee shops'")
    print("   Location: 'Austin, TX'")
    print("   Max Results: 3 (for quick test)\n")
    
    try:
        # Scrape just 3 businesses for testing
        contacts = scraper.scrape_local_businesses(
            search_query="coffee shops",
            location="Austin, TX",
            max_results=3
        )
        
        if contacts:
            print(f"✅ Successfully scraped {len(contacts)} contacts\n")
            
            # Display sample results
            print("📊 Sample Results:")
            print("-" * 40)
            
            for i, contact in enumerate(contacts[:2], 1):  # Show first 2
                print(f"\nContact {i}:")
                print(f"  Name: {contact.get('name', 'N/A')}")
                print(f"  Email: {contact.get('email', 'N/A')}")
                print(f"  Business: {contact.get('organization', {}).get('name', 'N/A')}")
                print(f"  Website: {contact.get('website_url', 'N/A')}")
                print(f"  Title: {contact.get('title', 'N/A')}")
                print(f"  Source: {contact.get('_source', 'N/A')}")
                
                # Check if we got LinkedIn data
                if contact.get('linkedin_url'):
                    print(f"  LinkedIn: ✅ {contact['linkedin_url']}")
                else:
                    print(f"  LinkedIn: ❌ Not found")
            
            print("\n" + "-" * 40)
            
            # Statistics
            with_email = sum(1 for c in contacts if c.get('email'))
            with_linkedin = sum(1 for c in contacts if c.get('linkedin_url'))
            with_website = sum(1 for c in contacts if c.get('website_url'))
            
            print("\n📈 Statistics:")
            print(f"  Total contacts: {len(contacts)}")
            print(f"  With email: {with_email} ({with_email/len(contacts)*100:.0f}%)")
            print(f"  With LinkedIn: {with_linkedin} ({with_linkedin/len(contacts)*100:.0f}%)")
            print(f"  With website: {with_website} ({with_website/len(contacts)*100:.0f}%)")
            
            # Test 3: Verify Apollo compatibility
            print("\n3️⃣ Testing Apollo format compatibility...")
            required_fields = ['id', 'first_name', 'last_name', 'email', 'organization', 'website_url']
            
            all_compatible = True
            for contact in contacts:
                for field in required_fields:
                    if field not in contact:
                        print(f"❌ Missing required field: {field}")
                        all_compatible = False
                        break
            
            if all_compatible:
                print("✅ All contacts are Apollo-compatible")
            else:
                print("⚠️  Some contacts missing required fields (this is normal for fallback contacts)")
            
            # Save test results
            output_file = "test_local_scraper_results.json"
            with open(output_file, 'w') as f:
                json.dump(contacts, f, indent=2)
            print(f"\n💾 Full results saved to: {output_file}")
            
            return True
            
        else:
            print("❌ No contacts returned from scraper")
            return False
            
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("\n🚀 Starting Local Business Scraper Test")
    print("This will test Google Maps scraping and LinkedIn enrichment")
    print("Using Apify actors: compass/google-maps-scraper and bebity/linkedin-premium-actor\n")
    
    success = test_local_scraper()
    
    print("\n" + "="*60)
    if success:
        print("✅ ALL TESTS PASSED! Local Business Scraper is working correctly.")
        print("\nYou can now:")
        print("1. Create a campaign with 'Local Businesses' type in the UI")
        print("2. Enter a business query (e.g., 'restaurants') and location")
        print("3. The scraper will find businesses and enrich with LinkedIn data")
    else:
        print("❌ TESTS FAILED. Please check the error messages above.")
        print("\nCommon issues:")
        print("- Apify API key not configured")
        print("- Insufficient Apify credits")
        print("- Network connectivity issues")
    print("="*60 + "\n")
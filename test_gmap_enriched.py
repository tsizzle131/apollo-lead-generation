#!/usr/bin/env python3
"""Test Google Maps with lead enrichment"""

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

print("\n" + "="*60)
print("🧪 TESTING GOOGLE MAPS WITH LEAD ENRICHMENT")
print("="*60 + "\n")

# Initialize scraper
scraper = LocalBusinessScraper()

# Test with just 2 businesses to save on costs
print("📍 Testing with: Hair Salons in Austin, TX")
print("📊 Requesting: 2 businesses (to test enrichment)\n")

try:
    # Scrape with enrichment
    contacts = scraper.scrape_local_businesses(
        search_query="hair salons",
        location="Austin, TX",
        max_results=2
    )
    
    if contacts:
        print(f"\n✅ Successfully found {len(contacts)} contacts\n")
        
        # Display results
        for i, contact in enumerate(contacts, 1):
            print(f"Contact {i}:")
            print(f"  Name: {contact.get('name', 'N/A')}")
            print(f"  Email: {contact.get('email', 'N/A')}")
            print(f"  Title: {contact.get('title', 'N/A')}")
            print(f"  Business: {contact.get('organization', {}).get('name', 'N/A')}")
            print(f"  Website: {contact.get('website_url', 'N/A')}")
            print(f"  Source: {contact.get('_source', 'N/A')}")
            
            # Check if we got enriched lead data
            if contact.get('_source') == 'google_maps_enriched':
                print(f"  ✨ ENRICHED LEAD DATA FOUND!")
                if contact.get('linkedin_url'):
                    print(f"  LinkedIn: {contact['linkedin_url']}")
            print()
        
        # Statistics
        enriched = sum(1 for c in contacts if c.get('_source') == 'google_maps_enriched')
        with_email = sum(1 for c in contacts if c.get('email'))
        
        print("\n📈 Results:")
        print(f"  Total contacts: {len(contacts)}")
        print(f"  Enriched leads: {enriched}")
        print(f"  With email: {with_email}")
        
        # Save results
        with open('test_gmap_enriched_results.json', 'w') as f:
            json.dump(contacts, f, indent=2)
        print(f"\n💾 Results saved to: test_gmap_enriched_results.json")
        
    else:
        print("❌ No contacts returned")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
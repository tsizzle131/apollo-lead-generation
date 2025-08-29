#!/usr/bin/env python3
"""Test LinkedIn enrichment for a business"""

import requests
import json
import sys
import time
sys.path.append('.')
import config

APIFY_API_KEY = config.APIFY_API_KEY

print("🔍 Testing LinkedIn enrichment for local businesses\n")

# Test with a well-known Austin business
business_name = "Mozart's Coffee Roasters"
location = "Austin"

# LinkedIn search URL
search_query = f'"{business_name}" owner OR CEO OR manager {location}'
linkedin_url = f"https://www.linkedin.com/search/results/people/?keywords={requests.utils.quote(search_query)}"

print(f"Business: {business_name}")
print(f"Location: {location}")
print(f"Search Query: {search_query}")
print(f"LinkedIn URL: {linkedin_url}\n")

# Actor ID (corrected)
actor_id = "bebity~linkedin-premium-actor"

# Try with both search and specific profile URLs
# You can find these by searching LinkedIn manually
test_urls = [
    linkedin_url,  # Search URL
    # Add some direct profile URLs if known
    # "https://www.linkedin.com/in/example-profile/"
]

# Payload for bebity LinkedIn actor
payload = {
    "urls": test_urls,
    "scrapeCompany": False,
    "scrapeEmail": True,  # Most important - get emails
    "scrapePhone": True,
    "maxResults": 10,  # Get more results
    "searchQuery": f'"{business_name}" owner manager Austin Texas',  # Try adding search query
}

print(f"📦 Payload: {json.dumps(payload, indent=2)}\n")

# Start the run
url = f"https://api.apify.com/v2/acts/{actor_id}/runs"
headers = {
    "Authorization": f"Bearer {APIFY_API_KEY}",
    "Content-Type": "application/json"
}

print("🚀 Starting LinkedIn scraper...")
response = requests.post(url, json=payload, headers=headers)

print(f"Response Status: {response.status_code}")

if response.status_code not in [200, 201]:
    print(f"❌ Failed to start LinkedIn scraper")
    print(f"Response: {response.text}")
    exit(1)

data = response.json()
run_id = data.get('data', {}).get('id')

if not run_id:
    print("❌ No run ID in response")
    exit(1)

print(f"✅ Run started with ID: {run_id}")
print("⏳ Waiting for completion (this may take 30-60 seconds)...\n")

# Wait for completion
max_wait = 120  # 2 minutes
elapsed = 0
while elapsed < max_wait:
    time.sleep(5)
    elapsed += 5
    
    status_url = f"https://api.apify.com/v2/acts/{actor_id}/runs/{run_id}"
    status_response = requests.get(status_url, headers=headers)
    
    if status_response.status_code != 200:
        print(f"❌ Failed to get status: {status_response.status_code}")
        continue
    
    status_data = status_response.json()
    status = status_data.get('data', {}).get('status')
    print(f"   Status: {status} ({elapsed}s elapsed)")
    
    if status == 'SUCCEEDED':
        print("\n✅ LinkedIn scrape completed!")
        
        # Get results
        dataset_id = status_data.get('data', {}).get('defaultDatasetId')
        if dataset_id:
            results_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
            results_response = requests.get(results_url, headers=headers)
            
            if results_response.status_code == 200:
                results = results_response.json()
                print(f"\n📊 Results: {len(results)} profiles found\n")
                
                if results:
                    for i, profile in enumerate(results[:3], 1):
                        print(f"Profile {i}:")
                        print(f"  Name: {profile.get('name', 'N/A')}")
                        print(f"  Headline: {profile.get('headline', 'N/A')}")
                        print(f"  Email: {profile.get('email', 'Not found')}")
                        print(f"  Phone: {profile.get('phone', 'Not found')}")
                        print(f"  LinkedIn: {profile.get('profileUrl', 'N/A')}")
                        print()
                    
                    # Save full results
                    with open('linkedin_enrichment_test.json', 'w') as f:
                        json.dump(results, f, indent=2)
                    print("💾 Full results saved to: linkedin_enrichment_test.json")
                else:
                    print("❌ No profiles found")
            else:
                print(f"❌ Failed to get results: {results_response.status_code}")
        break
    
    elif status == 'FAILED':
        print(f"\n❌ LinkedIn scrape failed!")
        error_msg = status_data.get('data', {}).get('statusMessage', 'Unknown error')
        print(f"Error: {error_msg}")
        break

print("\n✅ Test complete!")
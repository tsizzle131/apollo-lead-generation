#!/usr/bin/env python3
"""Test new LinkedIn scraper actor"""

import requests
import json
import sys
import time
sys.path.append('.')
import config

APIFY_API_KEY = config.APIFY_API_KEY

print("🔍 Testing new LinkedIn scraper (SR4OyownzCZLawx9r)\n")

# Actor ID
actor_id = "SR4OyownzCZLawx9r"

# Test with a well-known Austin business
business_name = "Mozart's Coffee Roasters"
location = "Austin"

# Try different payload formats to see what works
test_payloads = [
    {
        # Format 1: Similar to bebity actor
        "urls": [f"https://www.linkedin.com/search/results/people/?keywords={requests.utils.quote(f'{business_name} owner manager Austin')}"],
        "scrapeEmail": True,
        "scrapePhone": True,
        "maxResults": 5
    },
    {
        # Format 2: Search query based
        "searchQuery": f'"{business_name}" owner OR CEO OR manager {location}',
        "extractEmails": True,
        "maxResults": 5
    },
    {
        # Format 3: Minimal
        "query": f'"{business_name}" owner manager',
        "location": location
    }
]

# Try each payload format
for idx, payload in enumerate(test_payloads, 1):
    print(f"📦 Testing payload format {idx}:")
    print(json.dumps(payload, indent=2))
    print()
    
    # Start the run
    url = f"https://api.apify.com/v2/acts/{actor_id}/runs"
    headers = {
        "Authorization": f"Bearer {APIFY_API_KEY}",
        "Content-Type": "application/json"
    }
    
    print(f"🚀 Starting LinkedIn scraper with format {idx}...")
    response = requests.post(url, json=payload, headers=headers, timeout=30)
    
    print(f"Response Status: {response.status_code}")
    
    if response.status_code in [200, 201]:
        print(f"✅ Format {idx} accepted!")
        data = response.json()
        run_id = data.get('data', {}).get('id')
        
        if run_id:
            print(f"Run ID: {run_id}")
            print("⏳ Waiting 30 seconds for completion...\n")
            
            # Wait a bit
            time.sleep(30)
            
            # Check status
            status_url = f"https://api.apify.com/v2/acts/{actor_id}/runs/{run_id}"
            status_response = requests.get(status_url, headers=headers)
            
            if status_response.status_code == 200:
                status_data = status_response.json()
                status = status_data.get('data', {}).get('status')
                print(f"Status: {status}")
                
                if status == 'SUCCEEDED':
                    # Get results
                    dataset_id = status_data.get('data', {}).get('defaultDatasetId')
                    if dataset_id:
                        results_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
                        results_response = requests.get(results_url, headers=headers)
                        
                        if results_response.status_code == 200:
                            results = results_response.json()
                            print(f"📊 Results: {len(results)} items")
                            if results:
                                print("\nFirst result sample:")
                                print(json.dumps(results[0], indent=2)[:500] + "...")
                            break
                elif status == 'FAILED':
                    error = status_data.get('data', {}).get('statusMessage', 'Unknown error')
                    print(f"❌ Run failed: {error}")
        break
    else:
        print(f"❌ Format {idx} failed")
        try:
            error_data = response.json()
            print(f"Error: {error_data}")
        except:
            print(f"Response: {response.text[:200]}")
        print()

print("\n✅ Test complete!")
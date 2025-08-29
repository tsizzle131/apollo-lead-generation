#!/usr/bin/env python3
"""Direct test of Google Maps Apify actor"""

import requests
import json
import os
import sys
import time

# Add modules directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))
import config

APIFY_API_KEY = config.APIFY_API_KEY
if not APIFY_API_KEY:
    print("❌ APIFY_API_KEY not set")
    exit(1)

# Actor ID from the URL you provided
actor_id = "nwua9Gu5YrADL7ZDj"

print(f"🔍 Testing Google Maps actor: {actor_id}")
print(f"🔑 API Key: {APIFY_API_KEY[:10]}...")

# Test payload - using correct field names for this actor
payload = {
    "searchStringsArray": ["coffee shops Austin TX"],
    "maxCrawledPlacesPerSearch": 3,
    "language": "en",
    "exportPlaceUrls": False,
    "saveHtml": False,
    "saveScreenshots": False,
    "proxyConfig": {
        "useApifyProxy": True
    }
}

print(f"\n📦 Payload: {json.dumps(payload, indent=2)}")

# Start the run
url = f"https://api.apify.com/v2/acts/{actor_id}/runs"
headers = {
    "Authorization": f"Bearer {APIFY_API_KEY}",
    "Content-Type": "application/json"
}

print(f"\n🚀 Starting actor run...")
response = requests.post(url, json=payload, headers=headers)

print(f"📡 Response Status: {response.status_code}")
print(f"📡 Response Headers: {dict(response.headers)}")

if response.status_code not in [200, 201]:
    print(f"❌ Failed to start run")
    print(f"Response: {response.text}")
    exit(1)

data = response.json()
print(f"\n✅ Run started successfully!")
print(f"Response: {json.dumps(data, indent=2)}")

run_id = data.get('data', {}).get('id')
if not run_id:
    print("❌ No run ID in response")
    exit(1)

print(f"\n🆔 Run ID: {run_id}")
print(f"⏳ Waiting for completion...")

# Wait for completion
max_wait = 60  # 1 minute
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
        print("\n✅ Run completed!")
        
        # Get results
        dataset_id = status_data.get('data', {}).get('defaultDatasetId')
        if dataset_id:
            results_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
            results_response = requests.get(results_url, headers=headers)
            
            if results_response.status_code == 200:
                results = results_response.json()
                print(f"\n📊 Results: {len(results)} places found")
                
                if results:
                    print("\nFirst result:")
                    print(json.dumps(results[0], indent=2))
            else:
                print(f"❌ Failed to get results: {results_response.status_code}")
        break
    
    elif status == 'FAILED':
        print(f"\n❌ Run failed!")
        print(f"Details: {json.dumps(status_data, indent=2)}")
        break

print("\n✅ Test complete!")
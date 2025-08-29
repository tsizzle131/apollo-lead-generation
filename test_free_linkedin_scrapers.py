#!/usr/bin/env python3
"""Test free LinkedIn scrapers on Apify"""

import requests
import json
import sys
import time
sys.path.append('.')
import config

APIFY_API_KEY = config.APIFY_API_KEY

print("🔍 Testing free LinkedIn scrapers\n")

# List of potentially free/trial LinkedIn scrapers to test
scrapers = [
    {
        "name": "saswave/linkedin-profile",
        "actor_id": "saswave~linkedin-profile",
        "payload": {
            "urls": ["https://www.linkedin.com/search/results/people/?keywords=coffee%20shop%20owner%20Austin"],
            "scrapeEmail": True,
            "maxResults": 3
        }
    },
    {
        "name": "dev_fusion/linkedin-profile-scraper",
        "actor_id": "dev_fusion~linkedin-profile-scraper",
        "payload": {
            "profileUrls": ["https://www.linkedin.com/in/sample-profile/"],  # Would need actual URLs
            "proxyConfiguration": {"useApifyProxy": True}
        }
    },
    {
        "name": "curious_coder/linkedin-profile-scraper",
        "actor_id": "curious_coder~linkedin-profile-scraper",
        "payload": {
            "urls": ["https://www.linkedin.com/search/results/people/?keywords=restaurant%20owner%20Austin"],
            "maxProfiles": 3
        }
    }
]

for scraper in scrapers:
    print(f"Testing: {scraper['name']}")
    print(f"Actor ID: {scraper['actor_id']}")
    
    url = f"https://api.apify.com/v2/acts/{scraper['actor_id']}/runs"
    headers = {
        "Authorization": f"Bearer {APIFY_API_KEY}",
        "Content-Type": "application/json"
    }
    
    print(f"Payload: {json.dumps(scraper['payload'], indent=2)}")
    
    response = requests.post(url, json=scraper['payload'], headers=headers, timeout=30)
    
    print(f"Response Status: {response.status_code}")
    
    if response.status_code in [200, 201]:
        print(f"✅ {scraper['name']} - Run started successfully!")
        data = response.json()
        run_id = data.get('data', {}).get('id')
        print(f"Run ID: {run_id}")
    else:
        print(f"❌ {scraper['name']} - Failed")
        try:
            error_data = response.json()
            error_msg = error_data.get('error', {}).get('message', 'Unknown error')
            print(f"Error: {error_msg}")
        except:
            print(f"Response: {response.text[:200]}")
    
    print("-" * 60)
    print()

print("\n✅ Test complete!")
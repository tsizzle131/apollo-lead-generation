#!/usr/bin/env python3
"""
Use Apify's Google Search Results Scraper to find business owners
This is a legitimate API approach that works
"""

import requests
import json
import time
import sys
sys.path.append('.')
import config

APIFY_API_KEY = config.APIFY_API_KEY

print("🔍 Testing Google Search via Apify for business owners\n")

# Apify Google Search Results Scraper
# This one is free/cheap and very useful
actor_id = "nFJndFXA5zjCTuudP"  # Google Search Results Scraper

def search_for_owner(business_name: str, location: str):
    """Search Google for business owner information"""
    
    # Smart search queries that often reveal owners
    queries = [
        f'"{business_name}" owner {location} -site:yelp.com',
        f'"{business_name}" "founded by" OR "started by" {location}',
        f'site:linkedin.com/in "{business_name}" {location} owner OR founder OR CEO',
        f'"{business_name}" {location} "meet the owner"',
    ]
    
    all_results = []
    
    for query in queries:
        print(f"📍 Searching: {query}")
        
        url = f"https://api.apify.com/v2/acts/{actor_id}/runs"
        headers = {
            "Authorization": f"Bearer {APIFY_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "queries": query,
            "maxPagesPerQuery": 1,
            "resultsPerPage": 10,
            "languageCode": "en",
            "mobileResults": False
        }
        
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        if response.status_code in [200, 201]:
            data = response.json()
            run_id = data.get('data', {}).get('id')
            
            if run_id:
                print(f"  ⏳ Run ID: {run_id}")
                
                # Wait for completion
                time.sleep(10)
                
                # Get results
                status_url = f"https://api.apify.com/v2/acts/{actor_id}/runs/{run_id}"
                status_response = requests.get(status_url, headers=headers)
                
                if status_response.status_code == 200:
                    status_data = status_response.json()
                    
                    if status_data.get('data', {}).get('status') == 'SUCCEEDED':
                        dataset_id = status_data.get('data', {}).get('defaultDatasetId')
                        
                        if dataset_id:
                            results_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
                            results_response = requests.get(results_url, headers=headers)
                            
                            if results_response.status_code == 200:
                                results = results_response.json()
                                
                                # Parse results for owner names
                                for result in results:
                                    organic = result.get('organicResults', [])
                                    for item in organic[:5]:
                                        title = item.get('title', '')
                                        snippet = item.get('description', '')
                                        url = item.get('url', '')
                                        
                                        # Look for owner names in snippets
                                        import re
                                        owner_patterns = [
                                            r'(?:owner|owned by|founder|founded by)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)',
                                            r'([A-Z][a-z]+ [A-Z][a-z]+),?\s+(?:owner|founder|CEO)',
                                        ]
                                        
                                        for pattern in owner_patterns:
                                            matches = re.findall(pattern, snippet, re.IGNORECASE)
                                            for name in matches:
                                                if len(name.split()) == 2 and business_name.lower() not in name.lower():
                                                    print(f"  ✅ Found: {name} (from {url})")
                                                    all_results.append({
                                                        'name': name,
                                                        'source': url,
                                                        'snippet': snippet
                                                    })
                                        
                                        # Check for LinkedIn URLs
                                        if 'linkedin.com/in/' in url:
                                            profile = url.split('/in/')[-1].split('?')[0].strip('/')
                                            print(f"  🔗 LinkedIn: {profile}")
        else:
            print(f"  ❌ Failed: {response.status_code}")
        
        time.sleep(2)
    
    return all_results


# Test with real Austin businesses
businesses = [
    ("Franklin Barbecue", "Austin TX"),  # Aaron Franklin is the famous owner
    ("Uchi Restaurant", "Austin TX"),     # Tyson Cole is the chef/owner  
    ("Amy's Ice Creams", "Austin TX"),    # Amy Simmons is the founder
]

for business_name, location in businesses:
    print(f"\n{'='*60}")
    print(f"🏪 Searching for owner of: {business_name}")
    print(f"📍 Location: {location}")
    print('='*60 + '\n')
    
    results = search_for_owner(business_name, location)
    
    if results:
        print(f"\n📊 Summary: Found {len(results)} potential owner references")
        # Deduplicate by name
        unique_names = {}
        for r in results:
            name = r['name']
            if name not in unique_names:
                unique_names[name] = r
        
        print("\n🎯 Unique names found:")
        for name, info in unique_names.items():
            print(f"  - {name}")
            print(f"    Source: {info['source'][:50]}...")
    else:
        print("\n❌ No owner names found")

print("\n✅ Test complete!")
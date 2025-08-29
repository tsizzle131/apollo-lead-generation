#!/usr/bin/env python3
"""Test LinkedIn actor availability and find correct ID"""

import requests
import json
import sys
sys.path.append('.')
import config

APIFY_API_KEY = config.APIFY_API_KEY

print("🔍 Searching for LinkedIn actors on Apify...")

# Search for LinkedIn actors
search_url = "https://api.apify.com/v2/acts"
headers = {"Authorization": f"Bearer {APIFY_API_KEY}"}

# Try different potential actor IDs
potential_actors = [
    "bebity/linkedin-premium-actor",
    "linkedin-premium-actor",
    "bebity~linkedin-premium-actor",
    "vdrmota/linkedin-scraper",
    "anchor/linkedin-scraper",
    "curious_coder/linkedin-scraper-api",
    "voyager/linkedin-profile-scraper",
]

print("\n📋 Testing potential LinkedIn actor IDs:\n")

for actor_id in potential_actors:
    # Try to get actor info
    url = f"https://api.apify.com/v2/acts/{actor_id}"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        actor_data = data.get('data', {})
        print(f"✅ FOUND: {actor_id}")
        print(f"   Name: {actor_data.get('name', 'N/A')}")
        print(f"   Title: {actor_data.get('title', 'N/A')}")
        print(f"   Description: {actor_data.get('description', 'N/A')[:100]}...")
        print(f"   Latest Build: {actor_data.get('defaultRunOptions', {}).get('build', 'N/A')}")
        print()
    else:
        print(f"❌ Not found: {actor_id} (Status: {response.status_code})")

print("\n🔍 Searching for 'linkedin' in actor store...\n")

# Search the store
store_search_url = "https://api.apify.com/v2/store?search=linkedin&limit=10"
response = requests.get(store_search_url)

if response.status_code == 200:
    data = response.json()
    items = data.get('data', {}).get('items', [])
    
    print(f"Found {len(items)} LinkedIn-related actors:\n")
    
    for item in items[:10]:  # Show first 10
        actor_id = item.get('username', '') + '/' + item.get('name', '')
        print(f"📦 {actor_id}")
        print(f"   Title: {item.get('title', 'N/A')}")
        print(f"   Description: {item.get('description', 'N/A')[:100]}...")
        print(f"   Type: {item.get('type', 'N/A')}")
        print()
else:
    print(f"❌ Failed to search store: {response.status_code}")
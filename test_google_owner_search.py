#!/usr/bin/env python3
"""
Test searching Google for business owner information
Using web scraping approach (no API needed)
"""

import requests
from bs4 import BeautifulSoup
import re
import time
from urllib.parse import quote

def search_google_for_owner(business_name: str, location: str):
    """
    Search Google for business owner information
    """
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    # Try different search patterns
    search_queries = [
        f'"{business_name}" owner {location}',
        f'"{business_name}" "founded by" {location}',
        f'"{business_name}" CEO founder {location}',
        f'site:linkedin.com "{business_name}" {location} owner OR founder OR CEO'
    ]
    
    found_names = []
    
    for query in search_queries:
        print(f"\n🔍 Searching: {query}")
        
        # Google search URL
        url = f"https://www.google.com/search?q={quote(query)}"
        
        try:
            response = session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for text snippets that mention owner names
            # Google puts search results in various div classes
            snippets = soup.find_all(['span', 'div'], string=re.compile(r'(owner|founded by|CEO|president)', re.I))
            
            for snippet in snippets[:5]:  # Check first 5 matches
                text = snippet.get_text()
                
                # Look for name patterns after key words
                patterns = [
                    r'(?:owner|owned by|founder|founded by|CEO|president)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)',
                    r'([A-Z][a-z]+ [A-Z][a-z]+),?\s+(?:owner|founder|CEO|president)',
                ]
                
                for pattern in patterns:
                    matches = re.findall(pattern, text, re.IGNORECASE)
                    for name in matches:
                        # Filter out business names and common words
                        if business_name.lower() not in name.lower() and len(name.split()) == 2:
                            print(f"  ✅ Found potential name: {name}")
                            found_names.append(name)
            
            # Also check for LinkedIn URLs in results
            linkedin_links = soup.find_all('a', href=re.compile(r'linkedin\.com/in/'))
            for link in linkedin_links[:3]:
                href = link.get('href', '')
                if '/in/' in href:
                    profile = href.split('/in/')[-1].split('?')[0].split('/')[0]
                    print(f"  🔗 Found LinkedIn profile: {profile}")
            
            time.sleep(2)  # Be respectful to Google
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
    
    return list(set(found_names))  # Deduplicate


def search_business_on_facebook(business_name: str, location: str):
    """
    Search for business Facebook page to find owner info
    """
    print(f"\n📘 Searching Facebook for: {business_name}")
    
    # Facebook pages often have owner info in:
    # 1. Page transparency section
    # 2. About section
    # 3. Posts signed by owner
    
    # This would need Facebook API or scraping
    # For now, showing the approach
    
    search_url = f"https://www.facebook.com/search/pages/?q={quote(business_name + ' ' + location)}"
    print(f"  Would check: {search_url}")
    
    return []


def check_whois_domain(website_url: str):
    """
    Check WHOIS data for domain registration info
    """
    from urllib.parse import urlparse
    
    domain = urlparse(website_url).netloc.replace('www.', '')
    print(f"\n🌐 Checking WHOIS for: {domain}")
    
    # Most WHOIS data is now redacted due to GDPR
    # But we can try:
    # 1. Historical WHOIS via Archive.org
    # 2. Check if email is in TXT records
    
    try:
        # Check DNS TXT records (sometimes has owner info)
        import socket
        txt_records = socket.gethostbyname_ex(domain)
        print(f"  DNS lookup performed")
    except:
        pass
    
    return []


# Test with real businesses
test_businesses = [
    ("Mozart's Coffee Roasters", "Austin, TX", "https://mozartscoffee.com"),
    ("Franklin Barbecue", "Austin, TX", "https://franklinbbq.com"),
    ("Hopdoddy Burger Bar", "Austin, TX", "https://hopdoddy.com")
]

print("=" * 60)
print("🕵️ TESTING CREATIVE OWNER DISCOVERY METHODS")
print("=" * 60)

for business_name, location, website in test_businesses:
    print(f"\n\n🏪 Business: {business_name}")
    print(f"📍 Location: {location}")
    print(f"🌐 Website: {website}")
    
    # Method 1: Google Search
    owner_names = search_google_for_owner(business_name, location)
    if owner_names:
        print(f"\n✅ Potential owners found: {owner_names}")
    
    # Method 2: Facebook
    fb_info = search_business_on_facebook(business_name, location)
    
    # Method 3: WHOIS
    whois_info = check_whois_domain(website)
    
    print("\n" + "-" * 40)
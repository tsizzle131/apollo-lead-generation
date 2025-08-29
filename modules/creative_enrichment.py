"""
Creative Enrichment Module
Alternative methods to find business decision makers without LinkedIn
"""

import requests
import logging
import re
import time
from typing import List, Dict, Any, Optional
from urllib.parse import quote, urlparse
import json

class CreativeEnrichment:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def find_decision_makers(self, business_name: str, location: str, website: str = None) -> List[Dict[str, Any]]:
        """
        Try multiple creative methods to find business decision makers
        """
        contacts = []
        
        # Method 1: Google Search for owner information
        owner_info = self._search_google_for_owner(business_name, location)
        if owner_info:
            contacts.extend(owner_info)
        
        # Method 2: Facebook page scraping
        fb_contacts = self._check_facebook_page(business_name, location)
        if fb_contacts:
            contacts.extend(fb_contacts)
        
        # Method 3: Check state business registry
        registry_info = self._check_business_registry(business_name, location)
        if registry_info:
            contacts.extend(registry_info)
        
        # Method 4: Parse website for team/about pages
        if website:
            website_contacts = self._parse_website_for_contacts(website)
            if website_contacts:
                contacts.extend(website_contacts)
        
        # Method 5: Check Instagram business profile
        insta_contacts = self._check_instagram(business_name)
        if insta_contacts:
            contacts.extend(insta_contacts)
        
        return self._deduplicate_contacts(contacts)
    
    def _search_google_for_owner(self, business_name: str, location: str) -> List[Dict[str, Any]]:
        """
        Search Google for owner information using specific patterns
        """
        contacts = []
        try:
            # Search patterns that often reveal owners
            search_patterns = [
                f'"{business_name}" owner {location}',
                f'"{business_name}" "founded by" {location}',
                f'"{business_name}" "owned and operated by"',
                f'site:linkedin.com "{business_name}" owner OR founder OR CEO {location}'
            ]
            
            for pattern in search_patterns:
                # Use Google Custom Search API or web scraping
                # For now, using a mock implementation
                logging.info(f"🔍 Searching: {pattern}")
                
                # This would actually search Google
                # results = self._perform_google_search(pattern)
                # contacts.extend(self._extract_names_from_results(results))
                
            return contacts
            
        except Exception as e:
            logging.warning(f"Google search error: {e}")
            return []
    
    def _check_facebook_page(self, business_name: str, location: str) -> List[Dict[str, Any]]:
        """
        Check Facebook business pages for owner information
        """
        try:
            # Facebook Graph API approach (requires API key)
            # Or scrape public Facebook pages
            
            # Look for:
            # - Page transparency section (shows page admins sometimes)
            # - About section with founder info
            # - Posts signed by owner
            
            fb_url = f"https://www.facebook.com/search/pages/?q={quote(business_name + ' ' + location)}"
            logging.info(f"📘 Checking Facebook: {fb_url}")
            
            # Implementation would go here
            return []
            
        except Exception as e:
            logging.warning(f"Facebook check error: {e}")
            return []
    
    def _check_business_registry(self, business_name: str, location: str) -> List[Dict[str, Any]]:
        """
        Check state business registries for registered owners/officers
        """
        try:
            state = self._extract_state_from_location(location)
            
            # Different states have different APIs/websites
            # Examples:
            # - Texas: SOSDirect API
            # - California: BusinessSearch.sos.ca.gov
            # - Florida: sunbiz.org API
            
            if state == "TX":
                return self._check_texas_registry(business_name)
            elif state == "CA":
                return self._check_california_registry(business_name)
            # Add more states...
            
            return []
            
        except Exception as e:
            logging.warning(f"Registry check error: {e}")
            return []
    
    def _parse_website_for_contacts(self, website: str) -> List[Dict[str, Any]]:
        """
        Parse business website for team/about pages with owner info
        """
        contacts = []
        try:
            # Check common pages that list team members
            pages_to_check = [
                '/about', '/about-us', '/team', '/our-team',
                '/leadership', '/founders', '/contact', '/staff'
            ]
            
            base_url = website.rstrip('/')
            
            for page in pages_to_check:
                url = base_url + page
                logging.info(f"🌐 Checking: {url}")
                
                try:
                    response = self.session.get(url, timeout=5)
                    if response.status_code == 200:
                        # Look for patterns like:
                        # - "Owner: John Smith"
                        # - "Founded by Jane Doe"
                        # - Email patterns
                        
                        text = response.text
                        
                        # Extract names with titles
                        owner_patterns = [
                            r'(?:Owner|Founder|CEO|President)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)',
                            r'(?:owned by|founded by|operated by)\s+([A-Z][a-z]+ [A-Z][a-z]+)',
                        ]
                        
                        for pattern in owner_patterns:
                            matches = re.findall(pattern, text, re.IGNORECASE)
                            for name in matches:
                                contacts.append({
                                    'name': name,
                                    'source': 'website',
                                    'url': url
                                })
                        
                        # Extract emails
                        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                        emails = re.findall(email_pattern, text)
                        
                        # Filter out generic emails
                        personal_emails = [
                            e for e in emails 
                            if not any(prefix in e.lower() for prefix in ['info@', 'contact@', 'sales@', 'support@'])
                        ]
                        
                        for email in personal_emails:
                            contacts.append({
                                'email': email,
                                'source': 'website',
                                'url': url
                            })
                    
                except:
                    continue
            
            return contacts
            
        except Exception as e:
            logging.warning(f"Website parsing error: {e}")
            return []
    
    def _check_instagram(self, business_name: str) -> List[Dict[str, Any]]:
        """
        Check Instagram business profiles for owner information
        """
        try:
            # Instagram often shows:
            # - Business owner in bio
            # - Owner tagged in posts
            # - Personal account linked
            
            # This would require Instagram API or scraping
            # For now, return empty
            return []
            
        except Exception as e:
            logging.warning(f"Instagram check error: {e}")
            return []
    
    def _extract_state_from_location(self, location: str) -> str:
        """Extract state abbreviation from location string"""
        # Simple implementation - could be improved
        state_abbrevs = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
        
        location_upper = location.upper()
        for state in state_abbrevs:
            if state in location_upper:
                return state
        return ""
    
    def _check_texas_registry(self, business_name: str) -> List[Dict[str, Any]]:
        """Check Texas Secretary of State for business registration"""
        # Implementation would use Texas SOSDirect API
        # Requires API credentials
        return []
    
    def _check_california_registry(self, business_name: str) -> List[Dict[str, Any]]:
        """Check California Secretary of State for business registration"""
        # Implementation would scrape businesssearch.sos.ca.gov
        return []
    
    def _deduplicate_contacts(self, contacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate contacts based on name/email"""
        seen = set()
        unique = []
        
        for contact in contacts:
            key = (contact.get('name', ''), contact.get('email', ''))
            if key not in seen and (key[0] or key[1]):
                seen.add(key)
                unique.append(contact)
        
        return unique
    
    def generate_email_permutations(self, first_name: str, last_name: str, domain: str) -> List[str]:
        """
        Generate common email patterns for a person at a domain
        """
        if not domain or not first_name:
            return []
        
        first = first_name.lower()
        last = last_name.lower() if last_name else ''
        
        patterns = []
        
        if last:
            patterns.extend([
                f"{first}@{domain}",
                f"{last}@{domain}",
                f"{first}.{last}@{domain}",
                f"{first}{last}@{domain}",
                f"{first[0]}{last}@{domain}",
                f"{first}_{last}@{domain}",
                f"{last}.{first}@{domain}",
                f"{first[0]}.{last}@{domain}",
            ])
        else:
            patterns.append(f"{first}@{domain}")
        
        return patterns
    
    def verify_email(self, email: str) -> bool:
        """
        Verify if an email address is valid and deliverable
        Would use services like:
        - Hunter.io verify API
        - NeverBounce API
        - EmailListVerify API
        """
        # For now, just check format
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(email_regex, email))


# Example usage
if __name__ == "__main__":
    enricher = CreativeEnrichment()
    
    # Test with a local business
    contacts = enricher.find_decision_makers(
        business_name="Mozart's Coffee Roasters",
        location="Austin, TX",
        website="https://mozartscoffee.com"
    )
    
    print(f"Found {len(contacts)} potential contacts")
    for contact in contacts:
        print(f"  - {contact}")
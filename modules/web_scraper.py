import requests
import logging
import time
import random
import re
import json
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from urllib.parse import urljoin, urlparse
from config import (
    USER_AGENTS, REQUEST_TIMEOUT, MAX_RETRIES, DELAY_BETWEEN_REQUESTS,
    MAX_LINKS_PER_SITE, WEBSITE_TIMEOUT, WEBSITE_MAX_RETRIES,
    MAX_WEBSITE_WORKERS, ENABLE_PARALLEL_PROCESSING
)
from .rate_limiter import rate_limiter

# High-value pages to prioritize for scraping
HIGH_VALUE_PAGES = [
    '/about', '/about-us', '/aboutus', '/about_us',
    '/team', '/our-team', '/ourteam', '/staff', '/leadership', '/people',
    '/contact', '/contact-us', '/contactus', '/contact_us',
    '/services', '/what-we-do', '/our-services',
]

class WebScraper:
    def __init__(self):
        self.session = requests.Session()
        
    def scrape_website_content(self, website_url: str) -> Dict[str, Any]:
        """
        Scrape website content and extract internal links (with domain throttling)

        Args:
            website_url: The website URL to scrape

        Returns:
            Dictionary containing extracted links, content summaries, and enriched data
        """
        try:
            domain = urlparse(website_url).netloc

            # Check if domain is blocked
            if rate_limiter.domain_throttler.is_domain_blocked(domain):
                logging.warning(f"Domain {domain} is blocked due to repeated failures")
                return self._empty_result()

            logging.info(f"Starting website research for: {website_url}")

            # Step 1: Scrape the homepage with domain throttling
            homepage_content = self._scrape_page_with_throttle(website_url)
            if not homepage_content:
                return self._empty_result()

            # Step 2: Extract structured data from homepage (JSON-LD, phone, social)
            structured_data = self._extract_structured_data(homepage_content)
            phone_numbers = self._extract_phone_numbers(homepage_content)
            social_links = self._extract_social_links(homepage_content, website_url)

            # Step 3: Extract internal links
            internal_links = self._extract_internal_links(homepage_content, website_url)

            # Step 4: Prioritize high-value pages (about, team, contact, services)
            prioritized_links = self._prioritize_links(internal_links)

            # Step 5: Filter and clean links
            filtered_links = self._filter_links(prioritized_links)

            # Step 6: Limit to max links and scrape pages
            limited_links = filtered_links[:MAX_LINKS_PER_SITE]

            # Use parallel or sequential scraping based on config
            if ENABLE_PARALLEL_PROCESSING and len(limited_links) > 1:
                page_summaries = self._scrape_pages_parallel(website_url, limited_links)
            else:
                page_summaries = self._scrape_pages_sequential(website_url, limited_links)

            logging.info(f"Successfully scraped {len(page_summaries)} pages from {website_url}")

            # Step 7: Extract emails and phones from all content
            all_emails = self._extract_emails_from_content(homepage_content)
            for summary in page_summaries:
                if 'content' in summary:
                    all_emails.extend(self._extract_emails_from_content(summary['content']))
                    phone_numbers.extend(self._extract_phone_numbers(summary.get('raw_html', '')))

            # Deduplicate
            unique_emails = list(set(all_emails))
            unique_phones = list(set(phone_numbers))

            # Step 8: Extract team members if we found a team page
            team_members = []
            for summary in page_summaries:
                url_lower = summary.get('url', '').lower()
                if any(kw in url_lower for kw in ['team', 'staff', 'people', 'leadership', 'about']):
                    team_members.extend(self._extract_team_members(summary.get('raw_html', '')))

            return {
                "links": limited_links,
                "summaries": page_summaries,
                "emails": unique_emails,
                "phone_numbers": unique_phones,
                "social_links": social_links,
                "structured_data": structured_data,
                "team_members": team_members[:10],  # Limit to 10 team members
            }

        except Exception as e:
            logging.error(f"Error scraping website {website_url}: {e}")
            domain = urlparse(website_url).netloc
            rate_limiter.mark_website_failed(domain)
            return self._empty_result()

    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure"""
        return {
            "links": [],
            "summaries": [],
            "emails": [],
            "phone_numbers": [],
            "social_links": {},
            "structured_data": {},
            "team_members": [],
        }
    
    def _scrape_pages_parallel(self, base_url: str, links: List[str]) -> List[Dict[str, Any]]:
        """Scrape multiple pages in parallel with domain throttling"""
        page_summaries = []

        # Since all links are from same domain, we need to serialize them
        # but we can process the markdown conversion in parallel
        for link in links:
            try:
                full_url = urljoin(base_url, link)
                page_content = self._scrape_page_with_throttle(full_url)

                if page_content:
                    # Convert to markdown
                    markdown_content = self._html_to_markdown(page_content)
                    page_summaries.append({
                        'url': full_url,
                        'content': markdown_content,
                        'raw_html': page_content,  # Keep raw HTML for extraction
                    })
            except Exception as e:
                logging.warning(f"Failed to scrape {link}: {e}")
                continue

        return page_summaries

    def _scrape_pages_sequential(self, base_url: str, links: List[str]) -> List[Dict[str, Any]]:
        """Original sequential scraping method"""
        page_summaries = []

        for link in links:
            try:
                full_url = urljoin(base_url, link)
                page_content = self._scrape_page_with_throttle(full_url)

                if page_content:
                    # Convert to markdown
                    markdown_content = self._html_to_markdown(page_content)
                    page_summaries.append({
                        'url': full_url,
                        'content': markdown_content,
                        'raw_html': page_content,  # Keep raw HTML for extraction
                    })

            except Exception as e:
                logging.warning(f"Failed to scrape {link}: {e}")
                continue

        return page_summaries

    def _prioritize_links(self, links: List[str]) -> List[str]:
        """Prioritize high-value pages (about, team, contact) at the front"""
        high_value = []
        regular = []

        for link in links:
            link_lower = link.lower()
            if any(hv in link_lower for hv in HIGH_VALUE_PAGES):
                high_value.append(link)
            else:
                regular.append(link)

        # Return high-value pages first, then regular pages
        return high_value + regular

    def _extract_structured_data(self, html_content: str) -> Dict[str, Any]:
        """Extract JSON-LD/Schema.org structured data from HTML"""
        result = {}
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find all JSON-LD script tags
            json_ld_scripts = soup.find_all('script', type='application/ld+json')

            for script in json_ld_scripts:
                try:
                    data = json.loads(script.string)

                    # Handle both single objects and arrays
                    items = data if isinstance(data, list) else [data]

                    for item in items:
                        item_type = item.get('@type', '')

                        # Extract LocalBusiness / Organization data
                        if item_type in ['LocalBusiness', 'Organization', 'Corporation',
                                        'Restaurant', 'Store', 'ProfessionalService']:
                            result['business_name'] = item.get('name')
                            result['description'] = item.get('description')
                            result['telephone'] = item.get('telephone')
                            result['founding_date'] = item.get('foundingDate')
                            result['number_of_employees'] = item.get('numberOfEmployees')
                            result['price_range'] = item.get('priceRange')

                            # Address
                            address = item.get('address', {})
                            if isinstance(address, dict):
                                result['street_address'] = address.get('streetAddress')
                                result['city'] = address.get('addressLocality')
                                result['state'] = address.get('addressRegion')
                                result['postal_code'] = address.get('postalCode')

                            # Geo coordinates
                            geo = item.get('geo', {})
                            if isinstance(geo, dict):
                                result['latitude'] = geo.get('latitude')
                                result['longitude'] = geo.get('longitude')

                            # Opening hours
                            result['opening_hours'] = item.get('openingHours')

                            # Social/same as links
                            same_as = item.get('sameAs', [])
                            if same_as:
                                result['same_as_links'] = same_as if isinstance(same_as, list) else [same_as]

                        # Extract Person data (founders, employees)
                        elif item_type == 'Person':
                            if 'people' not in result:
                                result['people'] = []
                            result['people'].append({
                                'name': item.get('name'),
                                'job_title': item.get('jobTitle'),
                                'email': item.get('email'),
                            })

                except json.JSONDecodeError:
                    continue

            if result:
                logging.info(f"Extracted structured data: {list(result.keys())}")

        except Exception as e:
            logging.debug(f"Error extracting structured data: {e}")

        return result

    def _extract_phone_numbers(self, html_content: str) -> List[str]:
        """Extract phone numbers from HTML content"""
        phones = []
        try:
            # Multiple phone patterns for US numbers
            patterns = [
                r'\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',  # (123) 456-7890, 123-456-7890
                r'tel:[\+]?1?[0-9]{10,11}',  # tel: links
            ]

            for pattern in patterns:
                matches = re.findall(pattern, html_content)
                for match in matches:
                    # Clean up the phone number
                    clean_phone = re.sub(r'[^\d+]', '', match.replace('tel:', ''))
                    if len(clean_phone) >= 10:
                        phones.append(clean_phone)

            # Also check href="tel:" links
            soup = BeautifulSoup(html_content, 'html.parser')
            tel_links = soup.find_all('a', href=re.compile(r'^tel:', re.I))
            for link in tel_links:
                phone = link.get('href', '').replace('tel:', '').strip()
                clean_phone = re.sub(r'[^\d+]', '', phone)
                if len(clean_phone) >= 10:
                    phones.append(clean_phone)

        except Exception as e:
            logging.debug(f"Error extracting phone numbers: {e}")

        return list(set(phones))  # Deduplicate

    def _extract_social_links(self, html_content: str, base_url: str) -> Dict[str, str]:
        """Extract social media links from HTML"""
        social = {}
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Social media patterns
            social_patterns = {
                'linkedin': r'linkedin\.com/(company|in)/[^/"\s]+',
                'facebook': r'facebook\.com/[^/"\s]+',
                'twitter': r'(twitter\.com|x\.com)/[^/"\s]+',
                'instagram': r'instagram\.com/[^/"\s]+',
                'youtube': r'youtube\.com/(channel|user|c)/[^/"\s]+',
                'tiktok': r'tiktok\.com/@[^/"\s]+',
            }

            # Search all links
            for link in soup.find_all('a', href=True):
                href = link['href']
                for platform, pattern in social_patterns.items():
                    if platform not in social:  # Only get first match per platform
                        match = re.search(pattern, href, re.I)
                        if match:
                            # Reconstruct full URL
                            if href.startswith('http'):
                                social[platform] = href.split('?')[0]  # Remove query params
                            else:
                                social[platform] = f"https://{match.group()}"
                            logging.debug(f"Found {platform}: {social[platform]}")

        except Exception as e:
            logging.debug(f"Error extracting social links: {e}")

        return social

    def _extract_team_members(self, html_content: str) -> List[Dict[str, str]]:
        """Extract team member names and titles from team/about pages"""
        team = []
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Common patterns for team member cards/sections
            # Look for elements with common team-related classes
            team_selectors = [
                soup.find_all(['div', 'article', 'li'], class_=re.compile(
                    r'team|staff|member|person|employee|leadership|bio', re.I)),
                soup.find_all(['div', 'article'], attrs={'data-member': True}),
            ]

            for selector_results in team_selectors:
                for element in selector_results[:20]:  # Limit to prevent over-extraction
                    member = {}

                    # Look for name (usually in h2, h3, h4, or strong)
                    name_elem = element.find(['h2', 'h3', 'h4', 'h5', 'strong', 'b'],
                                            class_=re.compile(r'name|title', re.I))
                    if not name_elem:
                        name_elem = element.find(['h2', 'h3', 'h4', 'h5'])

                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        # Filter out non-name text
                        if name and len(name) < 50 and not any(skip in name.lower() for skip in
                            ['about', 'team', 'staff', 'our', 'meet', 'leadership']):
                            member['name'] = name

                    # Look for title/position
                    title_elem = element.find(['p', 'span', 'div'],
                                             class_=re.compile(r'title|position|role|job', re.I))
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        if title and len(title) < 100:
                            member['title'] = title

                    # Look for email in this section
                    email_match = re.search(
                        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                        element.get_text()
                    )
                    if email_match:
                        member['email'] = email_match.group()

                    # Only add if we found at least a name
                    if member.get('name'):
                        team.append(member)

            # Deduplicate by name
            seen_names = set()
            unique_team = []
            for member in team:
                name = member.get('name', '').lower()
                if name and name not in seen_names:
                    seen_names.add(name)
                    unique_team.append(member)

            if unique_team:
                logging.info(f"Extracted {len(unique_team)} team members")

            return unique_team

        except Exception as e:
            logging.debug(f"Error extracting team members: {e}")
            return []
    
    def _scrape_page_with_throttle(self, url: str) -> Optional[str]:
        """Scrape a page with domain throttling"""
        domain = urlparse(url).netloc
        
        # Wait for domain throttling
        try:
            rate_limiter.wait_for_website(domain)
        except Exception as e:
            logging.warning(f"Domain {domain} is blocked: {e}")
            return None
        
        return self._scrape_page(url)
    
    def _scrape_page(self, url: str) -> Optional[str]:
        """Scrape a single page and return HTML content"""
        try:
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            response = self._make_request_with_retry(url, headers=headers)
            
            if response and response.status_code == 200:
                return response.text
            else:
                logging.warning(f"Failed to scrape {url}: {response.status_code if response else 'No response'}")
                return None
                
        except Exception as e:
            logging.error(f"Error scraping page {url}: {e}")
            return None
    
    def _make_request_with_retry(self, url: str, headers: Dict[str, str]) -> Optional[requests.Response]:
        """Make HTTP request with retry logic - optimized for faster website failures"""
        for attempt in range(WEBSITE_MAX_RETRIES):  # Use website-specific retry count (1 instead of 3)
            try:
                response = self.session.get(
                    url, 
                    headers=headers, 
                    timeout=WEBSITE_TIMEOUT,  # Use website-specific timeout (10s instead of 1800s)
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limited
                    wait_time = 2 ** attempt
                    logging.warning(f"Rate limited on {url}, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.warning(f"HTTP {response.status_code} for {url}, attempt {attempt + 1}")
                    
            except requests.exceptions.Timeout:
                logging.warning(f"Timeout for {url}, attempt {attempt + 1}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Request error for {url}: {e}, attempt {attempt + 1}")
            
            if attempt < WEBSITE_MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
        
        return None
    
    def _extract_internal_links(self, html_content: str, base_url: str) -> List[str]:
        """Extract internal links from HTML content"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            links = []
            base_domain = urlparse(base_url).netloc
            
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                
                if not href:
                    continue
                
                # Handle relative URLs
                if href.startswith('/'):
                    links.append(href)
                # Handle absolute URLs from same domain
                elif href.startswith('http'):
                    parsed = urlparse(href)
                    if parsed.netloc == base_domain:
                        links.append(parsed.path)
                
            # Remove duplicates while preserving order
            unique_links = []
            seen = set()
            for link in links:
                if link not in seen:
                    unique_links.append(link)
                    seen.add(link)
            
            return unique_links
            
        except Exception as e:
            logging.error(f"Error extracting links: {e}")
            return []
    
    def _filter_links(self, links: List[str]) -> List[str]:
        """Filter links based on your n8n workflow logic"""
        filtered = []

        for link in links:
            # Must start with "/" (relative links only)
            if not link.startswith('/'):
                continue

            # Skip common non-content pages (but keep contact/about/team pages!)
            skip_patterns = [
                '/wp-admin', '/admin', '/login', '/register',
                '/cart', '/checkout', '/account', '/profile',
                '/search', '/privacy', '/terms', '/legal',
                '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip', '.css', '.js',
                '#', '?', 'javascript:', 'mailto:', 'tel:'
            ]

            if any(pattern in link.lower() for pattern in skip_patterns):
                continue
            
            # Clean up the link (remove trailing slash unless root)
            cleaned_link = self._normalize_link(link)
            
            if cleaned_link and cleaned_link not in filtered:
                filtered.append(cleaned_link)
        
        return filtered
    
    def _normalize_link(self, link: str) -> str:
        """Normalize link format (replicate your n8n Code node logic)"""
        try:
            # Handle absolute URLs
            if link.startswith('http://') or link.startswith('https://'):
                parsed = urlparse(link)
                path = parsed.path
            else:
                path = link
            
            # Strip trailing slash unless root "/"
            if path != "/" and path.endswith("/"):
                path = path[:-1]
            
            return path or "/"
            
        except Exception:
            return link
    
    def _extract_emails_from_content(self, content: str) -> List[str]:
        """Extract email addresses from content"""
        try:
            # Email regex pattern
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            emails = re.findall(email_pattern, content)
            
            # Filter out generic emails
            filtered_emails = [
                email for email in emails 
                if not any(prefix in email.lower() for prefix in [
                    'example@', 'test@', 'noreply@', 'no-reply@',
                    'donotreply@', 'do-not-reply@', 'webmaster@'
                ])
            ]
            
            return filtered_emails
        except Exception as e:
            logging.warning(f"Error extracting emails: {e}")
            return []
    
    def _html_to_markdown(self, html_content: str) -> str:
        """Convert HTML content to markdown"""
        try:
            # Clean up HTML first
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            
            # Get main content (try to find main content area)
            main_content = (
                soup.find('main') or 
                soup.find('article') or 
                soup.find('div', class_=['content', 'main', 'body']) or
                soup.find('body') or
                soup
            )
            
            # Convert to markdown
            markdown = md(str(main_content), 
                         heading_style="ATX",
                         strip=['script', 'style'])
            
            # Clean up the markdown
            lines = markdown.split('\n')
            cleaned_lines = []
            
            for line in lines:
                line = line.strip()
                if line and not line.startswith('!['):  # Remove image markdown
                    cleaned_lines.append(line)
            
            cleaned_markdown = '\n'.join(cleaned_lines)
            
            # Limit length (approximate)
            if len(cleaned_markdown) > 5000:
                cleaned_markdown = cleaned_markdown[:5000] + "..."
            
            return cleaned_markdown if cleaned_markdown.strip() else "<div>empty</div>"
            
        except Exception as e:
            logging.error(f"Error converting to markdown: {e}")
            return "<div>empty</div>"
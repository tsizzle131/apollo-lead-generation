import requests
import logging
import time
import random
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
from rate_limiter import rate_limiter

class WebScraper:
    def __init__(self):
        self.session = requests.Session()
        
    def scrape_website_content(self, website_url: str) -> Dict[str, Any]:
        """
        Scrape website content and extract internal links (with domain throttling)
        
        Args:
            website_url: The website URL to scrape
            
        Returns:
            Dictionary containing extracted links and content summaries
        """
        try:
            domain = urlparse(website_url).netloc
            
            # Check if domain is blocked
            if rate_limiter.domain_throttler.is_domain_blocked(domain):
                logging.warning(f"Domain {domain} is blocked due to repeated failures")
                return {"links": [], "summaries": []}
            
            logging.info(f"Starting website research for: {website_url}")
            
            # Step 1: Scrape the homepage with domain throttling
            homepage_content = self._scrape_page_with_throttle(website_url)
            if not homepage_content:
                return {"links": [], "summaries": []}
            
            # Step 2: Extract internal links
            internal_links = self._extract_internal_links(homepage_content, website_url)
            
            # Step 3: Filter and clean links
            filtered_links = self._filter_links(internal_links)
            
            # Step 4: Limit to max links and scrape pages
            limited_links = filtered_links[:MAX_LINKS_PER_SITE]
            
            # Use parallel or sequential scraping based on config
            if ENABLE_PARALLEL_PROCESSING and len(limited_links) > 1:
                page_summaries = self._scrape_pages_parallel(website_url, limited_links)
            else:
                page_summaries = self._scrape_pages_sequential(website_url, limited_links)
            
            logging.info(f"Successfully scraped {len(page_summaries)} pages from {website_url}")
            
            return {
                "links": limited_links,
                "summaries": page_summaries
            }
            
        except Exception as e:
            logging.error(f"Error scraping website {website_url}: {e}")
            domain = urlparse(website_url).netloc
            rate_limiter.mark_website_failed(domain)
            return {"links": [], "summaries": []}
    
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
                        'content': markdown_content
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
                        'content': markdown_content
                    })
                
            except Exception as e:
                logging.warning(f"Failed to scrape {link}: {e}")
                continue
        
        return page_summaries
    
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
                
            # Skip common non-content pages
            skip_patterns = [
                '/wp-admin', '/admin', '/login', '/register',
                '/cart', '/checkout', '/account', '/profile',
                '/search', '/contact', '/privacy', '/terms',
                '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip',
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
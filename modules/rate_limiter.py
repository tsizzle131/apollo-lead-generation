"""
Rate limiting utilities for API calls
Implements token bucket algorithm for smooth rate limiting
"""

import time
import threading
from typing import Dict, Optional
import logging

class TokenBucket:
    """
    Thread-safe token bucket for rate limiting
    """
    def __init__(self, rate: float, capacity: float):
        """
        Initialize a token bucket
        
        Args:
            rate: Tokens refilled per second
            capacity: Maximum bucket capacity
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill = time.time()
        self.lock = threading.Lock()
    
    def consume(self, tokens: int = 1) -> float:
        """
        Consume tokens from the bucket
        
        Args:
            tokens: Number of tokens to consume
            
        Returns:
            Time to wait before tokens are available (0 if immediate)
        """
        with self.lock:
            now = time.time()
            
            # Refill tokens based on elapsed time
            elapsed = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0
            
            # Calculate wait time
            deficit = tokens - self.tokens
            wait_time = deficit / self.rate
            return wait_time
    
    def wait_and_consume(self, tokens: int = 1):
        """
        Wait if necessary and consume tokens
        
        Args:
            tokens: Number of tokens to consume
        """
        wait_time = self.consume(tokens)
        if wait_time > 0:
            logging.debug(f"Rate limit: waiting {wait_time:.2f}s")
            time.sleep(wait_time)
            self.consume(tokens)


class DomainThrottler:
    """
    Throttle requests per domain to avoid IP blocking
    """
    def __init__(self, min_delay: float = 2.0):
        """
        Initialize domain throttler
        
        Args:
            min_delay: Minimum seconds between requests to same domain
        """
        self.min_delay = min_delay
        self.last_request: Dict[str, float] = {}
        self.failed_domains: set = set()
        self.lock = threading.Lock()
    
    def is_domain_blocked(self, domain: str) -> bool:
        """Check if domain has consistently failed"""
        return domain in self.failed_domains
    
    def mark_domain_failed(self, domain: str):
        """Mark a domain as consistently failing"""
        with self.lock:
            self.failed_domains.add(domain)
            logging.warning(f"Domain marked as failed: {domain}")
    
    def wait_for_domain(self, domain: str):
        """
        Wait if necessary before making request to domain
        
        Args:
            domain: The domain to check
        """
        with self.lock:
            now = time.time()
            
            if domain in self.last_request:
                elapsed = now - self.last_request[domain]
                if elapsed < self.min_delay:
                    wait_time = self.min_delay - elapsed
                    logging.debug(f"Domain throttle: waiting {wait_time:.2f}s for {domain}")
                    time.sleep(wait_time)
                    now = time.time()
            
            self.last_request[domain] = now


class APIRateLimiter:
    """
    Manage rate limits for different APIs
    """
    def __init__(self):
        """Initialize rate limiters for different APIs"""
        # OpenAI rate limits (requests per minute converted to per second)
        self.openai_gpt4 = TokenBucket(
            rate=10000 / 60,  # 10,000 RPM = 166.67 RPS
            capacity=100  # Burst capacity
        )
        
        self.openai_gpt4_mini = TokenBucket(
            rate=30000 / 60,  # 30,000 RPM = 500 RPS
            capacity=200  # Burst capacity
        )
        
        # Website scraping (conservative)
        self.domain_throttler = DomainThrottler(min_delay=2.0)
        
        # Apify (keep conservative)
        self.apify = TokenBucket(
            rate=1,  # 1 request per second
            capacity=5
        )
    
    def wait_for_openai(self, model: str = "gpt-4o"):
        """Wait for OpenAI rate limit"""
        if "mini" in model.lower():
            self.openai_gpt4_mini.wait_and_consume()
        else:
            self.openai_gpt4.wait_and_consume()
    
    def wait_for_website(self, domain: str):
        """Wait for website scraping rate limit"""
        if self.domain_throttler.is_domain_blocked(domain):
            raise Exception(f"Domain {domain} is blocked due to repeated failures")
        self.domain_throttler.wait_for_domain(domain)
    
    def mark_website_failed(self, domain: str):
        """Mark a website domain as failed"""
        self.domain_throttler.mark_domain_failed(domain)
    
    def wait_for_apify(self):
        """Wait for Apify rate limit"""
        self.apify.wait_and_consume()


# Global rate limiter instance
rate_limiter = APIRateLimiter()
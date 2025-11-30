"""
API Cost Configuration
Centralized pricing for all external API services used in the lead generation system.

Last updated: January 2025
Source: Research conducted on actual API pricing

This file serves as the single source of truth for all cost calculations.
Update this file when API pricing changes instead of modifying multiple files.
"""

from dataclasses import dataclass
from typing import Dict


@dataclass
class ServiceCost:
    """Cost configuration for a single API service"""
    name: str
    cost_per_thousand: float  # USD per 1000 items
    unit: str  # What we're charging per (e.g., "results", "pages", "verifications")
    notes: str = ""

    def calculate_cost(self, items: int) -> float:
        """Calculate cost for a given number of items"""
        return (items / 1000) * self.cost_per_thousand


# Current API Pricing (January 2025)
API_COSTS = {
    "google_maps": ServiceCost(
        name="Google Maps Scraper",
        cost_per_thousand=4.00,
        unit="results",
        notes="Apify pay-per-result pricing. Filters may add extra charges."
    ),

    "facebook": ServiceCost(
        name="Facebook Pages Scraper",
        cost_per_thousand=10.00,
        unit="pages",
        notes="Apify pay-per-result pricing. Most expensive enrichment service."
    ),

    "linkedin": ServiceCost(
        name="LinkedIn Premium Actor (bebity~linkedin-premium-actor)",
        cost_per_thousand=10.00,
        unit="profiles",
        notes="Premium LinkedIn actor with comprehensive data. Budget alternatives available at $3/1000."
    ),

    "linkedin_premium": ServiceCost(
        name="LinkedIn Company Scraper (Premium)",
        cost_per_thousand=10.00,
        unit="profiles",
        notes="Premium scraper with more comprehensive data."
    ),

    "bouncer": ServiceCost(
        name="Bouncer Email Verification",
        cost_per_thousand=2.00,
        unit="verifications",
        notes="Bulk pricing tier. No charge for duplicates or unknown results."
    ),
}


# OpenAI Pricing (per 1K tokens)
@dataclass
class OpenAICost:
    """OpenAI model pricing"""
    model: str
    input_per_1k_tokens: float
    output_per_1k_tokens: float
    notes: str = ""

    def calculate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost for given token counts"""
        input_cost = (input_tokens / 1000) * self.input_per_1k_tokens
        output_cost = (output_tokens / 1000) * self.output_per_1k_tokens
        return input_cost + output_cost


OPENAI_COSTS = {
    "gpt-4": OpenAICost(
        model="gpt-4",
        input_per_1k_tokens=0.03,
        output_per_1k_tokens=0.06,
        notes="Original GPT-4 model. Most expensive."
    ),

    "gpt-4o": OpenAICost(
        model="gpt-4o",
        input_per_1k_tokens=0.0025,
        output_per_1k_tokens=0.01,
        notes="GPT-4 Optimized. 75% cheaper than GPT-4. Recommended for most use cases."
    ),

    "gpt-4o-mini": OpenAICost(
        model="gpt-4o-mini",
        input_per_1k_tokens=0.00015,
        output_per_1k_tokens=0.0006,
        notes="Smallest GPT-4 model. Best for simple tasks."
    ),

    "gpt-3.5-turbo": OpenAICost(
        model="gpt-3.5-turbo",
        input_per_1k_tokens=0.0005,
        output_per_1k_tokens=0.0015,
        notes="Legacy model. Much cheaper but less capable."
    ),
}


# Enrichment Success Rate Assumptions
# Used for cost estimation when actual counts aren't known
ENRICHMENT_ASSUMPTIONS = {
    "facebook_coverage": 0.30,  # 30% of businesses have Facebook pages
    "linkedin_coverage": 0.50,  # 50% of businesses need LinkedIn enrichment
    "email_success_rate": 0.15,  # 15% overall email discovery success rate
    "verification_rate": 1.0,  # Verify 100% of discovered emails
}


# Coverage Profile Default Costs (USD)
# Fallback estimates when detailed analysis isn't available
COVERAGE_PROFILE_DEFAULTS = {
    "budget": 25.00,
    "balanced": 50.00,
    "aggressive": 100.00,
    "custom": 50.00,
}


def get_service_cost(service: str, items: int) -> float:
    """
    Calculate cost for a service given number of items processed.

    Args:
        service: Service name (e.g., 'google_maps', 'facebook', 'linkedin', 'bouncer')
        items: Number of items processed

    Returns:
        Cost in USD

    Raises:
        ValueError: If service name is not recognized
    """
    if service not in API_COSTS:
        raise ValueError(
            f"Unknown service: {service}. "
            f"Available services: {', '.join(API_COSTS.keys())}"
        )

    return API_COSTS[service].calculate_cost(items)


def get_openai_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """
    Calculate OpenAI API cost for given token usage.

    Args:
        model: Model name (e.g., 'gpt-4o', 'gpt-4', 'gpt-3.5-turbo')
        input_tokens: Number of input tokens
        output_tokens: Number of output tokens

    Returns:
        Cost in USD

    Raises:
        ValueError: If model name is not recognized
    """
    if model not in OPENAI_COSTS:
        raise ValueError(
            f"Unknown model: {model}. "
            f"Available models: {', '.join(OPENAI_COSTS.keys())}"
        )

    return OPENAI_COSTS[model].calculate_cost(input_tokens, output_tokens)


def estimate_campaign_cost(
    total_businesses: int,
    coverage_profile: str = "balanced",
    include_facebook: bool = True,
    include_linkedin: bool = True,
    use_premium_linkedin: bool = False
) -> Dict[str, float]:
    """
    Estimate total campaign cost based on expected business count.

    Args:
        total_businesses: Expected number of businesses to scrape
        coverage_profile: Coverage profile name (for fallback estimation)
        include_facebook: Whether to include Facebook enrichment
        include_linkedin: Whether to include LinkedIn enrichment
        use_premium_linkedin: Whether to use premium LinkedIn scraper

    Returns:
        Dictionary with cost breakdown and totals
    """
    costs = {
        "google_maps_cost": get_service_cost("google_maps", total_businesses),
        "facebook_cost": 0.0,
        "linkedin_cost": 0.0,
        "bouncer_cost": 0.0,
    }

    if include_facebook:
        facebook_pages = int(total_businesses * ENRICHMENT_ASSUMPTIONS["facebook_coverage"])
        costs["facebook_cost"] = get_service_cost("facebook", facebook_pages)

    if include_linkedin:
        linkedin_searches = int(total_businesses * ENRICHMENT_ASSUMPTIONS["linkedin_coverage"])
        service = "linkedin_premium" if use_premium_linkedin else "linkedin"
        costs["linkedin_cost"] = get_service_cost(service, linkedin_searches)

        # Bouncer verification for discovered emails
        estimated_emails = int(linkedin_searches * ENRICHMENT_ASSUMPTIONS["email_success_rate"])
        costs["bouncer_cost"] = get_service_cost("bouncer", estimated_emails)

    # Calculate totals
    costs["total_cost"] = sum(costs.values())
    costs["cost_per_business"] = costs["total_cost"] / total_businesses if total_businesses > 0 else 0

    # Estimate cost per email (assuming 15% success rate)
    estimated_emails = int(total_businesses * ENRICHMENT_ASSUMPTIONS["email_success_rate"])
    costs["estimated_emails"] = estimated_emails
    costs["cost_per_email"] = costs["total_cost"] / estimated_emails if estimated_emails > 0 else 0

    return costs


# Export commonly used functions and constants
__all__ = [
    'API_COSTS',
    'OPENAI_COSTS',
    'ENRICHMENT_ASSUMPTIONS',
    'COVERAGE_PROFILE_DEFAULTS',
    'get_service_cost',
    'get_openai_cost',
    'estimate_campaign_cost',
]

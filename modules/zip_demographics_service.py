"""
ZIP Demographics Service

Provides on-demand ZIP code demographic lookups with intelligent caching.
- Checks database first (pre-populated with 29k+ ZIPs)
- Fetches from Census API for missing ZIPs
- Caches new data to database for future use

Usage:
    from zip_demographics_service import ZipDemographicsService

    service = ZipDemographicsService(supabase_client)
    demographics = service.get_demographics(['78701', '90210', '10001'])
    # Returns: {'78701': {...}, '90210': {...}, '10001': {...}}
"""

import os
import json
import logging
import requests
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# Census ACS API configuration
ACS_BASE_URL = "https://api.census.gov/data/2022/acs/acs5"

# Key ACS variables for business-relevant demographics
ACS_VARIABLES = {
    "B01002_001E": "median_age",
    "B01001_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B19301_001E": "per_capita_income",
    "B15003_022E": "edu_bachelors",
    "B15003_023E": "edu_masters",
    "B15003_024E": "edu_professional",
    "B15003_025E": "edu_doctorate",
    "B15003_001E": "edu_total",
    "B23025_005E": "unemployed",
    "B23025_002E": "labor_force",
    "B25003_002E": "housing_owner",
    "B25003_003E": "housing_renter",
    "B25003_001E": "housing_total",
}


class ZipDemographicsService:
    """Service for on-demand ZIP demographics with caching."""

    def __init__(self, supabase_client):
        """
        Initialize the service.

        Args:
            supabase_client: Initialized Supabase client
        """
        self.client = supabase_client
        self.census_api_key = self._get_census_api_key()
        self._cache = {}  # In-memory cache for current session

    def _get_census_api_key(self) -> Optional[str]:
        """Get Census API key from environment or app-state."""
        key = os.getenv("CENSUS_API_KEY")
        if key:
            return key

        # Try app-state.json
        try:
            app_state_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                '.app-state.json'
            )
            if os.path.exists(app_state_path):
                with open(app_state_path) as f:
                    app_state = json.load(f)
                    return app_state.get('apiKeys', {}).get('census_api_key')
        except Exception:
            pass

        return None

    def get_demographics(self, zip_codes: List[str]) -> Dict[str, Dict]:
        """
        Get demographics for multiple ZIP codes.

        Checks database first, fetches from Census API for missing ZIPs,
        and caches new data to database.

        Args:
            zip_codes: List of 5-digit ZIP codes

        Returns:
            Dict mapping zip_code -> demographics dict
        """
        if not zip_codes:
            return {}

        # Normalize ZIP codes to 5 digits
        zip_codes = [str(z).zfill(5) for z in zip_codes]
        unique_zips = list(set(zip_codes))

        # Check in-memory cache first
        results = {}
        uncached_zips = []
        for z in unique_zips:
            if z in self._cache:
                results[z] = self._cache[z]
            else:
                uncached_zips.append(z)

        if not uncached_zips:
            return results

        # Check database for remaining ZIPs
        db_results = self._fetch_from_database(uncached_zips)
        results.update(db_results)

        # Update in-memory cache
        for z, data in db_results.items():
            self._cache[z] = data

        # Find ZIPs still missing (not in database)
        missing_zips = [z for z in uncached_zips if z not in db_results]

        if missing_zips:
            logger.info(f"Fetching {len(missing_zips)} new ZIP codes from Census API...")
            census_results = self._fetch_from_census(missing_zips)

            # Save new ZIPs to database for future use
            if census_results:
                self._save_to_database(census_results)
                results.update(census_results)

                # Update in-memory cache
                for z, data in census_results.items():
                    self._cache[z] = data

        return results

    def get_single(self, zip_code: str) -> Optional[Dict]:
        """Get demographics for a single ZIP code."""
        results = self.get_demographics([zip_code])
        return results.get(str(zip_code).zfill(5))

    def _fetch_from_database(self, zip_codes: List[str]) -> Dict[str, Dict]:
        """Fetch demographics from database."""
        try:
            result = self.client.table('zip_demographics').select(
                'zip_code, city, state, county, population, population_density, '
                'median_household_income, median_age, per_capita_income, '
                'pct_college_or_higher, unemployment_rate, pct_self_employed, '
                'pct_owner_occupied, pct_renter_occupied, '
                'market_opportunity_score, lead_quality_tier, email_rate, '
                'known_for, target_industries, local_economy_summary'
            ).in_('zip_code', zip_codes).execute()

            return {row['zip_code']: row for row in result.data}
        except Exception as e:
            logger.error(f"Error fetching from database: {e}")
            return {}

    def _fetch_from_census(self, zip_codes: List[str]) -> Dict[str, Dict]:
        """Fetch demographics from Census API for missing ZIPs."""
        if not self.census_api_key:
            logger.warning("Census API key not configured. Skipping Census lookup.")
            return {}

        results = {}

        # Census API works best with individual ZIP lookups or all ZCTAs
        # For a small number of ZIPs, we'll do individual lookups
        for zip_code in zip_codes[:20]:  # Limit to 20 to avoid rate limits
            data = self._fetch_single_from_census(zip_code)
            if data:
                results[zip_code] = data

        return results

    def _fetch_single_from_census(self, zip_code: str) -> Optional[Dict]:
        """Fetch a single ZIP from Census API."""
        try:
            variables = ",".join(ACS_VARIABLES.keys())
            url = f"{ACS_BASE_URL}?get=NAME,{variables}&for=zip%20code%20tabulation%20area:{zip_code}"
            if self.census_api_key:
                url += f"&key={self.census_api_key}"

            response = requests.get(url, timeout=30)

            if response.status_code == 204 or not response.content:
                # ZIP not found in Census data
                return None

            response.raise_for_status()
            data = response.json()

            if len(data) < 2:
                return None

            headers = data[0]
            values = data[1]
            raw = dict(zip(headers, values))

            return self._process_census_data(zip_code, raw)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(f"ZIP {zip_code} not found in Census data")
            else:
                logger.error(f"Census API error for {zip_code}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching Census data for {zip_code}: {e}")
            return None

    def _process_census_data(self, zip_code: str, raw: Dict) -> Dict:
        """Process raw Census data into our schema format."""
        def safe_float(val):
            try:
                v = float(val)
                return v if v >= 0 else None
            except:
                return None

        def safe_int(val):
            try:
                v = int(float(val))
                return v if v >= 0 else None
            except:
                return None

        result = {
            'zip_code': zip_code,
            'data_source': 'census_acs_2022',
        }

        # Population
        result['population'] = safe_int(raw.get('B01001_001E'))

        # Age
        result['median_age'] = safe_float(raw.get('B01002_001E'))

        # Income
        result['median_household_income'] = safe_int(raw.get('B19013_001E'))
        result['per_capita_income'] = safe_int(raw.get('B19301_001E'))

        # Education (calculate % college or higher)
        edu_total = safe_float(raw.get('B15003_001E')) or 1
        bachelors = safe_float(raw.get('B15003_022E')) or 0
        masters = safe_float(raw.get('B15003_023E')) or 0
        professional = safe_float(raw.get('B15003_024E')) or 0
        doctorate = safe_float(raw.get('B15003_025E')) or 0
        college_plus = bachelors + masters + professional + doctorate
        result['pct_college_or_higher'] = round((college_plus / edu_total) * 100, 2) if edu_total > 0 else None

        # Employment
        labor_force = safe_float(raw.get('B23025_002E')) or 1
        unemployed = safe_float(raw.get('B23025_005E')) or 0
        result['unemployment_rate'] = round((unemployed / labor_force) * 100, 2) if labor_force > 0 else None

        # Housing
        housing_total = safe_float(raw.get('B25003_001E')) or 1
        owner = safe_float(raw.get('B25003_002E')) or 0
        renter = safe_float(raw.get('B25003_003E')) or 0
        result['pct_owner_occupied'] = round((owner / housing_total) * 100, 2) if housing_total > 0 else None
        result['pct_renter_occupied'] = round((renter / housing_total) * 100, 2) if housing_total > 0 else None

        # Calculate market opportunity score (simple heuristic)
        result['market_opportunity_score'] = self._calculate_market_score(result)
        result['lead_quality_tier'] = self._calculate_tier(result['market_opportunity_score'])

        return result

    def _calculate_market_score(self, data: Dict) -> float:
        """Calculate market opportunity score (0-100) based on demographics."""
        score = 50  # Base score

        # Income factor (+/- 20 points)
        income = data.get('median_household_income')
        if income:
            if income > 100000:
                score += 20
            elif income > 75000:
                score += 15
            elif income > 50000:
                score += 10
            elif income < 30000:
                score -= 10

        # Education factor (+/- 15 points)
        edu = data.get('pct_college_or_higher')
        if edu:
            if edu > 50:
                score += 15
            elif edu > 30:
                score += 10
            elif edu < 15:
                score -= 5

        # Population factor (+/- 10 points)
        pop = data.get('population')
        if pop:
            if pop > 50000:
                score += 10
            elif pop > 20000:
                score += 5
            elif pop < 1000:
                score -= 10

        # Unemployment factor (+/- 5 points)
        unemp = data.get('unemployment_rate')
        if unemp:
            if unemp < 3:
                score += 5
            elif unemp > 8:
                score -= 5

        return max(0, min(100, score))

    def _calculate_tier(self, score: Optional[float]) -> str:
        """Calculate lead quality tier from market score."""
        if score is None:
            return 'C'
        if score >= 75:
            return 'A'
        elif score >= 55:
            return 'B'
        elif score >= 35:
            return 'C'
        else:
            return 'D'

    def _save_to_database(self, demographics: Dict[str, Dict]) -> int:
        """Save new ZIP demographics to database.

        Only saves records that have city/state info (from existing DB records).
        Census-only data is kept in memory cache but not persisted since it's incomplete.
        """
        saved = 0
        for zip_code, data in demographics.items():
            # Skip saving if we don't have city/state (required NOT NULL fields)
            # This happens with Census-only data
            if not data.get('city') or not data.get('state'):
                logger.debug(f"Skipping save for {zip_code} - missing city/state (Census-only data)")
                continue

            try:
                # Use upsert to handle duplicates
                self.client.table('zip_demographics').upsert(
                    data,
                    on_conflict='zip_code'
                ).execute()
                saved += 1
            except Exception as e:
                logger.error(f"Error saving ZIP {zip_code}: {e}")

        if saved > 0:
            logger.info(f"Cached {saved} new ZIP codes to database")

        return saved

    def enrich_business_data(self, business: Dict) -> Dict:
        """
        Enrich a single business record with ZIP demographics.

        Args:
            business: Business dict with 'zip_code' or 'postal_code' field

        Returns:
            Business dict with added demographics fields
        """
        zip_code = business.get('zip_code') or business.get('postal_code') or business.get('extracted_zip')

        if not zip_code:
            return business

        # Normalize to 5 digits
        zip_code = str(zip_code)[:5].zfill(5)

        demographics = self.get_single(zip_code)

        if demographics:
            # Add demographics fields with 'zip_' prefix
            business['zip_demographics'] = {
                'population': demographics.get('population'),
                'median_income': demographics.get('median_household_income'),
                'median_age': demographics.get('median_age'),
                'pct_college': demographics.get('pct_college_or_higher'),
                'unemployment_rate': demographics.get('unemployment_rate'),
                'market_score': demographics.get('market_opportunity_score'),
                'lead_tier': demographics.get('lead_quality_tier'),
                'known_for': demographics.get('known_for'),
                'target_industries': demographics.get('target_industries'),
            }

        return business

    def enrich_businesses_batch(self, businesses: List[Dict]) -> List[Dict]:
        """
        Enrich a batch of businesses with ZIP demographics.

        More efficient than enriching one at a time - does a single batch lookup.

        Args:
            businesses: List of business dicts

        Returns:
            List of businesses with added demographics
        """
        # Collect all unique ZIP codes
        zip_codes = set()
        for b in businesses:
            z = b.get('zip_code') or b.get('postal_code') or b.get('extracted_zip')
            if z:
                zip_codes.add(str(z)[:5].zfill(5))

        if not zip_codes:
            return businesses

        # Batch fetch all demographics at once
        demographics_map = self.get_demographics(list(zip_codes))

        # Enrich each business
        for business in businesses:
            z = business.get('zip_code') or business.get('postal_code') or business.get('extracted_zip')
            if z:
                z = str(z)[:5].zfill(5)
                demographics = demographics_map.get(z, {})

                if demographics:
                    business['zip_demographics'] = {
                        'population': demographics.get('population'),
                        'median_income': demographics.get('median_household_income'),
                        'median_age': demographics.get('median_age'),
                        'pct_college': demographics.get('pct_college_or_higher'),
                        'unemployment_rate': demographics.get('unemployment_rate'),
                        'market_score': demographics.get('market_opportunity_score'),
                        'lead_tier': demographics.get('lead_quality_tier'),
                        'known_for': demographics.get('known_for'),
                        'target_industries': demographics.get('target_industries'),
                    }

        return businesses

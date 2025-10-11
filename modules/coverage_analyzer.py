"""
Coverage Analyzer Module
Intelligently determines which ZIP codes to search based on location and coverage profile
Uses OpenAI to analyze locations and select optimal ZIP codes
"""

import logging
import json
import requests
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import openai
from config import OPENAI_API_KEY, AI_MODEL_SUMMARY

@dataclass
class CoverageProfile:
    """Coverage profile configuration"""
    name: str
    coverage_percentage: float  # Target coverage percentage
    min_zips: int  # Minimum ZIP codes to include
    max_zips: int  # Maximum ZIP codes to include (None = unlimited)
    description: str
    warning_threshold: int  # Warn if exceeding this many ZIPs

class CoverageAnalyzer:
    def __init__(self, supabase_manager=None):
        """Initialize the coverage analyzer"""
        self.db = supabase_manager
        self.openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
        # Define coverage profiles with smart limits
        self.profiles = {
            "budget": CoverageProfile(
                name="budget",
                coverage_percentage=0.90,  # Target 90% coverage
                min_zips=5,  # At least 5 ZIPs
                max_zips=10,  # Maximum 10 ZIPs
                description="Cost-effective coverage of high-value areas only",
                warning_threshold=8
            ),
            "balanced": CoverageProfile(
                name="balanced", 
                coverage_percentage=0.94,  # Target 94% coverage
                min_zips=10,  # At least 10 ZIPs
                max_zips=25,  # Maximum 25 ZIPs
                description="Good coverage focusing on business-dense areas",
                warning_threshold=20
            ),
            "aggressive": CoverageProfile(
                name="aggressive",
                coverage_percentage=0.97,  # Target 97% coverage
                min_zips=25,  # At least 25 ZIPs
                max_zips=None,  # No hard limit
                description="Complete market coverage - all relevant ZIP codes",
                warning_threshold=50  # Warn if >50 ZIPs
            ),
            "custom": CoverageProfile(
                name="custom",
                coverage_percentage=0.0,  # User-defined
                min_zips=1,
                max_zips=None,
                description="Custom selection based on specific requirements",
                warning_threshold=100
            )
        }
        
    def analyze_location(self, location: str, keywords: List[str], profile: str = "balanced") -> Dict[str, Any]:
        """
        Use AI to analyze location and determine optimal ZIP codes to search
        
        Args:
            location: Location string (e.g., "Los Angeles, CA", "Austin, TX", "90210")
            keywords: Business keywords to search for
            profile: Coverage profile to use
            
        Returns:
            Dictionary with selected ZIP codes and analysis
        """
        try:
            logging.info(f"🤖 Analyzing location: {location} with profile: {profile}")
            
            # Check if location is already a ZIP code
            if self._is_zip_code(location):
                return self._handle_single_zip(location, keywords)
            
            # First, check if this is a state-level search
            location_check = self._check_location_type(location)
            
            if location_check.get("is_state", False):
                # Use multi-step approach for states
                logging.info(f"🌎 Detected state-level search for {location}, using multi-step analysis")
                zip_analysis = self._analyze_state_location(location, keywords, profile)
            else:
                # Use standard single-step for cities/neighborhoods
                zip_analysis = self._ai_analyze_location(location, keywords, profile)
            
            # If we have a database, check for existing ZIP code data
            if self.db:
                zip_analysis = self._enrich_with_database(zip_analysis)
            
            # Calculate costs
            zip_analysis = self._calculate_costs(zip_analysis)
            
            return zip_analysis
            
        except Exception as e:
            logging.error(f"Error analyzing location: {e}")
            return self._fallback_analysis(location, keywords, profile)
    
    def _ai_analyze_location(self, location: str, keywords: List[str], profile: str) -> Dict[str, Any]:
        """Use OpenAI to intelligently analyze location and suggest ZIP codes"""
        
        profile_config = self.profiles.get(profile, self.profiles["balanced"])
        
        prompt = f"""You are a geographic data analyst specializing in business density analysis.

Analyze this location and provide ZIP codes for a business scraping campaign:

Location: {location}
Business Keywords: {', '.join(keywords)}
Coverage Profile: {profile} - {profile_config.description}
Max ZIP codes: {profile_config.max_zips if profile_config.max_zips else 'No limit - provide as many as needed'}

Based on the location and keywords, identify the most relevant ZIP codes where these types of businesses would be concentrated.

Consider:
1. Business density in each area
2. Relevance to the keywords (e.g., "restaurants" would be dense in downtown/entertainment districts)
3. Geographic distribution for good coverage
4. The coverage profile requirements

IMPORTANT GUIDELINES BY LOCATION TYPE:
- For STATES: Return MANY ZIP codes (minimum 50-100 for large states like Texas, California, Florida). Cover ALL major cities.
- For CITIES: Return 15-30 ZIP codes covering all commercial districts
- For NEIGHBORHOODS: Return 3-10 ZIP codes in the immediate area

Return a JSON response with this exact structure:
{{
    "location_type": "city|state|region|neighborhood",
    "primary_city": "City name or 'Multiple' for states",
    "state": "State code (2 letters)",
    "zip_codes": [
        {{
            "zip": "12345",
            "neighborhood": "Neighborhood/Area name, City",
            "density_score": 1-10,  // Business density score
            "relevance_score": 1-10, // Relevance to keywords
            "estimated_businesses": 50-500 // Estimate based on area
        }}
    ],
    "reasoning": "Brief explanation of ZIP code selection strategy",
    "total_estimated_businesses": 0,
    "coverage_notes": "Any important notes about coverage"
}}

For the profile "{profile}":
- Aggressive: Return MANY ZIP codes. For states: 100-300+ ZIPs. For cities: 20-40 ZIPs. Include ALL commercial areas.
- Balanced: Return moderate number. For states: 50-100 ZIPs. For cities: 10-20 ZIPs. Focus on high/medium density areas.
- Budget: Return minimal ZIPs. For states: 20-40 ZIPs. For cities: 5-10 ZIPs. Only highest density districts.

CRITICAL FOR STATE SEARCHES: 
- For states, include ALL commercial ZIP codes across ALL major, medium, and small cities
- Texas aggressive should return 200+ ZIP codes (Houston: 30+, Dallas: 30+, Austin: 20+, San Antonio: 20+, Fort Worth: 15+, etc.)
- Each major city should contribute 15-30 ZIP codes
- Each medium city should contribute 5-15 ZIP codes  
- Each small city should contribute 2-5 ZIP codes
- DO NOT return just one ZIP per city - return ALL relevant commercial ZIPs in each city"""

        try:
            response = self.openai_client.chat.completions.create(
                model=AI_MODEL_SUMMARY,
                messages=[
                    {"role": "system", "content": "You are a geographic data expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Sort ZIP codes by combined score
            if "zip_codes" in result:
                for zip_data in result["zip_codes"]:
                    # Calculate combined score for ranking
                    density = zip_data.get("density_score", 5)
                    relevance = zip_data.get("relevance_score", 5)
                    zip_data["combined_score"] = (density * 0.6) + (relevance * 0.4)
                
                # Sort by combined score
                result["zip_codes"].sort(key=lambda x: x["combined_score"], reverse=True)
                
                # Apply smart selection based on coverage target
                selected_zips = self._smart_select_zips(
                    result["zip_codes"],
                    profile_config
                )
                
                result["zip_codes"] = selected_zips["selected"]
                result["coverage_achieved"] = selected_zips["coverage_percentage"]
                result["warning"] = selected_zips.get("warning", "")
                
                # Update total
                result["total_estimated_businesses"] = sum(
                    z.get("estimated_businesses", 200) for z in result["zip_codes"]
                )
            
            logging.info(f"✅ AI selected {len(result.get('zip_codes', []))} ZIP codes")
            return result
            
        except Exception as e:
            logging.error(f"Error in AI analysis: {e}")
            raise
    
    def _smart_select_zips(self, all_zips: List[Dict], profile: CoverageProfile) -> Dict[str, Any]:
        """
        Intelligently select ZIP codes to achieve coverage target
        
        Args:
            all_zips: All available ZIP codes sorted by score
            profile: Coverage profile with targets and limits
            
        Returns:
            Dict with selected ZIPs and coverage metrics
        """
        if not all_zips:
            return {
                "selected": [],
                "coverage_percentage": 0,
                "warning": "No ZIP codes available"
            }
        
        # Calculate total businesses across all ZIPs
        total_businesses = sum(z.get("estimated_businesses", 0) for z in all_zips)
        if total_businesses == 0:
            return {
                "selected": all_zips[:profile.min_zips],
                "coverage_percentage": 0,
                "warning": "No business estimates available"
            }
        
        selected_zips = []
        businesses_covered = 0
        target_businesses = total_businesses * profile.coverage_percentage
        
        # First, ensure minimum ZIPs are included
        for i in range(min(profile.min_zips, len(all_zips))):
            selected_zips.append(all_zips[i])
            businesses_covered += all_zips[i].get("estimated_businesses", 0)
        
        # Keep adding ZIPs until we reach coverage target or hit max limit
        for i in range(profile.min_zips, len(all_zips)):
            # Check if we've reached coverage target
            if businesses_covered >= target_businesses:
                break
                
            # Check if we've hit max limit
            if profile.max_zips and len(selected_zips) >= profile.max_zips:
                break
                
            # Add this ZIP
            selected_zips.append(all_zips[i])
            businesses_covered += all_zips[i].get("estimated_businesses", 0)
        
        # Calculate actual coverage percentage
        coverage_percentage = (businesses_covered / total_businesses) * 100 if total_businesses > 0 else 0
        
        # Generate warnings
        warning = ""
        if len(selected_zips) >= profile.warning_threshold:
            if profile.name == "aggressive":
                warning = f"⚠️ This campaign will search {len(selected_zips)} ZIP codes. Consider if a state-level search would be more appropriate."
            else:
                warning = f"⚠️ Reached {len(selected_zips)} ZIP codes. Consider using '{profile.name}' profile or adjusting your location."
        
        if coverage_percentage < (profile.coverage_percentage * 100) and profile.max_zips and len(selected_zips) >= profile.max_zips:
            warning += f" Note: Only achieved {coverage_percentage:.1f}% coverage due to ZIP limit."
        
        logging.info(f"📊 Selected {len(selected_zips)} ZIPs achieving {coverage_percentage:.1f}% coverage")
        
        return {
            "selected": selected_zips,
            "coverage_percentage": coverage_percentage,
            "warning": warning,
            "total_businesses_available": total_businesses,
            "businesses_covered": businesses_covered
        }
    
    def _is_zip_code(self, location: str) -> bool:
        """Check if the location string is a ZIP code"""
        import re
        # Match 5-digit or 5+4 digit ZIP codes
        return bool(re.match(r'^\d{5}(-\d{4})?$', location.strip()))
    
    def _check_location_type(self, location: str) -> Dict[str, Any]:
        """Check if location is a state, city, or neighborhood"""
        # List of US state names and abbreviations
        us_states = {
            'alabama', 'al', 'alaska', 'ak', 'arizona', 'az', 'arkansas', 'ar', 'california', 'ca',
            'colorado', 'co', 'connecticut', 'ct', 'delaware', 'de', 'florida', 'fl', 'georgia', 'ga',
            'hawaii', 'hi', 'idaho', 'id', 'illinois', 'il', 'indiana', 'in', 'iowa', 'ia',
            'kansas', 'ks', 'kentucky', 'ky', 'louisiana', 'la', 'maine', 'me', 'maryland', 'md',
            'massachusetts', 'ma', 'michigan', 'mi', 'minnesota', 'mn', 'mississippi', 'ms',
            'missouri', 'mo', 'montana', 'mt', 'nebraska', 'ne', 'nevada', 'nv', 'new hampshire', 'nh',
            'new jersey', 'nj', 'new mexico', 'nm', 'new york', 'ny', 'north carolina', 'nc',
            'north dakota', 'nd', 'ohio', 'oh', 'oklahoma', 'ok', 'oregon', 'or', 'pennsylvania', 'pa',
            'rhode island', 'ri', 'south carolina', 'sc', 'south dakota', 'sd', 'tennessee', 'tn',
            'texas', 'tx', 'utah', 'ut', 'vermont', 'vt', 'virginia', 'va', 'washington', 'wa',
            'west virginia', 'wv', 'wisconsin', 'wi', 'wyoming', 'wy'
        }
        
        location_lower = location.lower().strip()
        
        # Check if it's a standalone state name
        if location_lower in us_states:
            return {"is_state": True, "state": location_lower}
        
        # Check if it contains a comma (city, state format)
        if ',' in location:
            parts = [p.strip() for p in location.split(',')]
            if len(parts) == 2 and parts[1].lower() in us_states:
                # It's a city, state format
                return {"is_state": False, "city": parts[0], "state": parts[1]}
        
        return {"is_state": False, "location": location}
    
    def _analyze_state_location(self, location: str, keywords: List[str], profile: str) -> Dict[str, Any]:
        """
        Multi-step analysis for state-level locations
        Step 1: Get list of cities in the state
        Step 2: Analyze each city individually (in parallel when appropriate)
        Step 3: Combine all ZIP codes
        """
        # Always try parallel processing first for states
        try:
            # Try to use parallel processing for better performance
            from coverage_analyzer_parallel import analyze_state_location_parallel
            logging.info("🚀 Using parallel processing for comprehensive state analysis")
            return analyze_state_location_parallel(self, location, keywords, profile)
        except ImportError:
            logging.info("Parallel processing not available, falling back to sequential")
            # Fall through to sequential processing
        except Exception as e:
            logging.error(f"Parallel analysis failed: {e}, falling back to sequential")
            # Fall through to sequential processing
        
        try:
            # Step 1: Get cities in the state
            cities = self._get_cities_in_state(location, profile)
            
            if not cities:
                # Fallback to single analysis if no cities found
                return self._ai_analyze_location(location, keywords, profile)
            
            # Limit cities to prevent timeout (max 10 initially)
            if len(cities) > 10:
                logging.info(f"📍 Found {len(cities)} cities, limiting to top 10 for efficiency")
                # Prioritize by size: major > medium > small
                cities = sorted(cities, key=lambda x: {'major': 0, 'medium': 1, 'small': 2}.get(x.get('size', 'medium'), 3))[:10]
            else:
                logging.info(f"📍 Found {len(cities)} cities to analyze in {location}")
            
            # Step 2: Analyze each city
            all_zip_codes = []
            total_businesses = 0
            analyzed_count = 0
            
            for idx, city_info in enumerate(cities, 1):
                city_name = city_info['city']
                city_size = city_info.get('size', 'medium')
                
                # Use aggressive profile for all cities when state is aggressive
                # This ensures we get many ZIPs from each city
                if profile == "aggressive":
                    # For aggressive state analysis, major cities should return many ZIPs
                    if city_size == "major":
                        city_profile = "aggressive"  # Should return 25+ ZIPs
                    elif city_size == "medium":
                        city_profile = "aggressive"  # Should return 15-25 ZIPs
                    else:
                        city_profile = "balanced"  # Small cities get 10-15 ZIPs
                elif profile == "balanced":
                    if city_size == "major":
                        city_profile = "balanced"  # 10-25 ZIPs
                    elif city_size == "medium":
                        city_profile = "balanced"  # 10-20 ZIPs
                    else:
                        city_profile = "budget"  # 5-10 ZIPs
                else:  # budget
                    if city_size == "major":
                        city_profile = "budget"  # 5-10 ZIPs
                    else:
                        city_profile = "budget"  # 5 ZIPs max
                
                logging.info(f"🏙️ [{idx}/{len(cities)}] Analyzing {city_name}, {location} ({city_size} city, {city_profile} profile)")
                
                try:
                    # Analyze this city
                    city_location = f"{city_name}, {location}"
                    city_analysis = self._ai_analyze_location(city_location, keywords, city_profile)
                    
                    if city_analysis.get("zip_codes"):
                        # Add city name to each ZIP for clarity
                        for zip_data in city_analysis["zip_codes"]:
                            zip_data["city"] = city_name
                            all_zip_codes.append(zip_data)
                        
                        total_businesses += city_analysis.get("total_estimated_businesses", 0)
                        analyzed_count += 1
                    
                    # Small delay to avoid rate limiting
                    time.sleep(0.5)
                    
                except Exception as e:
                    logging.error(f"Error analyzing {city_name}: {e}")
                    continue
            
            # Step 3: Combine and deduplicate
            unique_zips = self._deduplicate_zips(all_zip_codes)
            
            logging.info(f"✅ Total unique ZIP codes for {location}: {len(unique_zips)}")
            
            # Build final result
            result = {
                "location_type": "state",
                "primary_city": "Multiple",
                "state": self._get_state_abbreviation(location),
                "zip_codes": unique_zips,
                "reasoning": f"State-level analysis covering {analyzed_count} of {len(cities)} top cities across {location}",
                "total_estimated_businesses": total_businesses,
                "coverage_notes": f"Analyzed {analyzed_count} cities (limited to top 10 for efficiency) to provide state coverage. Total unique ZIP codes: {len(unique_zips)}",
                "cities_analyzed": analyzed_count
            }
            
            # For state analysis, DON'T apply smart selection limits again
            # The individual city analyses already applied their limits
            # We want ALL the ZIPs from all cities combined
            result["coverage_achieved"] = 95.0  # Estimate high coverage from multiple cities
            result["warning"] = ""
            
            # Only apply minimal filtering to remove low-scoring ZIPs
            if len(unique_zips) > 500:  # Safety limit for extreme cases
                # Keep top 500 by score
                unique_zips = sorted(unique_zips, key=lambda x: x.get("combined_score", 0), reverse=True)[:500]
                result["warning"] = "Limited to top 500 ZIP codes for practical reasons"
            
            result["zip_codes"] = unique_zips
            
            return result
            
        except Exception as e:
            logging.error(f"Error in state analysis: {e}")
            # Fallback to single analysis
            return self._ai_analyze_location(location, keywords, profile)
    
    def _get_cities_in_state(self, state: str, profile: str) -> List[Dict[str, Any]]:
        """Use AI to get a list of cities in the state"""
        logging.info(f"🔍 Getting cities in {state} for profile {profile}")
        if not self.openai_client:
            logging.warning("No OpenAI client available")
            return []
        
        # Determine how many cities based on profile
        if profile == "aggressive":
            cities_instruction = """Categories:
- major: 500,000+ population (include ALL of these)
- medium: 100,000-500,000 population (include ALL of these)
- small: 50,000-100,000 population (include top 10)"""
        elif profile == "balanced":
            cities_instruction = """Categories:
- major: 500,000+ population (include ALL of these)
- medium: 100,000-500,000 population (include top 10)
- small: 50,000-100,000 population (include top 5)"""
        else:  # budget
            cities_instruction = """Categories:
- major: 500,000+ population (include ALL of these)
- medium: 100,000-500,000 population (include top 5)
- small: 50,000-100,000 population (skip these for budget profile)"""
        
        prompt = f"""List the major, medium, and small cities in {state} for business scraping.

Return a JSON array with this structure:
[
    {{"city": "Houston", "size": "major", "population": 2300000}},
    {{"city": "Austin", "size": "major", "population": 960000}},
    {{"city": "Plano", "size": "medium", "population": 285000}},
    {{"city": "Round Rock", "size": "small", "population": 120000}}
]

{cities_instruction}

Focus on cities with significant commercial activity. There's no maximum limit for major cities - include ALL cities over 500k population."""
        
        try:
            response = self.openai_client.chat.completions.create(
                model=AI_MODEL_SUMMARY,
                messages=[
                    {"role": "system", "content": "You are a geographic data expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Handle both array and object responses
            if isinstance(result, list):
                return result
            elif isinstance(result, dict) and "cities" in result:
                return result["cities"]
            else:
                return []
                
        except Exception as e:
            logging.error(f"Error getting cities: {e}")
            return []
    
    def _deduplicate_zips(self, zip_codes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate ZIP codes, keeping the one with highest score"""
        seen = {}
        for zip_data in zip_codes:
            zip_code = zip_data["zip"]
            score = zip_data.get("combined_score", 0)
            
            if zip_code not in seen or score > seen[zip_code].get("combined_score", 0):
                seen[zip_code] = zip_data
        
        # Return sorted by score
        return sorted(seen.values(), key=lambda x: x.get("combined_score", 0), reverse=True)
    
    def _get_state_abbreviation(self, state: str) -> str:
        """Get state abbreviation from state name"""
        state_abbr = {
            'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 'california': 'CA',
            'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE', 'florida': 'FL', 'georgia': 'GA',
            'hawaii': 'HI', 'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
            'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
            'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
            'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV', 'new hampshire': 'NH',
            'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC',
            'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA',
            'rhode island': 'RI', 'south carolina': 'SC', 'south dakota': 'SD', 'tennessee': 'TN',
            'texas': 'TX', 'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
            'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY'
        }
        
        state_lower = state.lower().strip()
        
        # If already an abbreviation, return uppercase
        if len(state_lower) == 2:
            return state_lower.upper()
        
        return state_abbr.get(state_lower, state[:2].upper())
    
    def _handle_single_zip(self, zip_code: str, keywords: List[str]) -> Dict[str, Any]:
        """Handle when a single ZIP code is provided"""
        return {
            "location_type": "zip_code",
            "primary_city": "Unknown",
            "state": "Unknown",
            "zip_codes": [
                {
                    "zip": zip_code.strip(),
                    "neighborhood": "Direct ZIP search",
                    "density_score": 5,
                    "relevance_score": 10,
                    "estimated_businesses": 250
                }
            ],
            "reasoning": f"Direct ZIP code search for {zip_code}",
            "total_estimated_businesses": 250,
            "coverage_notes": "Searching single ZIP code as provided"
        }
    
    def _enrich_with_database(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich AI analysis with database ZIP code data if available"""
        try:
            if not self.db or "zip_codes" not in analysis:
                return analysis
            
            # Get ZIP codes from analysis
            zip_list = [z["zip"] for z in analysis["zip_codes"]]
            
            # Query database for these ZIP codes
            result = self.db.client.table("gmaps_zip_codes").select("*").in_("zip_code", zip_list).execute()
            
            if result.data:
                # Create lookup dict
                db_zips = {z["zip_code"]: z for z in result.data}
                
                # Enrich analysis with database data
                for zip_data in analysis["zip_codes"]:
                    if zip_data["zip"] in db_zips:
                        db_info = db_zips[zip_data["zip"]]
                        zip_data["neighborhood"] = db_info.get("neighborhood", zip_data["neighborhood"])
                        zip_data["estimated_businesses"] = db_info.get("expected_businesses", zip_data["estimated_businesses"])
                        zip_data["actual_businesses"] = db_info.get("actual_businesses")
                        zip_data["last_scraped"] = db_info.get("last_scraped_at")
                        
                logging.info(f"✅ Enriched {len(db_zips)} ZIP codes with database data")
                
        except Exception as e:
            logging.warning(f"Could not enrich with database: {e}")
        
        return analysis
    
    def _calculate_costs(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate estimated costs for the campaign"""
        
        if "zip_codes" not in analysis:
            return analysis
        
        total_businesses = analysis.get("total_estimated_businesses", 0)
        
        # Apify pricing: ~$7 per 1000 Google Maps results
        google_maps_cost = (total_businesses / 1000) * 7
        
        # Facebook scraping: ~$3 per 1000 pages (estimate 30% have Facebook)
        facebook_pages = total_businesses * 0.3
        facebook_cost = (facebook_pages / 1000) * 3
        
        analysis["cost_estimates"] = {
            "google_maps_cost": round(google_maps_cost, 2),
            "facebook_cost": round(facebook_cost, 2),
            "total_cost": round(google_maps_cost + facebook_cost, 2),
            "cost_per_business": round((google_maps_cost + facebook_cost) / total_businesses, 4) if total_businesses > 0 else 0,
            "estimated_emails": int(total_businesses * 0.15),  # Estimate 15% email success rate
            "cost_per_email": round((google_maps_cost + facebook_cost) / (total_businesses * 0.15), 2) if total_businesses > 0 else 0
        }
        
        return analysis
    
    def _fallback_analysis(self, location: str, keywords: List[str], profile: str) -> Dict[str, Any]:
        """Fallback analysis when AI is not available"""
        
        logging.warning("Using fallback analysis (no AI available)")
        
        # Simple fallback - suggest searching the location as-is
        return {
            "location_type": "unknown",
            "primary_city": location,
            "state": "Unknown",
            "zip_codes": [],
            "reasoning": "AI analysis unavailable - manual ZIP code entry required",
            "total_estimated_businesses": 0,
            "coverage_notes": "Please manually specify ZIP codes to search",
            "manual_mode": True
        }
    
    def get_nearby_zips(self, zip_code: str, radius_miles: int = 5) -> List[str]:
        """
        Get nearby ZIP codes within a radius (requires external API or database with lat/long)
        This is a placeholder for future enhancement
        """
        # This would require a ZIP code database with lat/long or an external API
        # For now, return just the provided ZIP
        return [zip_code]
    
    def optimize_coverage(self, zip_codes: List[Dict], budget: float) -> List[Dict]:
        """
        Optimize ZIP code selection based on budget constraints
        
        Args:
            zip_codes: List of ZIP code dictionaries with scores
            budget: Maximum budget in USD
            
        Returns:
            Optimized list of ZIP codes within budget
        """
        if not zip_codes:
            return []
        
        # Sort by combined score (already done in AI analysis)
        selected = []
        total_cost = 0
        
        for zip_data in zip_codes:
            # Estimate cost for this ZIP
            businesses = zip_data.get("estimated_businesses", 250)
            zip_cost = (businesses / 1000) * 7  # $7 per 1000 results
            
            if total_cost + zip_cost <= budget:
                selected.append(zip_data)
                total_cost += zip_cost
            else:
                break
        
        logging.info(f"✅ Selected {len(selected)} ZIPs within ${budget} budget (estimated cost: ${total_cost:.2f})")
        return selected
    
    def suggest_keywords_for_location(self, location: str) -> List[str]:
        """
        Use AI to suggest relevant business keywords for a location
        """
        prompt = f"""For the location "{location}", suggest 10 relevant business search keywords that would yield good results for B2B lead generation.

Focus on business types that:
1. Are likely to need services/products
2. Have decision makers accessible via email
3. Are numerous enough for good coverage

Return as a JSON array of keywords."""

        try:
            response = self.openai_client.chat.completions.create(
                model=AI_MODEL_SUMMARY,
                messages=[
                    {"role": "system", "content": "You are a B2B lead generation expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.5
            )
            
            keywords = json.loads(response.choices[0].message.content)
            return keywords if isinstance(keywords, list) else []
            
        except Exception as e:
            logging.error(f"Error suggesting keywords: {e}")
            # Fallback keywords
            return ["restaurants", "retail stores", "professional services", "medical offices", "salons"]

# Example usage
if __name__ == "__main__":
    analyzer = CoverageAnalyzer()
    
    # Test different locations
    test_cases = [
        ("Los Angeles, CA", ["restaurants", "cafes"], "balanced"),
        ("Austin, TX", ["tech startups", "software companies"], "budget"),
        ("90210", ["luxury retail", "boutiques"], "aggressive"),
        ("Manhattan, NY", ["financial services", "law firms"], "balanced")
    ]
    
    for location, keywords, profile in test_cases:
        print(f"\n{'='*60}")
        print(f"Location: {location}")
        print(f"Keywords: {', '.join(keywords)}")
        print(f"Profile: {profile}")
        print(f"{'='*60}")
        
        result = analyzer.analyze_location(location, keywords, profile)
        
        print(f"Location Type: {result.get('location_type')}")
        print(f"ZIP Codes Selected: {len(result.get('zip_codes', []))}")
        print(f"Estimated Businesses: {result.get('total_estimated_businesses')}")
        
        if "cost_estimates" in result:
            costs = result["cost_estimates"]
            print(f"Estimated Cost: ${costs['total_cost']}")
            print(f"Cost per Email: ${costs['cost_per_email']}")
        
        print(f"Reasoning: {result.get('reasoning')}")
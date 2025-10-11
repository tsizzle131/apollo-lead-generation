"""
Parallel processing version of state analysis for faster execution
"""
import concurrent.futures
import threading
import logging
from typing import List, Dict, Any

def analyze_state_location_parallel(analyzer, location: str, keywords: List[str], profile: str) -> Dict[str, Any]:
    """
    Multi-step analysis for state-level locations with parallel processing
    """
    try:
        # Step 1: Get cities in the state
        logging.info(f"🗺️ State-level analysis for {location} (PARALLEL)")
        logging.info(f"📊 Profile: {profile}, Keywords: {keywords}")
        
        logging.info(f"🔍 Step 1: Getting cities in {location}...")
        cities = analyzer._get_cities_in_state(location, profile)
        
        if not cities:
            # Fallback to single analysis if no cities found
            logging.info(f"⚠️ No cities found, falling back to direct analysis")
            return analyzer._ai_analyze_location(location, keywords, profile)
        
        logging.info(f"✅ Found {len(cities)} cities in {location}")
        
        # With parallel processing and longer timeout, we can handle more cities
        if len(cities) > 50:
            logging.info(f"📍 Found {len(cities)} cities, limiting to top 50")
            # Prioritize by size: major > medium > small
            cities = sorted(cities, key=lambda x: {'major': 0, 'medium': 1, 'small': 2}.get(x.get('size', 'medium'), 3))[:50]
        else:
            logging.info(f"📍 Found {len(cities)} cities to analyze in {location}")
        
        # Shared data with thread-safe access
        all_zip_codes = []
        total_businesses = 0
        analyzed_count = 0
        lock = threading.Lock()
        
        def analyze_city(city_info):
            """Analyze a single city (called in parallel)"""
            nonlocal total_businesses, analyzed_count
            
            city_name = city_info['city']
            city_size = city_info.get('size', 'medium')
            
            # Use different profiles based on city size
            city_profile = profile
            if profile == "aggressive":
                # For aggressive, major cities get thorough analysis
                if city_size == "major":
                    city_profile = "aggressive"
                else:
                    city_profile = "balanced"
            elif city_size == "major":
                city_profile = "balanced"
            elif city_size == "small":
                city_profile = "budget"
            
            logging.info(f"🏙️ Starting analysis of {city_name}, {location} ({city_size} city, {city_profile} profile)")
            
            try:
                # Analyze this city
                city_location = f"{city_name}, {location}"
                logging.info(f"📍 Making API call for {city_location}...")
                city_analysis = analyzer._ai_analyze_location(city_location, keywords, city_profile)
                logging.info(f"✅ API call completed for {city_location}")
                
                if city_analysis.get("zip_codes"):
                    # Add city name to each ZIP for clarity
                    city_zips = []
                    for zip_data in city_analysis["zip_codes"]:
                        zip_data["city"] = city_name
                        city_zips.append(zip_data)
                    
                    # Thread-safe update of shared data
                    with lock:
                        all_zip_codes.extend(city_zips)
                        total_businesses += city_analysis.get("total_estimated_businesses", 0)
                        analyzed_count += 1
                        logging.info(f"✅ Completed {city_name}: {len(city_zips)} ZIPs, {city_analysis.get('total_estimated_businesses', 0)} businesses")
                
                return city_analysis
                
            except Exception as e:
                logging.error(f"Error analyzing {city_name}: {e}")
                return None
        
        # Process cities in parallel (max 10 at a time for faster processing)
        logging.info(f"🚀 Starting parallel analysis of {len(cities)} cities with up to 10 workers...")
        logging.info(f"📋 Cities to analyze: {[c['city'] for c in cities[:10]]}{'...' if len(cities) > 10 else ''}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all tasks
            logging.info(f"📤 Submitting {len(cities)} tasks to executor...")
            future_to_city = {executor.submit(analyze_city, city): city for city in cities}
            logging.info(f"✅ All tasks submitted")
            
            # Wait for completion with longer timeout (up to 15 minutes as you mentioned)
            logging.info(f"⏳ Waiting for tasks to complete (timeout: 900 seconds)...")
            completed, pending = concurrent.futures.wait(
                future_to_city,
                timeout=900,  # 15 minute timeout for all cities
                return_when=concurrent.futures.ALL_COMPLETED
            )
            
            logging.info(f"📊 Completed: {len(completed)}, Pending: {len(pending)}")
            
            # Cancel any pending tasks
            for future in pending:
                future.cancel()
                city = future_to_city[future]
                logging.warning(f"⏱️ Timeout analyzing {city['city']}")
        
        logging.info(f"🏁 Parallel analysis complete: {analyzed_count}/{len(cities)} cities analyzed")
        
        # Step 3: Combine and deduplicate
        unique_zips = analyzer._deduplicate_zips(all_zip_codes)
        
        logging.info(f"✅ Total unique ZIP codes for {location}: {len(unique_zips)}")
        
        # Build final result
        result = {
            "location_type": "state",
            "primary_city": "Multiple",
            "state": analyzer._get_state_abbreviation(location),
            "zip_codes": unique_zips,
            "reasoning": f"State-level analysis (PARALLEL) covering {analyzed_count} of {len(cities)} top cities across {location}",
            "total_estimated_businesses": total_businesses,
            "coverage_notes": f"Analyzed {analyzed_count} cities in parallel (limited to top 15) to provide state coverage. Total unique ZIP codes: {len(unique_zips)}",
            "cities_analyzed": analyzed_count
        }
        
        # Apply smart selection if needed
        profile_config = analyzer.profiles.get(profile, analyzer.profiles["balanced"])
        smart_result = analyzer._smart_select_zips(unique_zips, profile_config)
        
        result["zip_codes"] = smart_result["selected"]
        result["coverage_achieved"] = smart_result["coverage_percentage"]
        result["warning"] = smart_result.get("warning", "")
        
        return result
        
    except Exception as e:
        logging.error(f"Error in parallel state analysis: {e}")
        # Fallback to single analysis
        return analyzer._ai_analyze_location(location, keywords, profile)
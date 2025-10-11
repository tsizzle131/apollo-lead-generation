"""
Los Angeles ZIP Code Data with Density Classifications
Based on business density analysis for optimal scraping coverage
"""

# Los Angeles ZIP codes organized by business density
LA_ZIP_CODES = {
    "very_high": [
        # Downtown LA & Financial District
        {"zip": "90012", "neighborhood": "Downtown LA", "businesses": 450},
        {"zip": "90013", "neighborhood": "Downtown LA", "businesses": 425},
        {"zip": "90014", "neighborhood": "Downtown LA", "businesses": 400},
        {"zip": "90015", "neighborhood": "Downtown LA", "businesses": 380},
        {"zip": "90017", "neighborhood": "Downtown LA", "businesses": 420},
        {"zip": "90071", "neighborhood": "Downtown LA", "businesses": 390},
        
        # Beverly Hills & West LA
        {"zip": "90210", "neighborhood": "Beverly Hills", "businesses": 380},
        {"zip": "90211", "neighborhood": "Beverly Hills", "businesses": 350},
        {"zip": "90212", "neighborhood": "Beverly Hills", "businesses": 360},
        {"zip": "90024", "neighborhood": "Westwood", "businesses": 340},
        {"zip": "90025", "neighborhood": "West LA", "businesses": 320},
        {"zip": "90064", "neighborhood": "West LA", "businesses": 310},
        
        # Hollywood
        {"zip": "90028", "neighborhood": "Hollywood", "businesses": 390},
        {"zip": "90038", "neighborhood": "Hollywood", "businesses": 340},
        {"zip": "90046", "neighborhood": "West Hollywood", "businesses": 330},
        {"zip": "90048", "neighborhood": "West Hollywood", "businesses": 320},
        {"zip": "90069", "neighborhood": "West Hollywood", "businesses": 310},
        
        # Century City & Culver City
        {"zip": "90067", "neighborhood": "Century City", "businesses": 380},
        {"zip": "90230", "neighborhood": "Culver City", "businesses": 300},
        {"zip": "90232", "neighborhood": "Culver City", "businesses": 290},
    ],
    
    "high": [
        # Santa Monica & Venice
        {"zip": "90401", "neighborhood": "Santa Monica", "businesses": 320},
        {"zip": "90402", "neighborhood": "Santa Monica", "businesses": 280},
        {"zip": "90403", "neighborhood": "Santa Monica", "businesses": 290},
        {"zip": "90404", "neighborhood": "Santa Monica", "businesses": 270},
        {"zip": "90405", "neighborhood": "Santa Monica", "businesses": 260},
        {"zip": "90291", "neighborhood": "Venice", "businesses": 280},
        {"zip": "90292", "neighborhood": "Marina del Rey", "businesses": 270},
        
        # Brentwood & Pacific Palisades
        {"zip": "90049", "neighborhood": "Brentwood", "businesses": 250},
        {"zip": "90272", "neighborhood": "Pacific Palisades", "businesses": 240},
        
        # Mid-City
        {"zip": "90004", "neighborhood": "Hancock Park", "businesses": 270},
        {"zip": "90005", "neighborhood": "Koreatown", "businesses": 290},
        {"zip": "90006", "neighborhood": "Koreatown", "businesses": 280},
        {"zip": "90019", "neighborhood": "Mid-City", "businesses": 260},
        {"zip": "90020", "neighborhood": "Koreatown", "businesses": 275},
        {"zip": "90036", "neighborhood": "Miracle Mile", "businesses": 285},
        
        # Silver Lake & Los Feliz
        {"zip": "90026", "neighborhood": "Silver Lake", "businesses": 250},
        {"zip": "90027", "neighborhood": "Los Feliz", "businesses": 260},
        {"zip": "90029", "neighborhood": "Los Feliz", "businesses": 245},
        {"zip": "90039", "neighborhood": "Silver Lake", "businesses": 240},
        
        # Studio City & Sherman Oaks
        {"zip": "91604", "neighborhood": "Studio City", "businesses": 265},
        {"zip": "91403", "neighborhood": "Sherman Oaks", "businesses": 255},
        {"zip": "91423", "neighborhood": "Sherman Oaks", "businesses": 250},
        
        # Pasadena Area
        {"zip": "91101", "neighborhood": "Pasadena", "businesses": 280},
        {"zip": "91103", "neighborhood": "Pasadena", "businesses": 260},
        {"zip": "91105", "neighborhood": "Pasadena", "businesses": 250},
    ],
    
    "medium": [
        # South LA
        {"zip": "90001", "neighborhood": "Florence", "businesses": 220},
        {"zip": "90002", "neighborhood": "Watts", "businesses": 210},
        {"zip": "90003", "neighborhood": "Southeast LA", "businesses": 215},
        {"zip": "90007", "neighborhood": "South LA", "businesses": 225},
        {"zip": "90008", "neighborhood": "Baldwin Hills", "businesses": 230},
        {"zip": "90016", "neighborhood": "West LA", "businesses": 235},
        {"zip": "90018", "neighborhood": "Jefferson Park", "businesses": 220},
        {"zip": "90037", "neighborhood": "South LA", "businesses": 205},
        {"zip": "90043", "neighborhood": "Hyde Park", "businesses": 200},
        {"zip": "90044", "neighborhood": "Athens", "businesses": 195},
        {"zip": "90047", "neighborhood": "South LA", "businesses": 190},
        {"zip": "90062", "neighborhood": "South LA", "businesses": 185},
        
        # East LA
        {"zip": "90022", "neighborhood": "East LA", "businesses": 210},
        {"zip": "90023", "neighborhood": "East LA", "businesses": 200},
        {"zip": "90031", "neighborhood": "Lincoln Heights", "businesses": 195},
        {"zip": "90032", "neighborhood": "El Sereno", "businesses": 190},
        {"zip": "90033", "neighborhood": "Boyle Heights", "businesses": 205},
        {"zip": "90063", "neighborhood": "East LA", "businesses": 185},
        
        # North Hollywood & Van Nuys
        {"zip": "91601", "neighborhood": "North Hollywood", "businesses": 230},
        {"zip": "91602", "neighborhood": "North Hollywood", "businesses": 220},
        {"zip": "91605", "neighborhood": "North Hollywood", "businesses": 210},
        {"zip": "91401", "neighborhood": "Van Nuys", "businesses": 225},
        {"zip": "91405", "neighborhood": "Van Nuys", "businesses": 215},
        {"zip": "91406", "neighborhood": "Van Nuys", "businesses": 210},
        
        # Burbank & Glendale
        {"zip": "91502", "neighborhood": "Burbank", "businesses": 240},
        {"zip": "91505", "neighborhood": "Burbank", "businesses": 230},
        {"zip": "91201", "neighborhood": "Glendale", "businesses": 235},
        {"zip": "91202", "neighborhood": "Glendale", "businesses": 225},
        {"zip": "91203", "neighborhood": "Glendale", "businesses": 220},
    ],
    
    "low": [
        # Residential areas with fewer businesses
        {"zip": "90041", "neighborhood": "Eagle Rock", "businesses": 180},
        {"zip": "90042", "neighborhood": "Highland Park", "businesses": 175},
        {"zip": "90045", "neighborhood": "Westchester", "businesses": 170},
        {"zip": "90056", "neighborhood": "Ladera Heights", "businesses": 160},
        {"zip": "90057", "neighborhood": "Westlake", "businesses": 165},
        {"zip": "90059", "neighborhood": "South LA", "businesses": 155},
        {"zip": "90061", "neighborhood": "South LA", "businesses": 150},
        {"zip": "90065", "neighborhood": "Mount Washington", "businesses": 145},
        {"zip": "90066", "neighborhood": "Mar Vista", "businesses": 185},
        {"zip": "90068", "neighborhood": "Hollywood Hills", "businesses": 140},
        
        # San Fernando Valley residential
        {"zip": "91302", "neighborhood": "Calabasas", "businesses": 175},
        {"zip": "91306", "neighborhood": "Winnetka", "businesses": 165},
        {"zip": "91311", "neighborhood": "Chatsworth", "businesses": 170},
        {"zip": "91316", "neighborhood": "Encino", "businesses": 180},
        {"zip": "91324", "neighborhood": "Northridge", "businesses": 160},
        {"zip": "91325", "neighborhood": "Northridge", "businesses": 155},
        {"zip": "91331", "neighborhood": "Pacoima", "businesses": 150},
        {"zip": "91335", "neighborhood": "Reseda", "businesses": 165},
        {"zip": "91342", "neighborhood": "Sylmar", "businesses": 145},
        {"zip": "91343", "neighborhood": "North Hills", "businesses": 155},
        {"zip": "91344", "neighborhood": "Granada Hills", "businesses": 160},
        {"zip": "91352", "neighborhood": "Sun Valley", "businesses": 140},
        {"zip": "91356", "neighborhood": "Tarzana", "businesses": 175},
        {"zip": "91364", "neighborhood": "Woodland Hills", "businesses": 180},
        {"zip": "91367", "neighborhood": "Woodland Hills", "businesses": 170},
        {"zip": "91402", "neighborhood": "Panorama City", "businesses": 150},
    ]
}

def get_zip_codes_by_profile(profile: str = "balanced") -> list:
    """
    Get ZIP codes based on coverage profile
    
    Args:
        profile: 'aggressive', 'balanced', or 'budget'
        
    Returns:
        List of ZIP code dictionaries with metadata
    """
    all_zips = []
    
    if profile == "aggressive":
        # Use all 102 ZIP codes
        for density_level in ["very_high", "high", "medium", "low"]:
            all_zips.extend(LA_ZIP_CODES[density_level])
    
    elif profile == "balanced":
        # Use top 80 ZIP codes (all very_high, all high, and top medium)
        all_zips.extend(LA_ZIP_CODES["very_high"])  # 20 ZIPs
        all_zips.extend(LA_ZIP_CODES["high"])       # 26 ZIPs
        all_zips.extend(LA_ZIP_CODES["medium"][:34]) # 34 ZIPs = 80 total
    
    elif profile == "budget":
        # Use top 50 ZIP codes (all very_high + partial high)
        all_zips.extend(LA_ZIP_CODES["very_high"])  # 20 ZIPs
        all_zips.extend(LA_ZIP_CODES["high"][:30])  # 30 ZIPs = 50 total
    
    else:
        raise ValueError(f"Invalid profile: {profile}. Must be 'aggressive', 'balanced', or 'budget'")
    
    return all_zips

def calculate_coverage_stats(profile: str) -> dict:
    """
    Calculate coverage statistics for a given profile
    
    Returns:
        Dictionary with coverage metrics
    """
    selected_zips = get_zip_codes_by_profile(profile)
    total_zips = sum(len(LA_ZIP_CODES[level]) for level in LA_ZIP_CODES)
    
    # Calculate expected businesses
    expected_businesses = sum(z["businesses"] for z in selected_zips)
    
    # Estimate costs (Apify: $7 per 1000 results)
    estimated_cost = (expected_businesses / 1000) * 7
    
    return {
        "profile": profile,
        "total_zips_selected": len(selected_zips),
        "total_zips_available": total_zips,
        "coverage_percentage": round((len(selected_zips) / total_zips) * 100, 1),
        "expected_businesses": expected_businesses,
        "estimated_cost_usd": round(estimated_cost, 2),
        "cost_per_business": round(estimated_cost / expected_businesses, 4) if expected_businesses > 0 else 0
    }

def get_zips_for_custom_selection(density_levels: list, limit: int = None) -> list:
    """
    Get ZIP codes for custom selection based on density levels
    
    Args:
        density_levels: List of density levels to include
        limit: Maximum number of ZIPs to return
        
    Returns:
        List of ZIP codes
    """
    selected = []
    for level in density_levels:
        if level in LA_ZIP_CODES:
            selected.extend(LA_ZIP_CODES[level])
    
    if limit:
        return selected[:limit]
    return selected

def get_neighborhood_zips(neighborhood_name: str) -> list:
    """
    Get all ZIP codes for a specific neighborhood
    
    Args:
        neighborhood_name: Name of the neighborhood
        
    Returns:
        List of ZIP codes in that neighborhood
    """
    zips = []
    for density_level in LA_ZIP_CODES:
        for zip_data in LA_ZIP_CODES[density_level]:
            if neighborhood_name.lower() in zip_data["neighborhood"].lower():
                zips.append(zip_data)
    return zips

# Export functions for SQL seed data
def generate_sql_insert_statements():
    """Generate SQL INSERT statements for seeding the database"""
    statements = []
    
    for density_level, zips in LA_ZIP_CODES.items():
        for zip_data in zips:
            sql = f"""
INSERT INTO gmaps_scraper.zip_codes (
    zip_code, city, state, neighborhood, density_level, expected_businesses
) VALUES (
    '{zip_data["zip"]}', 
    'Los Angeles', 
    'CA', 
    '{zip_data["neighborhood"]}', 
    '{density_level}', 
    {zip_data["businesses"]}
) ON CONFLICT (zip_code) DO UPDATE SET
    density_level = EXCLUDED.density_level,
    expected_businesses = EXCLUDED.expected_businesses,
    neighborhood = EXCLUDED.neighborhood;"""
            statements.append(sql)
    
    return "\n".join(statements)

if __name__ == "__main__":
    # Test the functions
    print("Coverage Profiles for Los Angeles:")
    print("-" * 50)
    
    for profile in ["budget", "balanced", "aggressive"]:
        stats = calculate_coverage_stats(profile)
        print(f"\n{profile.upper()} Profile:")
        print(f"  ZIP codes: {stats['total_zips_selected']} / {stats['total_zips_available']}")
        print(f"  Coverage: {stats['coverage_percentage']}%")
        print(f"  Expected businesses: {stats['expected_businesses']:,}")
        print(f"  Estimated cost: ${stats['estimated_cost_usd']}")
        print(f"  Cost per business: ${stats['cost_per_business']}")
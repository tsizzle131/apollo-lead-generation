"""
ZIP Code Optimizer - Intelligent spacing and overlap reduction

This module provides utilities for:
- Distance calculations between ZIP codes
- Adjacency detection
- Clustering overlapping ZIP codes
- Optimal spacing selection
"""

import logging
from typing import List, Dict, Optional, Set, Tuple
from uszipcode import SearchEngine
from geopy.distance import geodesic

logger = logging.getLogger(__name__)


class ZIPCodeOptimizer:
    """
    Optimizes ZIP code selection to minimize overlap while maintaining coverage.
    Uses uszipcode for offline ZIP data and geopy for precise distance calculations.
    """

    def __init__(self):
        """Initialize the optimizer with ZIP code database"""
        self.engine = SearchEngine()
        logger.info("ZIP Code Optimizer initialized")

    def get_zipcode_data(self, zipcode: str) -> Optional[Dict]:
        """
        Get comprehensive data for a ZIP code including coordinates and demographics.

        Args:
            zipcode: 5-digit ZIP code string

        Returns:
            Dictionary with ZIP data or None if not found
        """
        result = self.engine.by_zipcode(zipcode)
        # Handle both .lng and .long for compatibility
        lng = getattr(result, 'lng', None) or getattr(result, 'long', None)
        if not result or not result.lat or not lng:
            logger.warning(f"ZIP code {zipcode} not found or missing coordinates")
            return None

        return {
            'zipcode': result.zipcode,
            'city': result.major_city,
            'state': result.state,
            'lat': result.lat,
            'lng': lng,
            'population': result.population or 0,
            'population_density': result.population_density or 0,
            'land_area_sqmi': result.land_area_in_sqmi or 0,
            'median_household_income': result.median_household_income or 0
        }

    def calculate_distance(self, zip1: str, zip2: str) -> Optional[float]:
        """
        Calculate precise distance in miles between two ZIP codes.

        Args:
            zip1: First ZIP code
            zip2: Second ZIP code

        Returns:
            Distance in miles or None if either ZIP not found
        """
        data1 = self.get_zipcode_data(zip1)
        data2 = self.get_zipcode_data(zip2)

        if not data1 or not data2:
            return None

        distance = geodesic(
            (data1['lat'], data1['lng']),
            (data2['lat'], data2['lng'])
        ).miles

        return round(distance, 2)

    def find_adjacent_zipcodes(
        self,
        center_zip: str,
        radius_miles: float = 2.0
    ) -> List[Dict]:
        """
        Find ZIP codes adjacent to center ZIP (within radius).

        Args:
            center_zip: Center ZIP code
            radius_miles: Search radius in miles (default 2 miles)

        Returns:
            List of dicts with zipcode, city, distance, and population
        """
        center = self.engine.by_zipcode(center_zip)
        center_lng = getattr(center, 'lng', None) or getattr(center, 'long', None)
        if not center or not center.lat or not center_lng:
            logger.warning(f"Center ZIP {center_zip} not found")
            return []

        # Search in radius using uszipcode
        nearby = self.engine.by_coordinates(
            center.lat,
            center_lng,
            radius=radius_miles,
            returns=None  # Return all results
        )

        # Calculate exact distances and exclude center
        adjacent = []
        for z in nearby:
            z_lng = getattr(z, 'lng', None) or getattr(z, 'long', None)
            if z.zipcode != center_zip and z.lat and z_lng:
                distance = geodesic(
                    (center.lat, center_lng),
                    (z.lat, z_lng)
                ).miles

                adjacent.append({
                    'zipcode': z.zipcode,
                    'city': z.major_city,
                    'state': z.state,
                    'distance_miles': round(distance, 2),
                    'population': z.population or 0,
                    'population_density': z.population_density or 0
                })

        # Sort by distance
        return sorted(adjacent, key=lambda x: x['distance_miles'])

    def are_zipcodes_adjacent(
        self,
        zip1: str,
        zip2: str,
        threshold_miles: float = 3.0
    ) -> bool:
        """
        Check if two ZIP codes are adjacent (within threshold distance).

        Args:
            zip1: First ZIP code
            zip2: Second ZIP code
            threshold_miles: Distance threshold for adjacency (default 3 miles)

        Returns:
            True if ZIP codes are adjacent, False otherwise
        """
        distance = self.calculate_distance(zip1, zip2)
        if distance is None:
            return False

        return distance <= threshold_miles

    def cluster_zipcodes(
        self,
        zipcodes: List[str],
        max_distance_miles: float = 5.0
    ) -> Dict[int, List[str]]:
        """
        Group ZIP codes into clusters based on geographic proximity.
        Uses simple distance-based clustering.

        Args:
            zipcodes: List of ZIP codes to cluster
            max_distance_miles: Maximum distance for cluster membership

        Returns:
            Dictionary mapping cluster_id to list of ZIP codes
        """
        if not zipcodes:
            return {}

        # Get coordinates for all ZIP codes
        zip_data = {}
        for zipcode in zipcodes:
            data = self.get_zipcode_data(zipcode)
            if data:
                zip_data[zipcode] = (data['lat'], data['lng'])

        if not zip_data:
            logger.warning("No valid ZIP codes found for clustering")
            return {}

        # Simple clustering: assign each ZIP to first cluster within distance
        clusters = {}
        cluster_centroids = {}
        next_cluster_id = 0

        for zipcode, coords in zip_data.items():
            # Check if ZIP fits in existing cluster
            assigned = False
            for cluster_id, centroid in cluster_centroids.items():
                distance = geodesic(coords, centroid).miles
                if distance <= max_distance_miles:
                    clusters[cluster_id].append(zipcode)
                    # Update centroid (moving average)
                    cluster_zips = clusters[cluster_id]
                    avg_lat = sum(zip_data[z][0] for z in cluster_zips) / len(cluster_zips)
                    avg_lng = sum(zip_data[z][1] for z in cluster_zips) / len(cluster_zips)
                    cluster_centroids[cluster_id] = (avg_lat, avg_lng)
                    assigned = True
                    break

            # Create new cluster if not assigned
            if not assigned:
                clusters[next_cluster_id] = [zipcode]
                cluster_centroids[next_cluster_id] = coords
                next_cluster_id += 1

        logger.info(f"Clustered {len(zipcodes)} ZIP codes into {len(clusters)} clusters")
        return clusters

    def select_optimal_spacing(
        self,
        candidate_zips: List[Dict],
        min_distance_miles: float = 3.0,
        max_zips: Optional[int] = None
    ) -> List[Dict]:
        """
        Select ZIP codes with optimal spacing to minimize overlap.
        Uses greedy algorithm: always pick highest-scoring ZIP that maintains minimum distance.

        Args:
            candidate_zips: List of dicts with 'zipcode' and 'combined_score'
            min_distance_miles: Minimum distance between selected ZIPs
            max_zips: Maximum number of ZIPs to select (None for unlimited)

        Returns:
            List of selected ZIP dicts with optimal spacing
        """
        if not candidate_zips:
            return []

        # Sort by score descending
        sorted_candidates = sorted(
            candidate_zips,
            key=lambda x: x.get('combined_score', 0),
            reverse=True
        )

        selected = []
        selected_coords = []

        for candidate in sorted_candidates:
            zipcode = candidate.get('zip') or candidate.get('zipcode')
            if not zipcode:
                continue

            # Get coordinates
            data = self.get_zipcode_data(zipcode)
            if not data:
                continue

            coords = (data['lat'], data['lng'])

            # Check distance to all selected ZIPs
            too_close = False
            for selected_coord in selected_coords:
                distance = geodesic(coords, selected_coord).miles
                if distance < min_distance_miles:
                    too_close = True
                    logger.debug(
                        f"Skipping {zipcode} - too close to existing ZIP "
                        f"({distance:.2f} miles < {min_distance_miles} miles)"
                    )
                    break

            # Add if not too close
            if not too_close:
                selected.append(candidate)
                selected_coords.append(coords)
                logger.debug(
                    f"Selected {zipcode} (score: {candidate.get('combined_score', 0):.2f})"
                )

                # Check max limit
                if max_zips and len(selected) >= max_zips:
                    logger.info(f"Reached max ZIP limit of {max_zips}")
                    break

        logger.info(
            f"Selected {len(selected)} ZIPs from {len(candidate_zips)} candidates "
            f"with {min_distance_miles} mile spacing"
        )

        return selected

    def calculate_coverage_metrics(
        self,
        selected_zips: List[str],
        search_radius_miles: float = 5.0
    ) -> Dict:
        """
        Calculate coverage metrics for selected ZIP codes.

        Args:
            selected_zips: List of selected ZIP codes
            search_radius_miles: Expected search radius per ZIP

        Returns:
            Dictionary with coverage metrics
        """
        if not selected_zips:
            return {
                'total_area_sqmi': 0,
                'estimated_coverage_sqmi': 0,
                'overlap_ratio': 0,
                'avg_distance_between_zips': 0
            }

        # Calculate total land area
        total_area = 0
        for zipcode in selected_zips:
            data = self.get_zipcode_data(zipcode)
            if data:
                total_area += data.get('land_area_sqmi', 0)

        # Estimate coverage with circular search areas
        coverage_per_zip = 3.14159 * (search_radius_miles ** 2)
        estimated_coverage = len(selected_zips) * coverage_per_zip

        # Calculate average distance between ZIPs
        distances = []
        for i, zip1 in enumerate(selected_zips):
            for zip2 in selected_zips[i+1:]:
                dist = self.calculate_distance(zip1, zip2)
                if dist:
                    distances.append(dist)

        avg_distance = sum(distances) / len(distances) if distances else 0

        # Estimate overlap ratio (coverage / actual area)
        overlap_ratio = estimated_coverage / total_area if total_area > 0 else 1.0

        return {
            'total_area_sqmi': round(total_area, 2),
            'estimated_coverage_sqmi': round(estimated_coverage, 2),
            'overlap_ratio': round(overlap_ratio, 2),
            'avg_distance_between_zips': round(avg_distance, 2),
            'num_zips': len(selected_zips)
        }

    def get_optimal_spacing_for_density(
        self,
        population_density: float
    ) -> float:
        """
        Determine optimal ZIP spacing based on population density.

        Args:
            population_density: Population per square mile

        Returns:
            Recommended minimum spacing in miles
        """
        # Urban: high density → tighter spacing
        # Suburban: medium density → moderate spacing
        # Rural: low density → wider spacing

        if population_density >= 10000:  # Dense urban (Manhattan, downtown LA)
            return 2.0
        elif population_density >= 5000:  # Urban (most cities)
            return 3.0
        elif population_density >= 2000:  # Suburban
            return 5.0
        elif population_density >= 500:  # Rural
            return 8.0
        else:  # Very rural
            return 10.0

    def recommend_coverage_profile(
        self,
        location: str,
        business_type: str
    ) -> Dict:
        """
        Recommend coverage profile settings based on location and business type.

        Args:
            location: Location name (city, state, or ZIP)
            business_type: Type of business (e.g., "dentists", "restaurants")

        Returns:
            Dictionary with recommended settings
        """
        # Try to geocode location
        center = self.engine.by_city_and_state(location, None)
        if not center and len(location) == 5:
            center = self.engine.by_zipcode(location)

        if not center or not center.population_density:
            # Default for unknown location
            return {
                'min_distance_miles': 5.0,
                'max_zips': 25,
                'search_radius_miles': 5.0,
                'coverage_percentage': 0.94
            }

        # Get optimal spacing
        spacing = self.get_optimal_spacing_for_density(center.population_density)

        # Adjust based on business type (high-volume businesses need more coverage)
        high_volume_types = ['restaurants', 'cafes', 'retail', 'stores']
        is_high_volume = any(t in business_type.lower() for t in high_volume_types)

        if is_high_volume:
            spacing *= 0.8  # Tighter spacing for high-volume businesses
            max_zips = 40
            coverage = 0.97
        else:
            max_zips = 25
            coverage = 0.94

        return {
            'min_distance_miles': round(spacing, 1),
            'max_zips': max_zips,
            'search_radius_miles': round(spacing * 1.5, 1),
            'coverage_percentage': coverage,
            'population_density': center.population_density
        }

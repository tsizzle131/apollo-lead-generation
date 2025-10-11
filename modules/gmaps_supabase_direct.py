"""
Direct SQL approach for Google Maps Supabase operations
Uses RPC functions and direct SQL execution since gmaps_scraper schema
is not directly exposed via PostgREST
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import uuid

class GmapsSupabaseDirect:
    """Direct SQL operations for gmaps_scraper schema"""
    
    def __init__(self, supabase_client):
        """Initialize with existing Supabase client"""
        self.client = supabase_client
        self.schema = "gmaps_scraper"
        
    def create_campaign(self, campaign_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create campaign using RPC function"""
        try:
            # Prepare data for RPC call
            rpc_data = {
                "name": campaign_data.get("name"),
                "description": campaign_data.get("description"),
                "keywords": campaign_data.get("keywords", []),
                "location": campaign_data.get("location"),
                "coverage_profile": campaign_data.get("coverage_profile", "balanced"),
                "status": campaign_data.get("status", "draft"),
                "target_zip_count": campaign_data.get("target_zip_count", 0),
                "coverage_percentage": campaign_data.get("coverage_percentage", 0),
                "estimated_cost": campaign_data.get("estimated_cost", 0),
                "organization_id": campaign_data.get("organization_id")
            }
            
            # Call RPC function
            result = self.client.rpc("gmaps_create_campaign", {"p_data": rpc_data}).execute()
            
            if result.data:
                logging.info(f"✅ Created campaign via RPC")
                return result.data
            return {}
            
        except Exception as e:
            logging.error(f"Error creating campaign: {e}")
            return {}
    
    def get_campaigns(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get campaigns using RPC function"""
        try:
            result = self.client.rpc("gmaps_get_campaigns", {"p_limit": limit}).execute()
            return result.data or []
        except Exception as e:
            logging.error(f"Error fetching campaigns: {e}")
            return []
    
    def execute_sql(self, query: str, params: Dict = None) -> Any:
        """Execute raw SQL for operations not covered by RPC"""
        try:
            # This would need server-side access or edge functions
            # For now, log the query that would be executed
            logging.info(f"Would execute SQL: {query[:100]}...")
            return None
        except Exception as e:
            logging.error(f"Error executing SQL: {e}")
            return None
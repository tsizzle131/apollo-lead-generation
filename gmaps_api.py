"""
Google Maps Campaign API endpoints
Provides REST API for frontend to manage Google Maps campaigns
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from modules.gmaps_campaign_manager import GmapsCampaignManager
from modules.gmaps_supabase_manager import GmapsSupabaseManager
from config import APIFY_API_KEY, OPENAI_API_KEY
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://ndrqixjdddcozjlevieo.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5kcnFpeGpkZGRjb3pqbGV2aWVvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA0NDk1MTcsImV4cCI6MjA2NjAyNTUxN30.XL1CmTW230m7QoubRhfsc8KmtKHYXEPGYdFpIlULTec")

# Initialize managers
manager = GmapsCampaignManager(
    supabase_url=SUPABASE_URL,
    supabase_key=SUPABASE_KEY,
    apify_key=APIFY_API_KEY,
    openai_key=OPENAI_API_KEY
)

db = GmapsSupabaseManager(SUPABASE_URL, SUPABASE_KEY)

@app.route('/api/gmaps/campaigns', methods=['GET'])
def get_campaigns():
    """Get all Google Maps campaigns"""
    try:
        # Get campaigns from database
        result = db.client.table("gmaps_campaigns").select("*").order("created_at", desc=True).execute()
        
        campaigns = []
        for campaign in result.data:
            campaigns.append({
                "id": campaign["id"],
                "name": campaign["name"],
                "location": campaign.get("location", ""),
                "keywords": campaign.get("keywords", []),
                "status": campaign.get("status", "draft"),
                "coverage_profile": campaign.get("coverage_profile", "balanced"),
                "target_zip_count": campaign.get("target_zip_count", 0),
                "estimated_cost": campaign.get("estimated_cost", 0),
                "total_businesses_found": campaign.get("total_businesses_found", 0),
                "total_emails_found": campaign.get("total_emails_found", 0),
                "total_facebook_pages_found": campaign.get("total_facebook_pages_found", 0),
                "actual_cost": campaign.get("actual_cost", 0),
                "created_at": campaign.get("created_at", ""),
                "completed_at": campaign.get("completed_at", "")
            })
        
        return jsonify({"campaigns": campaigns}), 200
    
    except Exception as e:
        logging.error(f"Error fetching campaigns: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/gmaps/campaigns/create', methods=['POST'])
def create_campaign():
    """Create a new Google Maps campaign"""
    try:
        data = request.json
        
        # Validate required fields
        if not data.get("name"):
            return jsonify({"error": "Campaign name is required"}), 400
        if not data.get("location"):
            return jsonify({"error": "Location is required"}), 400
        if not data.get("keywords"):
            return jsonify({"error": "Keywords are required"}), 400
        
        # Create campaign using manager (includes AI ZIP code selection)
        result = manager.create_campaign(
            name=data["name"],
            location=data["location"],
            keywords=data["keywords"],
            coverage_profile=data.get("coverage_profile", "balanced"),
            description=data.get("description")
        )
        
        if "error" in result:
            return jsonify({"error": result["error"]}), 400
        
        return jsonify(result), 201
    
    except Exception as e:
        logging.error(f"Error creating campaign: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/gmaps/campaigns/<campaign_id>/execute', methods=['POST'])
def execute_campaign(campaign_id):
    """Execute a Google Maps campaign"""
    try:
        data = request.json
        max_businesses = data.get("max_businesses_per_zip", 50)
        
        # Execute campaign in background (consider using Celery for production)
        # For now, we'll execute synchronously with a reasonable limit
        result = manager.execute_campaign(campaign_id, max_businesses_per_zip=max_businesses)
        
        if "error" in result:
            return jsonify({"error": result["error"]}), 400
        
        return jsonify(result), 200
    
    except Exception as e:
        logging.error(f"Error executing campaign: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/gmaps/campaigns/<campaign_id>', methods=['GET'])
def get_campaign_details(campaign_id):
    """Get detailed information about a specific campaign"""
    try:
        # Get campaign details
        campaign = db.get_campaign(campaign_id)
        if not campaign:
            return jsonify({"error": "Campaign not found"}), 404
        
        # Get campaign analytics
        analytics = db.get_campaign_analytics(campaign_id)
        
        # Get businesses for this campaign
        businesses_result = db.client.table("gmaps_businesses").select("*").eq("campaign_id", campaign_id).limit(100).execute()
        
        return jsonify({
            "campaign": campaign,
            "analytics": analytics,
            "businesses": businesses_result.data[:20],  # Return first 20 for UI
            "total_businesses": len(businesses_result.data)
        }), 200
    
    except Exception as e:
        logging.error(f"Error fetching campaign details: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/gmaps/campaigns/<campaign_id>/pause', methods=['POST'])
def pause_campaign(campaign_id):
    """Pause a running campaign"""
    try:
        success = manager.pause_campaign(campaign_id)
        if success:
            return jsonify({"message": "Campaign paused"}), 200
        else:
            return jsonify({"error": "Failed to pause campaign"}), 400
    
    except Exception as e:
        logging.error(f"Error pausing campaign: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/gmaps/campaigns/<campaign_id>/resume', methods=['POST'])
def resume_campaign(campaign_id):
    """Resume a paused campaign"""
    try:
        success = manager.resume_campaign(campaign_id)
        if success:
            return jsonify({"message": "Campaign resumed"}), 200
        else:
            return jsonify({"error": "Failed to resume campaign"}), 400
    
    except Exception as e:
        logging.error(f"Error resuming campaign: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/gmaps/campaigns/<campaign_id>/businesses', methods=['GET'])
def get_campaign_businesses(campaign_id):
    """Get businesses scraped in a campaign"""
    try:
        limit = request.args.get("limit", 100, type=int)
        offset = request.args.get("offset", 0, type=int)
        
        # Get businesses with pagination
        result = db.client.table("gmaps_businesses").select("*").eq("campaign_id", campaign_id).range(offset, offset + limit - 1).execute()
        
        return jsonify({
            "businesses": result.data,
            "total": len(result.data)
        }), 200
    
    except Exception as e:
        logging.error(f"Error fetching businesses: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/gmaps/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "Google Maps Campaign API"}), 200

if __name__ == '__main__':
    # Run the Flask app
    app.run(debug=True, port=5001)  # Using port 5001 to avoid conflict with main API
"""
Google Maps Supabase Manager
Handles all database operations for the Google Maps scraper
Uses public schema with gmaps_ prefixed tables for isolation
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from .supabase_manager import SupabaseManager

class GmapsSupabaseManager(SupabaseManager):
    """Extended Supabase manager for Google Maps scraper operations"""
    
    def __init__(self, supabase_url: str = None, supabase_key: str = None, organization_id: str = None):
        """Initialize with gmaps_ prefixed tables in public schema"""
        super().__init__(supabase_url, supabase_key, organization_id)
        logging.info("✅ GmapsSupabaseManager initialized with gmaps_ prefixed tables")
    
    # Campaign Management
    def create_campaign(self, campaign_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new Google Maps scraping campaign"""
        try:
            # Add timestamps and defaults
            campaign_data["created_at"] = datetime.now().isoformat()
            campaign_data["updated_at"] = datetime.now().isoformat()
            campaign_data["status"] = campaign_data.get("status", "draft")
            campaign_data["total_businesses_found"] = 0
            campaign_data["total_emails_found"] = 0
            
            result = self.client.table("gmaps_campaigns").insert(campaign_data).execute()
            
            if result.data:
                logging.info(f"✅ Created campaign: {result.data[0]['name']} (ID: {result.data[0]['id']})")
                return result.data[0]
            return {}
            
        except Exception as e:
            logging.error(f"Error creating campaign: {e}")
            return {}
    
    def update_campaign(self, campaign_id: str, updates: Dict[str, Any]) -> bool:
        """Update campaign data"""
        try:
            updates["updated_at"] = datetime.now().isoformat()
            
            result = self.client.table("gmaps_campaigns").update(updates).eq("id", campaign_id).execute()
            return len(result.data) > 0
            
        except Exception as e:
            logging.error(f"Error updating campaign: {e}")
            return False
    
    def get_campaign(self, campaign_id: str) -> Dict[str, Any]:
        """Get campaign by ID"""
        try:
            result = self.client.table("gmaps_campaigns").select("*").eq("id", campaign_id).execute()
            return result.data[0] if result.data else {}
            
        except Exception as e:
            logging.error(f"Error fetching campaign: {e}")
            return {}
    
    # ZIP Code Management
    def get_zip_codes(self, zip_list: List[str] = None, density_level: str = None) -> List[Dict[str, Any]]:
        """Get ZIP codes from database"""
        try:
            query = self.client.table("gmaps_zip_codes").select("*")
            
            if zip_list:
                query = query.in_("zip_code", zip_list)
            
            if density_level:
                query = query.eq("density_level", density_level)
            
            result = query.execute()
            return result.data or []
            
        except Exception as e:
            logging.error(f"Error fetching ZIP codes: {e}")
            return []
    
    def update_zip_code_stats(self, zip_code: str, businesses_found: int) -> bool:
        """Update ZIP code statistics after scraping"""
        try:
            updates = {
                "actual_businesses": businesses_found,
                "last_scraped_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            
            result = self.client.table("gmaps_zip_codes").update(updates).eq("zip_code", zip_code).execute()
            return len(result.data) > 0
            
        except Exception as e:
            logging.error(f"Error updating ZIP code stats: {e}")
            return False
    
    # Campaign Coverage Management
    def add_campaign_coverage(self, campaign_id: str, zip_codes: List[Dict[str, Any]]) -> int:
        """Add ZIP codes to campaign coverage"""
        try:
            coverage_records = []
            for zip_data in zip_codes:
                record = {
                    "campaign_id": campaign_id,
                    "zip_code": zip_data.get("zip", zip_data.get("zip_code")),
                    "keywords": zip_data.get("keywords", []),
                    "max_results": zip_data.get("max_results", 250),
                    "estimated_cost": zip_data.get("estimated_cost", 0),
                    "created_at": datetime.now().isoformat()
                }
                coverage_records.append(record)
            
            # Insert in batches
            batch_size = 50
            total_inserted = 0
            
            for i in range(0, len(coverage_records), batch_size):
                batch = coverage_records[i:i + batch_size]
                result = self.client.table("gmaps_campaign_coverage").insert(batch).execute()
                if result.data:
                    total_inserted += len(result.data)
            
            logging.info(f"✅ Added {total_inserted} ZIP codes to campaign coverage")
            return total_inserted
            
        except Exception as e:
            logging.error(f"Error adding campaign coverage: {e}")
            return 0
    
    def get_campaign_coverage(self, campaign_id: str, scraped: bool = None) -> List[Dict[str, Any]]:
        """Get ZIP codes for a campaign"""
        try:
            query = self.client.table("gmaps_campaign_coverage").select("*").eq("campaign_id", campaign_id)
            
            if scraped is not None:
                query = query.eq("scraped", scraped)
            
            result = query.execute()
            return result.data or []
            
        except Exception as e:
            logging.error(f"Error fetching campaign coverage: {e}")
            return []
    
    def update_coverage_status(self, campaign_id: str, zip_code: str, 
                              businesses_found: int, emails_found: int, actual_cost: float) -> bool:
        """Update coverage status after scraping a ZIP code"""
        try:
            updates = {
                "scraped": True,
                "scraped_at": datetime.now().isoformat(),
                "businesses_found": businesses_found,
                "emails_found": emails_found,
                "actual_cost": actual_cost,
                "updated_at": datetime.now().isoformat()
            }
            
            result = (self.client.table("gmaps_campaign_coverage")
                     .update(updates)
                     .eq("campaign_id", campaign_id)
                     .eq("zip_code", zip_code)
                     .execute())
            
            return len(result.data) > 0
            
        except Exception as e:
            logging.error(f"Error updating coverage status: {e}")
            return False
    
    # Business Management
    def _extract_facebook_url(self, business: Dict[str, Any]) -> Optional[str]:
        """Extract Facebook URL from various possible fields"""
        # Check 'facebooks' field (plural) - this is what Google Maps returns
        facebooks = business.get("facebooks", [])
        if facebooks and isinstance(facebooks, list) and len(facebooks) > 0:
            return facebooks[0]  # Return the first Facebook URL
        
        # Direct Facebook fields (singular)
        fb_url = business.get("facebookUrl") or business.get("facebook")
        if fb_url:
            return fb_url
        
        # Check if website is actually a Facebook URL
        website = business.get("website") or business.get("url", "")
        if "facebook.com" in website.lower():
            return website
        
        # Check websiteDetails if available
        website_details = business.get("websiteDetails", {})
        if isinstance(website_details, dict):
            # Check for social links in website details
            social_links = website_details.get("socialLinks", [])
            if isinstance(social_links, list):
                for link in social_links:
                    if "facebook.com" in str(link).lower():
                        return link
            
            # Check for Facebook in any field of website details
            for key, value in website_details.items():
                if isinstance(value, str) and "facebook.com" in value.lower():
                    return value
        
        return None
    
    def save_businesses(self, businesses: List[Dict[str, Any]], campaign_id: str, zip_code: str) -> int:
        """Save Google Maps businesses to database"""
        try:
            if not businesses:
                return 0
            
            # Prepare business records
            business_records = []
            for business in businesses:
                # Extract email from various possible fields
                email = None
                if business.get("emails") and len(business.get("emails", [])) > 0:
                    email = business.get("emails")[0]
                elif business.get("email"):
                    email = business.get("email")

                # Set email_source based on whether email was found from Google Maps
                email_source = "google_maps" if email else "not_found"

                record = {
                    "campaign_id": campaign_id,
                    "zip_code": zip_code,
                    "place_id": business.get("placeId") or business.get("place_id"),
                    "name": business.get("title") or business.get("name"),
                    "address": business.get("address") or business.get("fullAddress"),
                    "city": business.get("city"),
                    "state": business.get("state"),
                    "postal_code": business.get("postalCode") or business.get("zip"),
                    "latitude": business.get("latitude") or business.get("lat"),
                    "longitude": business.get("longitude") or business.get("lng"),
                    "phone": business.get("phone") or business.get("phoneNumber"),
                    "website": business.get("website") or business.get("url"),
                    "email": email,
                    "email_source": email_source,
                    "category": business.get("category") or business.get("categoryName"),
                    "categories": business.get("categories", []),
                    "description": business.get("description"),
                    "rating": business.get("totalScore") or business.get("rating"),
                    "reviews_count": business.get("reviewsCount") or business.get("totalReviews"),
                    "price_level": business.get("price"),
                    "hours": business.get("openingHours") or business.get("hours"),
                    "facebook_url": self._extract_facebook_url(business),
                    "instagram_url": business.get("instagrams", [None])[0] if business.get("instagrams") else business.get("instagramUrl") or business.get("instagram"),
                    "twitter_url": business.get("twitterUrl") or business.get("twitter"),
                    "linkedin_url": business.get("linkedinUrl") or business.get("linkedin"),
                    "needs_enrichment": bool(self._extract_facebook_url(business)),
                    "enrichment_status": "pending" if self._extract_facebook_url(business) else "no_facebook",
                    "raw_data": business,
                    "scraped_at": datetime.now().isoformat()
                }
                business_records.append(record)
            
            # Insert in batches with upsert to handle duplicates
            batch_size = 50
            total_saved = 0
            
            for i in range(0, len(business_records), batch_size):
                batch = business_records[i:i + batch_size]
                result = self.client.table("gmaps_businesses").upsert(
                    batch, 
                    on_conflict="place_id"
                ).execute()
                
                if result.data:
                    total_saved += len(result.data)
            
            logging.info(f"✅ Saved {total_saved} businesses for ZIP {zip_code}")
            return total_saved
            
        except Exception as e:
            logging.error(f"Error saving businesses: {e}")
            return 0
    
    def get_businesses_for_enrichment(self, campaign_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get businesses that need Facebook enrichment"""
        try:
            result = (self.client.table("gmaps_businesses")
                     .select("*")
                     .eq("campaign_id", campaign_id)
                     .eq("needs_enrichment", True)
                     .eq("enrichment_status", "pending")
                     .limit(limit)
                     .execute())
            
            return result.data or []
            
        except Exception as e:
            logging.error(f"Error fetching businesses for enrichment: {e}")
            return []
    
    def save_facebook_enrichment(self, business_id: str, campaign_id: str, 
                                 enrichment_data: Dict[str, Any]) -> bool:
        """Save Facebook enrichment results"""
        try:
            record = {
                "business_id": business_id,
                "campaign_id": campaign_id,
                "facebook_url": enrichment_data.get("facebook_url"),
                "page_name": enrichment_data.get("page_name"),
                "emails": enrichment_data.get("emails", []),
                "primary_email": enrichment_data.get("primary_email"),
                "email_sources": enrichment_data.get("email_sources", []),
                "phone_numbers": enrichment_data.get("phone_numbers", []),
                "success": enrichment_data.get("success", False),
                "error_message": enrichment_data.get("error_message"),
                "raw_data": enrichment_data.get("raw_data"),
                "scraped_at": datetime.now().isoformat()
            }
            
            result = self.client.table("gmaps_facebook_enrichments").insert(record).execute()
            
            if result.data:
                # Update business enrichment status AND email_source
                update_data = {
                    "enrichment_status": "enriched" if enrichment_data.get("success") else "failed",
                    "enrichment_attempts": 1,
                    "last_enrichment_attempt": datetime.now().isoformat()
                }

                # If Facebook enrichment found an email, update email and email_source
                if enrichment_data.get("primary_email"):
                    update_data["email"] = enrichment_data.get("primary_email")
                    update_data["email_source"] = "facebook"

                self.client.table("gmaps_businesses").update(update_data).eq("id", business_id).execute()

                return True
            return False
            
        except Exception as e:
            logging.error(f"Error saving Facebook enrichment: {e}")
            return False
    
    # LinkedIn Enrichment Methods
    def get_all_businesses(self, campaign_id: str, limit: int = None) -> List[Dict[str, Any]]:
        """Get all businesses for a campaign for LinkedIn enrichment"""
        try:
            query = (self.client.table("gmaps_businesses")
                    .select("*")
                    .eq("campaign_id", campaign_id)
                    .eq("linkedin_enriched", False))  # Only get non-LinkedIn enriched

            if limit:
                query = query.limit(limit)

            result = query.execute()
            return result.data or []

        except Exception as e:
            logging.error(f"Error getting businesses for LinkedIn enrichment: {e}")
            return []

    def save_linkedin_enrichment(self, business_id: str, campaign_id: str,
                                enrichment_data: Dict[str, Any]) -> bool:
        """Save LinkedIn enrichment results to database with email quality tracking"""
        try:
            # emails_generated is now TEXT[] in database - save the actual array of email patterns
            record = {
                "business_id": business_id,
                "campaign_id": campaign_id,
                "linkedin_url": enrichment_data.get("linkedin_url"),
                "profile_type": enrichment_data.get("profile_type"),
                "person_name": enrichment_data.get("person_name"),
                "person_title": enrichment_data.get("person_title"),
                "person_profile_url": enrichment_data.get("person_profile_url"),
                "company_name": enrichment_data.get("company"),
                "location": enrichment_data.get("location"),
                "connections": enrichment_data.get("connections"),
                "emails_found": enrichment_data.get("emails_found", []),
                "emails_generated": enrichment_data.get("emails_generated", []),  # Save array directly
                "primary_email": enrichment_data.get("primary_email"),
                "email_source": enrichment_data.get("email_source"),
                "phone_numbers": enrichment_data.get("phone_numbers", []),
                "error_message": enrichment_data.get("error"),

                # Email quality tracking fields
                "email_extraction_attempted": enrichment_data.get("email_extraction_attempted", False),
                "email_verified_source": enrichment_data.get("email_verified_source"),
                "phone_number": enrichment_data.get("phone_number"),
                "email_quality_tier": enrichment_data.get("email_quality_tier"),

                # Bouncer verification fields
                "bouncer_status": enrichment_data.get("bouncer_status"),
                "bouncer_score": enrichment_data.get("bouncer_score"),
                "bouncer_reason": enrichment_data.get("bouncer_reason"),
                "bouncer_verified_at": enrichment_data.get("bouncer_verified_at"),
                "bouncer_raw_response": enrichment_data.get("bouncer_raw_response"),
                "email_verified": enrichment_data.get("bouncer_verified", False),
                "is_safe": enrichment_data.get("bouncer_is_safe", False),
                "is_disposable": enrichment_data.get("bouncer_is_disposable", False),
                "is_role_based": enrichment_data.get("bouncer_is_role_based", False),
                "is_free_email": enrichment_data.get("bouncer_is_free_email", False),

                "enriched_at": datetime.now().isoformat()
            }

            result = self.client.table("gmaps_linkedin_enrichments").insert(record).execute()

            if result.data:
                # Update business with LinkedIn URL (skip linkedin_enriched_at for now - column doesn't exist)
                update_data = {
                    "linkedin_url": enrichment_data.get("linkedin_url"),
                    "linkedin_enriched": True
                }

                # If LinkedIn enrichment found an email, update email and email_source
                # Priority: linkedin_verified > linkedin_generated
                if enrichment_data.get("primary_email"):
                    update_data["email"] = enrichment_data.get("primary_email")

                    # Use specific email_source based on quality tier
                    if enrichment_data.get("email_verified_source") == "linkedin_public":
                        update_data["email_source"] = "linkedin"  # Verified emails use "linkedin"
                    elif enrichment_data.get("email_verified_source") == "pattern_generated":
                        update_data["email_source"] = "linkedin"  # Generated emails also use "linkedin"
                    else:
                        update_data["email_source"] = "linkedin"  # Default to "linkedin"

                self.client.table("gmaps_businesses").update(update_data).eq("id", business_id).execute()

                return True
            return False

        except Exception as e:
            logging.error(f"Error saving LinkedIn enrichment: {e}")
            return False

    def update_linkedin_verification(self, business_id: str,
                                    verification_data: Dict[str, Any]) -> bool:
        """Update LinkedIn enrichment with email verification results"""
        try:
            # Get the LinkedIn enrichment record
            enrichment_result = (self.client.table("gmaps_linkedin_enrichments")
                               .select("id")
                               .eq("business_id", business_id)
                               .execute())

            if not enrichment_result.data:
                return False

            enrichment_id = enrichment_result.data[0]["id"]

            # Update with verification results
            update_data = {
                "email_verified": True,
                "bouncer_status": verification_data.get("status"),
                "bouncer_score": verification_data.get("score"),
                "bouncer_reason": verification_data.get("reason"),
                "is_safe": verification_data.get("is_safe", False),
                "is_disposable": verification_data.get("is_disposable", False),
                "is_role_based": verification_data.get("is_role_based", False),
                "is_free_email": verification_data.get("is_free_email", False),
                "bouncer_verified_at": datetime.now().isoformat(),
                "bouncer_raw_response": verification_data.get("raw_response")
            }

            result = (self.client.table("gmaps_linkedin_enrichments")
                     .update(update_data)
                     .eq("id", enrichment_id)
                     .execute())

            # Also save to email verifications table for logging
            verification_record = {
                "business_id": business_id,
                "linkedin_enrichment_id": enrichment_id,
                "email": verification_data.get("email"),
                "status": verification_data.get("status"),
                "score": verification_data.get("score"),
                "is_safe": verification_data.get("is_safe", False),
                "is_disposable": verification_data.get("is_disposable", False),
                "is_role_based": verification_data.get("is_role_based", False),
                "is_free_email": verification_data.get("is_free_email", False),
                "is_gibberish": verification_data.get("is_gibberish", False),
                "domain": verification_data.get("domain"),
                "provider": verification_data.get("provider"),
                "mx_records": verification_data.get("mx_records"),
                "smtp_check": verification_data.get("smtp_check"),
                "reason": verification_data.get("reason"),
                "suggestion": verification_data.get("suggestion"),
                "raw_response": verification_data.get("raw_response"),
                "verified_at": datetime.now().isoformat()
            }

            self.client.table("gmaps_email_verifications").insert(verification_record).execute()

            return len(result.data) > 0

        except Exception as e:
            logging.error(f"Error updating LinkedIn verification: {e}")
            return False

    def update_facebook_verification(self, business_id: str,
                                     verification_data: Dict[str, Any]) -> bool:
        """Update Facebook enrichment with email verification results"""
        try:
            # Get the Facebook enrichment record
            enrichment_result = (self.client.table("gmaps_facebook_enrichments")
                               .select("id")
                               .eq("business_id", business_id)
                               .execute())

            if not enrichment_result.data:
                logging.warning(f"No Facebook enrichment found for business {business_id}")
                return False

            enrichment_id = enrichment_result.data[0]["id"]

            # Update Facebook enrichment with verification results
            update_data = {
                "email_verified": True,
                "bouncer_status": verification_data.get("status"),
                "bouncer_score": verification_data.get("score"),
                "bouncer_reason": verification_data.get("reason"),
                "is_safe": verification_data.get("is_safe", False),
                "is_disposable": verification_data.get("is_disposable", False),
                "is_role_based": verification_data.get("is_role_based", False),
                "is_free_email": verification_data.get("is_free_email", False),
                "bouncer_verified_at": datetime.now().isoformat(),
                "bouncer_raw_response": verification_data.get("raw_response")
            }

            result = (self.client.table("gmaps_facebook_enrichments")
                     .update(update_data)
                     .eq("id", enrichment_id)
                     .execute())

            # Save to email verifications table with facebook_enrichment_id
            verification_record = {
                "business_id": business_id,
                "facebook_enrichment_id": enrichment_id,
                "email": verification_data.get("email"),
                "source": "facebook",
                "status": verification_data.get("status"),
                "score": verification_data.get("score"),
                "is_safe": verification_data.get("is_safe", False),
                "is_disposable": verification_data.get("is_disposable", False),
                "is_role_based": verification_data.get("is_role_based", False),
                "is_free_email": verification_data.get("is_free_email", False),
                "is_gibberish": verification_data.get("is_gibberish", False),
                "domain": verification_data.get("domain"),
                "provider": verification_data.get("provider"),
                "mx_records": verification_data.get("mx_records"),
                "smtp_check": verification_data.get("smtp_check"),
                "reason": verification_data.get("reason"),
                "suggestion": verification_data.get("suggestion"),
                "raw_response": verification_data.get("raw_response"),
                "verified_at": datetime.now().isoformat()
            }

            self.client.table("gmaps_email_verifications").insert(verification_record).execute()

            logging.info(f"✅ Updated Facebook enrichment verification for business {business_id}")
            return len(result.data) > 0

        except Exception as e:
            logging.error(f"Error updating Facebook verification: {e}")
            return False

    def update_google_maps_verification(self, business_id: str,
                                        verification_data: Dict[str, Any]) -> bool:
        """Update Google Maps business with email verification results"""
        try:
            # Update business record with verification results
            update_data = {
                "email_verified": True,
                "bouncer_status": verification_data.get("status"),
                "bouncer_score": verification_data.get("score"),
                "bouncer_reason": verification_data.get("reason"),
                "is_safe": verification_data.get("is_safe", False),
                "is_disposable": verification_data.get("is_disposable", False),
                "is_role_based": verification_data.get("is_role_based", False),
                "is_free_email": verification_data.get("is_free_email", False),
                "bouncer_verified_at": datetime.now().isoformat()
            }

            result = (self.client.table("gmaps_businesses")
                     .update(update_data)
                     .eq("id", business_id)
                     .execute())

            # Save to email verifications table with source = 'google_maps'
            verification_record = {
                "business_id": business_id,
                "email": verification_data.get("email"),
                "source": "google_maps",
                "status": verification_data.get("status"),
                "score": verification_data.get("score"),
                "is_safe": verification_data.get("is_safe", False),
                "is_disposable": verification_data.get("is_disposable", False),
                "is_role_based": verification_data.get("is_role_based", False),
                "is_free_email": verification_data.get("is_free_email", False),
                "is_gibberish": verification_data.get("is_gibberish", False),
                "domain": verification_data.get("domain"),
                "provider": verification_data.get("provider"),
                "mx_records": verification_data.get("mx_records"),
                "smtp_check": verification_data.get("smtp_check"),
                "reason": verification_data.get("reason"),
                "suggestion": verification_data.get("suggestion"),
                "raw_response": verification_data.get("raw_response"),
                "verified_at": datetime.now().isoformat()
            }

            self.client.table("gmaps_email_verifications").insert(verification_record).execute()

            logging.info(f"✅ Updated Google Maps email verification for business {business_id}")
            return len(result.data) > 0

        except Exception as e:
            logging.error(f"Error updating Google Maps verification: {e}")
            return False

    # Cost Tracking
    def track_api_cost(self, campaign_id: str, service: str, items: int,
                      cost_usd: float, metadata: Dict = None) -> bool:
        """Track API costs for a campaign"""
        try:
            record = {
                "campaign_id": campaign_id,
                "service": service,
                "items_processed": items,
                "cost_usd": cost_usd,
                "metadata": metadata or {},
                "incurred_at": datetime.now().isoformat()
            }
            
            result = self.client.table("gmaps_api_costs").insert(record).execute()
            
            # Update campaign costs
            if result.data:
                if service == "google_maps":
                    self.client.table("gmaps_campaigns").update({
                        "google_maps_cost": cost_usd,
                        "actual_cost": cost_usd
                    }).eq("id", campaign_id).execute()
                elif service == "facebook":
                    # Get current costs and add Facebook cost
                    campaign = self.get_campaign(campaign_id)
                    total = campaign.get("google_maps_cost", 0) + cost_usd
                    self.client.table("gmaps_campaigns").update({
                        "facebook_cost": cost_usd,
                        "actual_cost": total
                    }).eq("id", campaign_id).execute()
                elif service == "linkedin":
                    # Add LinkedIn cost to total
                    campaign = self.get_campaign(campaign_id)
                    total = (campaign.get("google_maps_cost", 0) +
                            campaign.get("facebook_cost", 0) +
                            cost_usd)
                    self.client.table("gmaps_campaigns").update({
                        "linkedin_enrichment_cost": cost_usd,
                        "actual_cost": total
                    }).eq("id", campaign_id).execute()
                elif service == "bouncer":
                    # Add Bouncer verification cost to total
                    campaign = self.get_campaign(campaign_id)
                    total = (campaign.get("google_maps_cost", 0) +
                            campaign.get("facebook_cost", 0) +
                            campaign.get("linkedin_enrichment_cost", 0) +
                            cost_usd)
                    self.client.table("gmaps_campaigns").update({
                        "bouncer_verification_cost": cost_usd,
                        "actual_cost": total
                    }).eq("id", campaign_id).execute()
            
            return len(result.data) > 0
            
        except Exception as e:
            logging.error(f"Error tracking API cost: {e}")
            return False
    
    # Analytics
    def get_campaign_analytics(self, campaign_id: str) -> Dict[str, Any]:
        """Get comprehensive analytics for a campaign"""
        try:
            # Get campaign data
            campaign = self.get_campaign(campaign_id)
            if not campaign:
                return {}
            
            # Get coverage stats
            coverage = self.get_campaign_coverage(campaign_id)
            scraped = [c for c in coverage if c.get("scraped")]
            
            # Get business stats
            businesses_result = (self.client.table("gmaps_businesses")
                               .select("id, email, enrichment_status")
                               .eq("campaign_id", campaign_id)
                               .execute())
            
            businesses = businesses_result.data or []
            with_email = [b for b in businesses if b.get("email")]
            enriched = [b for b in businesses if b.get("enrichment_status") == "enriched"]
            
            # Calculate metrics
            analytics = {
                "campaign_name": campaign.get("name"),
                "status": campaign.get("status"),
                "coverage_profile": campaign.get("coverage_profile"),
                
                # Coverage metrics
                "total_zips": len(coverage),
                "zips_scraped": len(scraped),
                "coverage_completion": round((len(scraped) / len(coverage) * 100), 1) if coverage else 0,
                
                # Business metrics
                "total_businesses": len(businesses),
                "businesses_with_email": len(with_email),
                "email_success_rate": round((len(with_email) / len(businesses) * 100), 1) if businesses else 0,
                
                # Enrichment metrics
                "total_enriched": len(enriched),
                "enrichment_rate": round((len(enriched) / len(businesses) * 100), 1) if businesses else 0,
                
                # Cost metrics
                "total_cost": campaign.get("actual_cost", 0),
                "cost_per_business": round(campaign.get("actual_cost", 0) / len(businesses), 2) if businesses else 0,
                "cost_per_email": round(campaign.get("actual_cost", 0) / len(with_email), 2) if with_email else 0,
                
                # Time metrics
                "created_at": campaign.get("created_at"),
                "started_at": campaign.get("started_at"),
                "completed_at": campaign.get("completed_at")
            }
            
            return analytics
            
        except Exception as e:
            logging.error(f"Error getting campaign analytics: {e}")
            return {}
    
    def get_zip_performance(self, campaign_id: str = None) -> List[Dict[str, Any]]:
        """Get performance metrics by ZIP code"""
        try:
            result = self.client.table("gmaps_campaign_coverage").select("*").eq("scraped", True)
            if campaign_id:
                result = result.eq("campaign_id", campaign_id)
            
            data = result.execute().data or []
            
            # Calculate metrics
            for row in data:
                if row.get("businesses_found", 0) > 0:
                    row["email_rate"] = round((row.get("emails_found", 0) / row["businesses_found"]) * 100, 1)
                else:
                    row["email_rate"] = 0
                
                if row.get("emails_found", 0) > 0:
                    row["cost_per_email"] = round(row.get("actual_cost", 0) / row["emails_found"], 2)
                else:
                    row["cost_per_email"] = None
            
            return data
            
        except Exception as e:
            logging.error(f"Error getting ZIP performance: {e}")
            return []
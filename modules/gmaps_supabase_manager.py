"""
Google Maps Supabase Manager
Handles all database operations for the Google Maps scraper
Uses public schema with gmaps_ prefixed tables for isolation
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from .supabase_manager import SupabaseManager
from .zip_demographics_service import ZipDemographicsService

class GmapsSupabaseManager(SupabaseManager):
    """Extended Supabase manager for Google Maps scraper operations"""

    def __init__(self, supabase_url: str = None, supabase_key: str = None, organization_id: str = None):
        """Initialize with gmaps_ prefixed tables in public schema"""
        super().__init__(supabase_url, supabase_key, organization_id)

        # Initialize ZIP demographics service for on-demand lookups
        self.zip_demographics = ZipDemographicsService(self.client)

        logging.info("✅ GmapsSupabaseManager initialized with gmaps_ prefixed tables and ZIP demographics")

    # =========================================================================
    # Raw Data Extraction Helpers (Sprint 3: Extract untapped Google Maps data)
    # =========================================================================

    def _extract_business_attributes(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract business attributes from Google Maps additionalInfo field.
        Returns dict with: is_women_owned, is_small_business, accepts_credit_cards, etc.
        """
        attributes = {
            "is_women_owned": False,
            "is_small_business": False,
            "is_veteran_owned": False,
            "is_minority_owned": False,
            "accepts_credit_cards": False,
            "accepts_nfc_payments": False,
            "is_wheelchair_accessible": False,
            "appointment_required": False,
        }

        additional_info = raw_data.get("additionalInfo", {})
        if not additional_info:
            return attributes

        # Flatten all the nested arrays into searchable text
        all_attributes = []
        for category, items in additional_info.items():
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        for key, value in item.items():
                            if value is True:
                                all_attributes.append(key.lower())

        # Map Google's attribute names to our fields
        attribute_text = " ".join(all_attributes)

        # Business ownership attributes
        if "women" in attribute_text or "woman" in attribute_text:
            attributes["is_women_owned"] = True
        if "small business" in attribute_text:
            attributes["is_small_business"] = True
        if "veteran" in attribute_text:
            attributes["is_veteran_owned"] = True
        if "minority" in attribute_text or "black" in attribute_text or "lgbtq" in attribute_text:
            attributes["is_minority_owned"] = True

        # Payment attributes
        if "credit card" in attribute_text or "credit cards" in attribute_text:
            attributes["accepts_credit_cards"] = True
        if "nfc" in attribute_text or "contactless" in attribute_text or "mobile payment" in attribute_text:
            attributes["accepts_nfc_payments"] = True
        if "debit card" in attribute_text:
            attributes["accepts_credit_cards"] = True  # Debit implies credit too

        # Accessibility
        if "wheelchair" in attribute_text:
            attributes["is_wheelchair_accessible"] = True

        # Appointment
        if "appointment" in attribute_text:
            attributes["appointment_required"] = True

        return attributes

    def _extract_booking_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract booking information from Google Maps bookingLinks field.
        Returns dict with: has_online_booking, booking_url
        """
        booking_links = raw_data.get("bookingLinks", [])
        reserve_url = raw_data.get("reserveTableUrl")
        table_links = raw_data.get("tableReservationLinks", [])

        has_booking = False
        booking_url = None

        # Check bookingLinks array
        if booking_links and len(booking_links) > 0:
            has_booking = True
            if isinstance(booking_links[0], dict):
                booking_url = booking_links[0].get("url") or booking_links[0].get("link")
            elif isinstance(booking_links[0], str):
                booking_url = booking_links[0]

        # Check reserveTableUrl
        if reserve_url:
            has_booking = True
            booking_url = booking_url or reserve_url

        # Check tableReservationLinks
        if table_links and len(table_links) > 0:
            has_booking = True
            if not booking_url:
                if isinstance(table_links[0], dict):
                    booking_url = table_links[0].get("url") or table_links[0].get("link")
                elif isinstance(table_links[0], str):
                    booking_url = table_links[0]

        return {
            "has_online_booking": has_booking,
            "booking_url": booking_url
        }

    def _extract_review_metrics(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract review distribution metrics from Google Maps reviewsDistribution field.
        Returns dict with: five_star_percent, review_sentiment_tags
        """
        distribution = raw_data.get("reviewsDistribution", {})
        review_tags = raw_data.get("reviewsTags", [])

        five_star_percent = None

        if distribution:
            one_star = distribution.get("oneStar", 0) or 0
            two_star = distribution.get("twoStar", 0) or 0
            three_star = distribution.get("threeStar", 0) or 0
            four_star = distribution.get("fourStar", 0) or 0
            five_star = distribution.get("fiveStar", 0) or 0

            total = one_star + two_star + three_star + four_star + five_star
            if total > 0:
                five_star_percent = round((five_star / total) * 100, 2)

        # Extract sentiment tags (keywords from reviews)
        sentiment_tags = []
        if review_tags:
            for tag in review_tags[:10]:  # Limit to 10 tags
                if isinstance(tag, dict):
                    sentiment_tags.append(tag.get("tag") or tag.get("text", ""))
                elif isinstance(tag, str):
                    sentiment_tags.append(tag)

        return {
            "five_star_percent": five_star_percent,
            "review_sentiment_tags": [t for t in sentiment_tags if t]  # Filter empty
        }

    def _extract_competitor_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract competitor information from Google Maps peopleAlsoSearch field.
        Returns dict with: competitor_count, competitors (JSONB array)
        """
        people_also_search = raw_data.get("peopleAlsoSearch", [])

        competitors = []
        for item in people_also_search[:10]:  # Limit to top 10
            if isinstance(item, dict):
                competitors.append({
                    "name": item.get("title", ""),
                    "rating": item.get("totalScore"),
                    "reviews": item.get("reviewsCount")
                })

        return {
            "competitor_count": len(competitors),
            "competitors": competitors
        }

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

            logging.info(f"🔄 Attempting to update campaign {campaign_id} with {len(updates)} fields")
            logging.debug(f"   Update fields: {list(updates.keys())}")

            result = self.client.table("gmaps_campaigns").update(updates).eq("id", campaign_id).execute()

            logging.info(f"📊 Update result: data length = {len(result.data) if result.data else 0}")
            if result.data:
                logging.info(f"✅ Campaign {campaign_id} updated successfully")
                logging.debug(f"   Updated fields in response: {list(result.data[0].keys())}")
                return True
            else:
                logging.error(f"❌ Campaign {campaign_id} update returned empty data - no rows matched or permissions issue")
                logging.error(f"   This suggests either: 1) Campaign doesn't exist, 2) RLS blocking, or 3) Wrong API key")
                return False

        except Exception as e:
            logging.error(f"❌ EXCEPTION updating campaign {campaign_id}: {e}")
            logging.error(f"   Exception type: {type(e).__name__}")
            import traceback
            logging.error(f"   Full traceback: {traceback.format_exc()}")
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

    def _extract_name_from_linkedin_url(self, linkedin_url: str) -> Optional[Dict[str, str]]:
        """Extract first/last name from personal LinkedIn URL (/in/firstname-lastname-id)"""
        if not linkedin_url or '/in/' not in linkedin_url:
            return None

        try:
            # Extract the path segment after /in/
            # Example: https://www.linkedin.com/in/dr-allie-day-goodwin-a775188
            import re
            match = re.search(r'/in/([a-zA-Z0-9-]+)', linkedin_url)
            if not match:
                return None

            slug = match.group(1)

            # Remove trailing ID (numbers/hex at the end)
            # Pattern: name-name-123456 or name-name-abc123def
            slug = re.sub(r'-[a-f0-9]{6,}$', '', slug, flags=re.IGNORECASE)
            slug = re.sub(r'-\d{5,}$', '', slug)

            # Split by hyphens
            parts = slug.split('-')

            # Filter out common prefixes like 'dr', 'md', 'phd'
            prefixes = {'dr', 'md', 'phd', 'dds', 'dc', 'do', 'dvm', 'esq', 'jr', 'sr', 'ii', 'iii'}
            filtered_parts = [p for p in parts if p.lower() not in prefixes]

            if len(filtered_parts) >= 2:
                first_name = filtered_parts[0].title()
                last_name = ' '.join(filtered_parts[1:]).title()
                return {
                    'first_name': first_name,
                    'last_name': last_name,
                    'full_name': f"{first_name} {last_name}"
                }
            elif len(filtered_parts) == 1:
                return {
                    'first_name': filtered_parts[0].title(),
                    'last_name': None,
                    'full_name': filtered_parts[0].title()
                }
        except Exception as e:
            logging.warning(f"Error extracting name from LinkedIn URL: {e}")

        return None

    def _extract_linkedin_url(self, business: Dict[str, Any]) -> Optional[str]:
        """Extract LinkedIn URL from various possible fields"""
        # Check 'linkedIns' field (plural) - this is what Google Maps returns
        linkedins = business.get("linkedIns", [])
        if linkedins and isinstance(linkedins, list) and len(linkedins) > 0:
            return linkedins[0]  # Return the first LinkedIn URL

        # Direct LinkedIn fields (singular)
        li_url = business.get("linkedinUrl") or business.get("linkedin")
        if li_url:
            return li_url

        # Check websiteDetails if available
        website_details = business.get("websiteDetails", {})
        if isinstance(website_details, dict):
            social_links = website_details.get("socialLinks", [])
            if isinstance(social_links, list):
                for link in social_links:
                    if "linkedin.com" in str(link).lower():
                        return link

        return None

    def save_businesses(self, businesses: List[Dict[str, Any]], campaign_id: str, zip_code: str) -> int:
        """Save Google Maps businesses to database with enriched data extraction"""
        try:
            if not businesses:
                return 0

            # =====================================================
            # Batch-fetch ZIP demographics for all businesses
            # =====================================================
            unique_zips = set()
            for b in businesses:
                biz_zip = b.get('extracted_zip') or b.get('postalCode') or zip_code
                if biz_zip:
                    unique_zips.add(str(biz_zip)[:5].zfill(5))

            # Fetch demographics for all ZIPs at once (uses cache + Census API fallback)
            demographics_map = {}
            if unique_zips:
                try:
                    demographics_map = self.zip_demographics.get_demographics(list(unique_zips))
                    logging.info(f"📊 Fetched demographics for {len(demographics_map)} ZIP codes")
                except Exception as e:
                    logging.warning(f"Could not fetch ZIP demographics: {e}")

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

                # Extract icebreaker fields if present
                icebreaker = business.get('icebreaker')
                subject_line = business.get('subject_line')
                icebreaker_generated_at = datetime.now().isoformat() if icebreaker else None

                # =====================================================
                # Sprint 3: Extract untapped data from raw Google Maps
                # =====================================================
                raw_data = business  # The business dict IS the raw data

                # Extract business attributes (women-owned, small business, etc.)
                attributes = self._extract_business_attributes(raw_data)

                # Extract booking information
                booking = self._extract_booking_info(raw_data)

                # Extract review metrics (five-star percent, sentiment tags)
                review_metrics = self._extract_review_metrics(raw_data)

                # Extract competitor information
                competitor_info = self._extract_competitor_info(raw_data)

                # Extract LinkedIn URL and try to get name from personal profiles
                linkedin_url = self._extract_linkedin_url(business)
                linkedin_name = self._extract_name_from_linkedin_url(linkedin_url) if linkedin_url else None

                # Set contact name from LinkedIn if available (only for personal profiles)
                contact_first_name = linkedin_name.get('first_name') if linkedin_name else None
                contact_last_name = linkedin_name.get('last_name') if linkedin_name else None

                record = {
                    "campaign_id": campaign_id,
                    "zip_code": business.get('extracted_zip', zip_code),
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
                    "linkedin_url": linkedin_url,
                    "needs_enrichment": bool(self._extract_facebook_url(business)),
                    "enrichment_status": "pending" if self._extract_facebook_url(business) else "no_facebook",
                    "icebreaker": icebreaker,
                    "subject_line": subject_line,
                    "icebreaker_generated_at": icebreaker_generated_at,
                    "raw_data": business,
                    "scraped_at": datetime.now().isoformat(),

                    # =====================================================
                    # NEW: Enriched fields from raw data extraction
                    # =====================================================
                    # Business attributes
                    "is_women_owned": attributes["is_women_owned"],
                    "is_small_business": attributes["is_small_business"],
                    "is_veteran_owned": attributes["is_veteran_owned"],
                    "is_minority_owned": attributes["is_minority_owned"],
                    "accepts_credit_cards": attributes["accepts_credit_cards"],
                    "accepts_nfc_payments": attributes["accepts_nfc_payments"],
                    "is_wheelchair_accessible": attributes["is_wheelchair_accessible"],
                    "appointment_required": attributes["appointment_required"],

                    # Booking information
                    "has_online_booking": booking["has_online_booking"],
                    "booking_url": booking["booking_url"],

                    # Review metrics
                    "five_star_percent": review_metrics["five_star_percent"],
                    "review_sentiment_tags": review_metrics["review_sentiment_tags"],

                    # Competitor information
                    "competitor_count": competitor_info["competitor_count"],
                    "competitors": competitor_info["competitors"],

                    # Contact name from LinkedIn (if personal profile found)
                    "contact_first_name": contact_first_name,
                    "contact_last_name": contact_last_name,
                }

                # =====================================================
                # Add ZIP demographics to the business record
                # =====================================================
                biz_zip = business.get('extracted_zip') or business.get('postalCode') or zip_code
                if biz_zip:
                    biz_zip = str(biz_zip)[:5].zfill(5)
                    demo = demographics_map.get(biz_zip, {})
                    if demo:
                        record["zip_population"] = demo.get('population')
                        record["zip_median_income"] = demo.get('median_household_income')
                        record["zip_median_age"] = demo.get('median_age')
                        record["zip_pct_college"] = demo.get('pct_college_or_higher')
                        record["zip_unemployment_rate"] = demo.get('unemployment_rate')
                        record["zip_market_score"] = demo.get('market_opportunity_score')
                        record["zip_lead_tier"] = demo.get('lead_quality_tier')
                        record["zip_known_for"] = demo.get('known_for')
                        record["zip_target_industries"] = demo.get('target_industries')

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
        """Save Facebook enrichment results with Sprint 3 enhanced data extraction"""
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
                "scraped_at": datetime.now().isoformat(),
                # Sprint 3: Additional Facebook data
                "page_likes": enrichment_data.get("page_likes"),
                "page_followers": enrichment_data.get("page_followers"),
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

                # Sprint 3: Save company_age_years to business record
                company_age = enrichment_data.get("company_age_years")
                if company_age is not None:
                    update_data["company_age_years"] = company_age

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
        """Save LinkedIn enrichment results to database with email quality tracking and parsed contact info"""
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
                # Update business with LinkedIn URL and parsed contact info
                update_data = {
                    "linkedin_url": enrichment_data.get("linkedin_url"),
                    "linkedin_enriched": True
                }

                # NEW: Save parsed contact name fields to business record
                contact_first = enrichment_data.get("contact_first_name")
                contact_last = enrichment_data.get("contact_last_name")
                contact_title = enrichment_data.get("contact_title")
                contact_seniority = enrichment_data.get("contact_seniority_level")

                if contact_first:
                    update_data["contact_first_name"] = contact_first
                if contact_last:
                    update_data["contact_last_name"] = contact_last
                if contact_title:
                    update_data["contact_title"] = contact_title
                if contact_seniority:
                    update_data["contact_seniority_level"] = contact_seniority

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

    def refresh_master_leads(self) -> bool:
        """Refresh the master_leads materialized view.

        Call this after campaign completion to update the centralized
        deduplicated database of all businesses across all organizations.
        """
        try:
            self.client.rpc('refresh_master_leads').execute()
            logging.info("✅ Master leads view refreshed successfully")
            return True
        except Exception as e:
            logging.error(f"Error refreshing master leads: {e}")
            return False
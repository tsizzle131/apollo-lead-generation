import logging
import os
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from supabase import create_client, Client
import config

class SupabaseManager:
    def __init__(self, supabase_url: str = None, supabase_key: str = None, organization_id: str = None, audience_id: str = None):
        """Initialize Supabase client with organization and audience context"""
        self.supabase_url = supabase_url or self._get_supabase_url()
        self.supabase_key = supabase_key or self._get_supabase_key()
        self.organization_id = organization_id or config.CURRENT_ORGANIZATION_ID
        self.audience_id = audience_id
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Supabase URL and key must be provided")
        
        try:
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            if self.organization_id:
                logging.info(f"✅ Supabase client initialized for organization: {self.organization_id}")
            else:
                logging.info("✅ Supabase client initialized successfully (no organization context)")
        except Exception as e:
            logging.error(f"❌ Failed to initialize Supabase client: {e}")
            raise

    def _get_supabase_url(self) -> str:
        """Get Supabase URL from UI config or environment"""
        # Try UI config first, then environment
        ui_config = getattr(config, '_ui_config', {})
        supabase_settings = ui_config.get('supabase', {})
        return supabase_settings.get('url') or os.getenv('SUPABASE_URL')

    def _get_supabase_key(self) -> str:
        """Get Supabase key from UI config or environment"""
        ui_config = getattr(config, '_ui_config', {})
        supabase_settings = ui_config.get('supabase', {})
        return supabase_settings.get('key') or os.getenv('SUPABASE_KEY')

    async def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute raw SQL query - compatibility method for phone system"""
        try:
            # Supabase Python client doesn't have direct SQL execution
            # This is a workaround - in production, we'd use proper table methods
            # For now, return empty list to prevent crashes
            logging.warning(f"execute_query called with: {query[:100]}... - Not implemented with Supabase Python client")
            return []
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            raise

    # Search URLs Management
    def find_or_create_search_url(self, url: str, notes: str = None) -> Dict[str, Any]:
        """Find existing search URL or create a new one (within organization context)"""
        try:
            # First, try to find existing search URL within organization
            query = self.client.table("search_urls").select("*").eq("url", url)
            if self.organization_id:
                query = query.eq("organization_id", self.organization_id)
            existing = query.execute()
            
            if existing.data:
                logging.info(f"🔍 Found existing search URL: {existing.data[0]['id']} - reusing for re-run")
                # Reset status to pending for re-running
                search_url_id = existing.data[0]['id']
                self.update_search_url_status(search_url_id, "pending")
                return existing.data[0]
            
            # If not found, create new one
            logging.info(f"🔍 Creating new search URL entry")
            return self.create_search_url(url, notes)
            
        except Exception as e:
            logging.error(f"Error finding/creating search URL: {e}")
            return None

    def create_search_url(self, url: str, notes: str = None) -> Dict[str, Any]:
        """Create a new search URL entry"""
        try:
            data = {
                "url": url,
                "status": "pending",
                "notes": notes,
                "organization_id": self.organization_id
            }
            
            result = self.client.table("search_urls").insert(data).execute()
            
            if result.data:
                logging.info(f"✅ Created search URL: {url}")
                return result.data[0]
            else:
                logging.error("Failed to create search URL")
                return {}
                
        except Exception as e:
            logging.error(f"Error creating search URL: {e}")
            return {}

    def get_search_urls(self, status: str = None) -> List[Dict[str, Any]]:
        """Get search URLs, optionally filtered by status (within organization context)"""
        try:
            query = self.client.table("search_urls").select("*")
            
            if self.organization_id:
                query = query.eq("organization_id", self.organization_id)
            
            if status:
                query = query.eq("status", status)
            
            result = query.order("created_at", desc=True).execute()
            return result.data or []
            
        except Exception as e:
            logging.error(f"Error fetching search URLs: {e}")
            return []

    def update_search_url_status(self, search_url_id: str, status: str, total_contacts: int = None) -> bool:
        """Update search URL status and contact count"""
        try:
            data = {"status": status}
            
            if status == "completed":
                data["processed_at"] = datetime.now().isoformat()
            
            if total_contacts is not None:
                data["total_contacts_found"] = total_contacts
            
            result = self.client.table("search_urls").update(data).eq("id", search_url_id).execute()
            
            return len(result.data) > 0
            
        except Exception as e:
            logging.error(f"Error updating search URL status: {e}")
            return False

    # Campaign Management
    def get_campaigns(self, status: str = None) -> List[Dict[str, Any]]:
        """Get campaigns, optionally filtered by status (within organization context)"""
        try:
            query = self.client.table("campaigns").select("*")
            
            if self.organization_id:
                query = query.eq("organization_id", self.organization_id)
            
            if status:
                query = query.eq("status", status)
            
            result = query.order("priority", desc=True).order("created_at", desc=True).execute()
            return result.data or []
            
        except Exception as e:
            logging.error(f"Error fetching campaigns: {e}")
            return []

    def get_campaign_by_id(self, campaign_id: str) -> Dict[str, Any]:
        """Get a specific campaign by ID (within organization context)"""
        try:
            query = self.client.table("campaigns").select("*").eq("id", campaign_id)
            if self.organization_id:
                query = query.eq("organization_id", self.organization_id)
            result = query.execute()
            return result.data[0] if result.data else {}
            
        except Exception as e:
            logging.error(f"Error fetching campaign {campaign_id}: {e}")
            return {}

    def get_campaign_search_urls(self, campaign_id: str, status: str = None) -> List[Dict[str, Any]]:
        """Get search URLs for a specific campaign (within organization context)"""
        try:
            query = self.client.table("search_urls").select("*").eq("campaign_id", campaign_id)
            
            if self.organization_id:
                query = query.eq("organization_id", self.organization_id)
            
            if status:
                query = query.eq("status", status)
            
            result = query.order("created_at", desc=True).execute()
            return result.data or []
            
        except Exception as e:
            logging.error(f"Error fetching campaign search URLs: {e}")
            return []

    def update_campaign_status(self, campaign_id: str, status: str) -> bool:
        """Update campaign status"""
        try:
            result = self.client.table("campaigns").update({
                "status": status,
                "updated_at": datetime.now().isoformat()
            }).eq("id", campaign_id).execute()
            
            return len(result.data) > 0
            
        except Exception as e:
            logging.error(f"Error updating campaign status: {e}")
            return False

    # Raw Contacts Management
    def batch_insert_raw_contacts(self, contacts: List[Dict[str, Any]], search_url_id: str) -> int:
        """Batch insert raw contacts from Apollo/Apify"""
        try:
            logging.info(f"🔍 DEBUG SUPABASE: batch_insert_raw_contacts called with {len(contacts)} contacts, search_url_id={search_url_id}")
            
            if not contacts:
                logging.warning(f"🔍 DEBUG SUPABASE: No contacts provided - returning 0")
                return 0

            # Get campaign_id from search_url
            campaign_id = None
            try:
                search_url_result = self.client.table("search_urls").select("campaign_id").eq("id", search_url_id).single().execute()
                if search_url_result.data:
                    campaign_id = search_url_result.data.get("campaign_id")
                    logging.info(f"📌 Found campaign_id: {campaign_id} for search_url_id: {search_url_id}")
            except Exception as e:
                logging.warning(f"⚠️ Could not fetch campaign_id for search_url {search_url_id}: {e}")

            # Prepare contacts for insertion
            processed_contacts = []
            logging.info(f"🔍 DEBUG SUPABASE: Starting contact processing loop")
            for i, contact in enumerate(contacts):
                processed_contact = {
                    "search_url_id": search_url_id,
                    "campaign_id": campaign_id,  # Add the campaign_id
                    "apollo_id": contact.get("id"),
                    "last_name": contact.get("last_name"),
                    "name": contact.get("name") or contact.get("first_name"),
                    "linkedin_url": contact.get("linkedin_url"),
                    "title": contact.get("title") or contact.get("headline"),
                    "email_status": contact.get("email_status"),
                    "photo_url": contact.get("photo_url"),
                    "twitter_url": contact.get("twitter_url"),
                    "github_url": contact.get("github_url"),
                    "facebook_url": contact.get("facebook_url"),
                    "extrapolated_email_confidence": contact.get("extrapolated_email_confidence"),
                    "headline": contact.get("headline"),
                    "email": contact.get("email"),
                    "organization_id": self.organization_id,  # Use organization context, not contact data
                    "audience_id": self.audience_id,  # Link contacts to the audience being scraped
                    "degree": contact.get("degree"),
                    "grade_level": contact.get("grade_level"),
                    "website_url": self._extract_website_url(contact),
                    "raw_data_json": contact,  # Store complete original data
                    "processed": False
                }
                processed_contacts.append(processed_contact)

            # Insert in smaller, more reliable batches
            from config import DATABASE_BATCH_SIZE, DATABASE_MAX_RETRIES
            batch_size = DATABASE_BATCH_SIZE  # Use smaller batch size (25 instead of 100)
            total_inserted = 0
            
            logging.info(f"🔍 DEBUG SUPABASE: Processed {len(processed_contacts)} contacts, inserting in batches of {batch_size}")
            
            for i in range(0, len(processed_contacts), batch_size):
                batch = processed_contacts[i:i + batch_size]
                batch_num = i//batch_size + 1
                
                logging.info(f"🔍 DEBUG SUPABASE: Inserting batch {batch_num} with {len(batch)} contacts")
                logging.info(f"🔍 DEBUG SUPABASE: Sample contact from batch: {batch[0] if batch else 'Empty batch'}")
                
                # Try batch insertion with retry logic
                batch_inserted = self._insert_batch_with_retry(batch, batch_num, DATABASE_MAX_RETRIES)
                total_inserted += batch_inserted

            logging.info(f"✅ SUPABASE SUCCESS: Total raw contacts processed: {total_inserted}")
            logging.info(f"🔍 DEBUG SUPABASE: This includes new contacts + updated duplicates")
            return total_inserted
            
        except Exception as e:
            logging.error(f"❌ CRITICAL SUPABASE: Error batch inserting raw contacts: {e}")
            logging.error(f"🔍 DEBUG SUPABASE: Exception type: {type(e)}")
            logging.error(f"🔍 DEBUG SUPABASE: Exception details: {str(e)}")
            import traceback
            logging.error(f"🔍 DEBUG SUPABASE: Full traceback: {traceback.format_exc()}")
            return 0

    def _insert_batch_with_retry(self, batch: List[Dict[str, Any]], batch_num: int, max_retries: int) -> int:
        """Insert a batch with retry logic for timeouts and network errors"""
        import time
        
        for attempt in range(max_retries):
            try:
                # Use upsert to handle duplicates gracefully
                result = self.client.table("raw_contacts").upsert(batch, on_conflict="apollo_id,search_url_id").execute()
                
                if result.data:
                    inserted_count = len(result.data)
                    logging.info(f"✅ Batch {batch_num} inserted: {inserted_count} contacts (attempt {attempt + 1})")
                    return inserted_count
                else:
                    logging.error(f"❌ Batch {batch_num} returned no data (attempt {attempt + 1})")
                    
            except Exception as e:
                error_str = str(e).lower()
                
                # Handle different types of errors
                if "timeout" in error_str or "timed out" in error_str:
                    if attempt < max_retries - 1:
                        wait_time = 5 * (2 ** attempt)  # 5s, 10s, 20s
                        logging.warning(f"⏰ Batch {batch_num} timeout (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    else:
                        logging.error(f"❌ Batch {batch_num} timeout retries exhausted")
                        return self._fallback_individual_inserts(batch, batch_num)
                        
                elif "reset" in error_str or "connection" in error_str:
                    if attempt < max_retries - 1:
                        wait_time = 3 * (attempt + 1)  # 3s, 6s, 9s
                        logging.warning(f"🔌 Batch {batch_num} connection error (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    else:
                        logging.error(f"❌ Batch {batch_num} connection retries exhausted")
                        return self._fallback_individual_inserts(batch, batch_num)
                        
                elif "duplicate key" in error_str:
                    logging.warning(f"⚠️ Batch {batch_num} duplicate key error, trying individual inserts")
                    return self._fallback_individual_inserts(batch, batch_num)
                    
                else:
                    logging.error(f"❌ Batch {batch_num} unknown error (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    else:
                        return self._fallback_individual_inserts(batch, batch_num)
        
        return 0
    
    def _fallback_individual_inserts(self, batch: List[Dict[str, Any]], batch_num: int) -> int:
        """Fallback: try inserting contacts individually when batch fails"""
        logging.info(f"🔄 Trying individual inserts for batch {batch_num} ({len(batch)} contacts)")
        individual_count = 0
        
        for i, contact in enumerate(batch):
            try:
                result = self.client.table("raw_contacts").upsert([contact], on_conflict="apollo_id,search_url_id").execute()
                if result.data:
                    individual_count += len(result.data)
            except Exception as e:
                logging.warning(f"⚠️ Skipping contact {i+1} in batch {batch_num}: {str(e)[:100]}")
                continue
        
        logging.info(f"✅ Individual inserts for batch {batch_num}: {individual_count}/{len(batch)} contacts")
        return individual_count

    def _extract_website_url(self, contact: Dict[str, Any]) -> str:
        """Extract website URL from various possible fields"""
        # Try different fields where website might be stored
        website = contact.get("website_url")
        if website:
            return website
            
        # Try organization data
        org = contact.get("organization", {})
        if isinstance(org, dict):
            return org.get("website_url", "")
            
        return ""

    def get_google_maps_contacts_needing_enrichment(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get Google Maps contacts that need email enrichment"""
        try:
            logging.info(f"🔍 Looking for Google Maps contacts needing email enrichment...")
            
            # Simple query - just get contacts without email but with website
            query = (
                self.client.table("raw_contacts")
                .select("*")
                .eq("processed", False)
                .is_("email", "null")  # No email yet
                .not_.is_("website_url", "null")  # But has website
                .neq("website_url", "")
            )
            
            if self.organization_id:
                query = query.eq("organization_id", self.organization_id)
            
            if limit:
                query = query.limit(limit)
            
            result = query.execute()
            logging.info(f"📊 Found {len(result.data or [])} Google Maps contacts needing email enrichment")
            
            return result.data or []
            
        except Exception as e:
            logging.error(f"Error getting contacts for enrichment: {e}")
            return []
    
    def get_unprocessed_contacts(self, limit: int = 100, min_confidence: float = 0.7) -> List[Dict[str, Any]]:
        """Get unprocessed contacts that meet quality criteria (within organization context)"""
        try:
            logging.info(f"📋 Building query for unprocessed contacts...")
            logging.info(f"  - Organization ID: {self.organization_id}")
            logging.info(f"  - Limit: {limit}")
            logging.info(f"  - Min confidence: {min_confidence}")
            
            query = (
                self.client.table("raw_contacts")
                .select("*, search_urls!inner(url)")
                .eq("processed", False)
                .not_.is_("email", "null")
                .not_.is_("website_url", "null")
                .neq("website_url", "")
                .eq("email_status", "verified")  # Only accept verified emails - no guessed ones
            )
            
            if self.organization_id:
                logging.info(f"  - Adding organization filter: {self.organization_id}")
                query = query.eq("organization_id", self.organization_id)
            
            # Apply limit only if specified (None means get ALL contacts)
            if limit is not None:
                query = query.limit(limit)
            
            logging.info(f"🔍 Executing query...")
            result = query.execute()
            logging.info(f"🔍 Query returned {len(result.data or [])} unprocessed contacts")
            
            if result.data and len(result.data) > 0:
                logging.info(f"  - First contact: {result.data[0].get('name', 'Unknown')} ({result.data[0].get('email', 'No email')})")
            
            return result.data or []
            
        except Exception as e:
            import traceback
            logging.error(f"❌ Error fetching unprocessed contacts: {e}")
            logging.error(f"❌ Traceback:\n{traceback.format_exc()}")
            return []

    def update_contact_email(self, contact_id: str, email: str, email_status: str = "verified") -> bool:
        """Update a contact's email after website scraping"""
        try:
            result = (
                self.client.table("raw_contacts")
                .update({
                    "email": email,
                    "email_status": email_status
                })
                .eq("id", contact_id)
                .execute()
            )
            
            if result.data:
                logging.info(f"✅ Updated contact {contact_id} with email: {email}")
                return True
            return False
            
        except Exception as e:
            logging.error(f"Error updating contact email: {e}")
            return False
    
    def mark_contact_processed(self, contact_id: str) -> bool:
        """Mark a raw contact as processed"""
        try:
            result = (
                self.client.table("raw_contacts")
                .update({"processed": True})
                .eq("id", contact_id)
                .execute()
            )
            
            if result.data and len(result.data) > 0:
                logging.info(f"✅ Marked contact {contact_id} as processed")
                return True
            else:
                logging.error(f"❌ Failed to mark contact {contact_id} as processed - no rows updated")
                return False
            
        except Exception as e:
            logging.error(f"Error marking contact as processed: {e}")
            return False

    # Processed Leads Management
    def create_processed_lead(self, raw_contact_id: str, search_url_id: str, 
                            lead_data: Dict[str, Any], processing_settings: Dict[str, Any]) -> Dict[str, Any]:
        """Create a processed lead with AI-generated content (within organization context)"""
        try:
            data = {
                "raw_contact_id": raw_contact_id,
                "search_url_id": search_url_id,
                "first_name": lead_data.get("first_name", ""),
                "last_name": lead_data.get("last_name", ""),
                "email": lead_data.get("email", ""),
                "linkedin_url": lead_data.get("linkedin_url", ""),
                "headline": lead_data.get("headline", ""),
                "website_url": lead_data.get("website_url", ""),
                "location": lead_data.get("location", ""),
                "icebreaker": lead_data.get("icebreaker", ""),
                "subject_line": lead_data.get("subject_line", ""),
                "website_summaries": lead_data.get("website_summaries", []),
                "processing_settings_used": processing_settings,
                "organization_id": self.organization_id,
                "status": "new"
            }
            
            logging.info(f"🔍 DEBUG: Inserting lead data: {data['first_name']} {data['last_name']}")
            logging.info(f"🔍 DEBUG: Icebreaker length: {len(data['icebreaker'])} chars")
            
            # Use upsert to prevent duplicates - conflict on raw_contact_id
            result = self.client.table("processed_leads").upsert(data, on_conflict="raw_contact_id").execute()
            
            if result.data:
                logging.info(f"✅ SUCCESS: Processed lead stored in database with ID: {result.data[0].get('id')}")
                return result.data[0]
            else:
                logging.error(f"❌ FAILED: Database insert returned no data")
                logging.error(f"🔍 DEBUG: Insert result: {result}")
                return {}
                
        except Exception as e:
            logging.error(f"❌ CRITICAL: Error creating processed lead: {e}")
            logging.error(f"🔍 DEBUG: Exception type: {type(e)}")
            import traceback
            logging.error(f"🔍 DEBUG: Full traceback: {traceback.format_exc()}")
            return {}

    def get_processed_leads(self, status: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get processed leads, optionally filtered by status (within organization context)"""
        try:
            query = (
                self.client.table("processed_leads")
                .select("*, raw_contacts!inner(linkedin_url, title), search_urls!inner(url)")
            )
            
            if self.organization_id:
                query = query.eq("organization_id", self.organization_id)
            
            if status:
                query = query.eq("status", status)
            
            result = query.order("created_at", desc=True).limit(limit).execute()
            return result.data or []
            
        except Exception as e:
            logging.error(f"Error fetching processed leads: {e}")
            return []

    def update_lead_status(self, lead_id: str, status: str, notes: str = None) -> bool:
        """Update lead status and notes"""
        try:
            data = {"status": status}
            
            if notes:
                data["notes"] = notes
            
            if status == "contacted":
                data["contacted_at"] = datetime.now().isoformat()
            
            result = (
                self.client.table("processed_leads")
                .update(data)
                .eq("id", lead_id)
                .execute()
            )
            
            return len(result.data) > 0
            
        except Exception as e:
            logging.error(f"Error updating lead status: {e}")
            return False

    # Analytics and Reporting
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics"""
        try:
            # Get pipeline view data
            result = self.client.table("v_contact_pipeline").select("*").execute()
            pipeline_data = result.data or []
            
            # Calculate totals
            stats = {
                "total_searches": len(pipeline_data),
                "total_raw_contacts": sum(row.get("total_raw_contacts", 0) for row in pipeline_data),
                "total_processed": sum(row.get("processed_contacts", 0) for row in pipeline_data),
                "total_leads": sum(row.get("generated_leads", 0) for row in pipeline_data),
                "total_contacted": sum(row.get("contacted_leads", 0) for row in pipeline_data),
                "total_responded": sum(row.get("responded_leads", 0) for row in pipeline_data),
                "total_converted": sum(row.get("converted_leads", 0) for row in pipeline_data),
                "searches_by_status": {},
                "pipeline_data": pipeline_data
            }
            
            # Group by search status
            for row in pipeline_data:
                status = row.get("search_status", "unknown")
                if status not in stats["searches_by_status"]:
                    stats["searches_by_status"][status] = 0
                stats["searches_by_status"][status] += 1
            
            return stats
            
        except Exception as e:
            logging.error(f"Error getting pipeline stats: {e}")
            return {}

    def test_connection(self) -> bool:
        """Test Supabase connection"""
        try:
            # Try to fetch from search_urls table
            result = self.client.table("search_urls").select("id").limit(1).execute()
            logging.info("✅ Supabase connection test successful")
            return True
            
        except Exception as e:
            logging.error(f"❌ Supabase connection test failed: {e}")
            return False

    # Database Management
    def initialize_database(self) -> bool:
        """Initialize database with schema (if needed)"""
        try:
            # This would be handled by running the SQL schema file in Supabase dashboard
            # For now, just test the connection
            return self.test_connection()
            
        except Exception as e:
            logging.error(f"Error initializing database: {e}")
            return False

    def clear_all_data(self) -> bool:
        """Clear all data from tables (for testing)"""
        try:
            # Delete in correct order due to foreign keys
            self.client.table("processed_leads").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            self.client.table("raw_contacts").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            self.client.table("search_urls").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            
            logging.info("✅ All data cleared from database")
            return True
            
        except Exception as e:
            logging.error(f"Error clearing database: {e}")
            return False

    # Export Functions
    def export_leads_to_dict(self, status: str = None) -> List[Dict[str, Any]]:
        """Export leads data for CSV/Excel export"""
        try:
            leads = self.get_processed_leads(status=status, limit=10000)
            
            export_data = []
            for lead in leads:
                export_row = {
                    "First Name": lead.get("first_name"),
                    "Last Name": lead.get("last_name"),
                    "Email": lead.get("email"),
                    "Website": lead.get("website_url"),
                    "Location": lead.get("location"),
                    "Status": lead.get("status"),
                    "Icebreaker": lead.get("icebreaker"),
                    "LinkedIn": lead.get("raw_contacts", {}).get("linkedin_url"),
                    "Title": lead.get("raw_contacts", {}).get("title"),
                    "Created": lead.get("created_at"),
                    "Search URL": lead.get("search_urls", {}).get("url")
                }
                export_data.append(export_row)
            
            return export_data
            
        except Exception as e:
            logging.error(f"Error exporting leads: {e}")
            return []
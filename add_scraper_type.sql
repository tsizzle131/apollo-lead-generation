-- Migration to add scraper_type support for campaigns
-- This allows campaigns to use either Apollo or Local Business scrapers

-- Add scraper_type column to campaigns table
ALTER TABLE campaigns 
ADD COLUMN IF NOT EXISTS scraper_type TEXT DEFAULT 'apollo' 
CHECK (scraper_type IN ('apollo', 'local'));

-- Add scraper_type column to search_urls table to track source
ALTER TABLE search_urls 
ADD COLUMN IF NOT EXISTS scraper_type TEXT DEFAULT 'apollo' 
CHECK (scraper_type IN ('apollo', 'local'));

-- Add source column to raw_contacts to track where contacts came from
ALTER TABLE raw_contacts
ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'apollo';

-- Add source column to processed_leads for tracking
ALTER TABLE processed_leads
ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'apollo';

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_campaigns_scraper_type ON campaigns(scraper_type);
CREATE INDEX IF NOT EXISTS idx_search_urls_scraper_type ON search_urls(scraper_type);
CREATE INDEX IF NOT EXISTS idx_raw_contacts_source ON raw_contacts(source);
CREATE INDEX IF NOT EXISTS idx_processed_leads_source ON processed_leads(source);

-- Comment on new columns
COMMENT ON COLUMN campaigns.scraper_type IS 'Type of scraper used: apollo for B2B professionals, local for Google Maps + LinkedIn';
COMMENT ON COLUMN search_urls.scraper_type IS 'Type of scraper associated with this URL';
COMMENT ON COLUMN raw_contacts.source IS 'Source of the contact data (apollo, google_maps_only, local_business)';
COMMENT ON COLUMN processed_leads.source IS 'Source of the lead data for analytics';

SELECT 'Scraper type migration completed successfully!' as result;
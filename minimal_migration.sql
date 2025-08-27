-- Minimal Migration - Add missing tables and audience system
-- Run this in your Supabase SQL Editor

-- Create audiences table for managing target segments
CREATE TABLE IF NOT EXISTS audiences (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    
    name TEXT NOT NULL,
    description TEXT,
    
    -- Metadata
    total_urls INTEGER DEFAULT 0,
    estimated_contacts INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending', -- 'pending', 'scraping', 'ready', 'error'
    
    -- Processing info
    last_scraped_at TIMESTAMPTZ,
    scraping_progress INTEGER DEFAULT 0, -- percentage
    error_message TEXT,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(organization_id, name)
);

-- Create audience_urls junction table to link audiences to URLs
CREATE TABLE IF NOT EXISTS audience_urls (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    audience_id UUID NOT NULL REFERENCES audiences(id) ON DELETE CASCADE,
    search_url_id UUID NOT NULL REFERENCES search_urls(id) ON DELETE CASCADE,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(audience_id, search_url_id)
);

-- Update campaigns table to link to audiences (add column if missing)
ALTER TABLE campaigns 
ADD COLUMN IF NOT EXISTS audience_id UUID REFERENCES audiences(id) ON DELETE SET NULL;

-- Create contacts table (the main missing piece)
CREATE TABLE IF NOT EXISTS contacts (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    search_url_id UUID REFERENCES search_urls(id) ON DELETE SET NULL,
    campaign_id UUID REFERENCES campaigns(id) ON DELETE SET NULL,
    audience_id UUID REFERENCES audiences(id) ON DELETE SET NULL,
    
    -- Contact information
    first_name TEXT,
    last_name TEXT,
    full_name TEXT,
    email TEXT,
    phone TEXT,
    linkedin_url TEXT,
    company_name TEXT,
    job_title TEXT,
    location TEXT,
    
    -- Generated content
    website_summaries TEXT[],
    icebreaker_content TEXT,
    
    -- Timestamps and processing
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    
    -- Contact attempt tracking  
    email_sent_at TIMESTAMPTZ,
    phone_called_at TIMESTAMPTZ,
    last_contact_attempt TIMESTAMPTZ
);

-- Campaign URLs junction table (if missing)
CREATE TABLE IF NOT EXISTS campaign_urls (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    search_url_id UUID REFERENCES search_urls(id) ON DELETE CASCADE,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(campaign_id, search_url_id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_audiences_org_id ON audiences(organization_id);
CREATE INDEX IF NOT EXISTS idx_audiences_status ON audiences(status);
CREATE INDEX IF NOT EXISTS idx_audience_urls_org_id ON audience_urls(organization_id);
CREATE INDEX IF NOT EXISTS idx_audience_urls_audience_id ON audience_urls(audience_id);
CREATE INDEX IF NOT EXISTS idx_audience_urls_url_id ON audience_urls(search_url_id);
CREATE INDEX IF NOT EXISTS idx_campaigns_audience_id ON campaigns(audience_id);
CREATE INDEX IF NOT EXISTS idx_contacts_org_id ON contacts(organization_id);
CREATE INDEX IF NOT EXISTS idx_contacts_campaign_id ON contacts(campaign_id);
CREATE INDEX IF NOT EXISTS idx_contacts_audience_id ON contacts(audience_id);
CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email);
CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone);
CREATE INDEX IF NOT EXISTS idx_campaign_urls_org_id ON campaign_urls(organization_id);
CREATE INDEX IF NOT EXISTS idx_campaign_urls_campaign_id ON campaign_urls(campaign_id);

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Migration completed! Added audience management system with the following tables:';
    RAISE NOTICE '- audiences: For managing target segments';
    RAISE NOTICE '- audience_urls: Links audiences to Apollo URLs';
    RAISE NOTICE '- contacts: For storing scraped leads';
    RAISE NOTICE '- campaign_urls: Junction table for campaigns';
    RAISE NOTICE 'Your system now supports audience-centric lead management!';
END $$;
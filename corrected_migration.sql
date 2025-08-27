-- Corrected Minimal Migration - Audience System for Lead Generation
-- Run this in your Supabase SQL Editor
-- This migration is safe to run multiple times (idempotent)

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create organizations table if it doesn't exist
CREATE TABLE IF NOT EXISTS organizations (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    description TEXT,
    contact_email TEXT,
    subscription_plan TEXT DEFAULT 'free',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create campaigns table if it doesn't exist
CREATE TABLE IF NOT EXISTS campaigns (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT DEFAULT 'active',
    priority INTEGER DEFAULT 0,
    tags TEXT[],
    total_urls INTEGER DEFAULT 0,
    completed_urls INTEGER DEFAULT 0,
    failed_urls INTEGER DEFAULT 0,
    total_contacts_found INTEGER DEFAULT 0,
    total_leads_generated INTEGER DEFAULT 0,
    completion_percentage DECIMAL DEFAULT 0,
    lead_conversion_percentage DECIMAL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create search_urls table if it doesn't exist
CREATE TABLE IF NOT EXISTS search_urls (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    notes TEXT,
    status TEXT DEFAULT 'pending',
    total_contacts_found INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

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

-- Add audience_id column to campaigns table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'campaigns' AND column_name = 'audience_id'
    ) THEN
        ALTER TABLE campaigns ADD COLUMN audience_id UUID REFERENCES audiences(id) ON DELETE SET NULL;
    END IF;
END $$;

-- Create contacts table for storing scraped leads
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

-- Create campaign_urls junction table if it doesn't exist
CREATE TABLE IF NOT EXISTS campaign_urls (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    search_url_id UUID REFERENCES search_urls(id) ON DELETE CASCADE,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(campaign_id, search_url_id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_organizations_slug ON organizations(slug);
CREATE INDEX IF NOT EXISTS idx_campaigns_org_id ON campaigns(organization_id);
CREATE INDEX IF NOT EXISTS idx_campaigns_status ON campaigns(status);
CREATE INDEX IF NOT EXISTS idx_search_urls_org_id ON search_urls(organization_id);
CREATE INDEX IF NOT EXISTS idx_search_urls_status ON search_urls(status);
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

-- Insert a default organization if none exists (for testing)
INSERT INTO organizations (name, slug, description, contact_email)
SELECT 'Default Organization', 'default-org', 'Auto-created default organization', 'admin@example.com'
WHERE NOT EXISTS (SELECT 1 FROM organizations LIMIT 1);

-- Success message
DO $$
BEGIN
    RAISE NOTICE '=== MIGRATION COMPLETED SUCCESSFULLY ===';
    RAISE NOTICE 'Tables created/updated:';
    RAISE NOTICE '- organizations: Base organization management';
    RAISE NOTICE '- campaigns: Email campaign management';
    RAISE NOTICE '- search_urls: Apollo URL storage';
    RAISE NOTICE '- audiences: Target audience segments';
    RAISE NOTICE '- audience_urls: Links audiences to URLs';
    RAISE NOTICE '- contacts: Scraped lead storage';
    RAISE NOTICE '- campaign_urls: Campaign URL relationships';
    RAISE NOTICE '';
    RAISE NOTICE 'Your database now supports audience-centric lead management!';
    RAISE NOTICE 'You can now use the Leads section to create audiences and assign them to campaigns.';
END $$;
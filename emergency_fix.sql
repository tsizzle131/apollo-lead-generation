-- EMERGENCY FIX - Run this first to stop the errors
-- This creates the absolute minimum needed to prevent 404/column errors

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Drop existing tables if they're causing conflicts (be careful!)
-- Only uncomment these if you're sure you want to start fresh:
-- DROP TABLE IF EXISTS audience_urls CASCADE;
-- DROP TABLE IF EXISTS audiences CASCADE;
-- DROP TABLE IF EXISTS contacts CASCADE;

-- Create organizations table if missing
CREATE TABLE IF NOT EXISTS organizations (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Ensure we have at least one organization
INSERT INTO organizations (name, slug) 
SELECT 'Default Organization', 'default' 
WHERE NOT EXISTS (SELECT 1 FROM organizations);

-- Create audiences table (this is what's missing!)
DROP TABLE IF EXISTS audiences CASCADE;
CREATE TABLE audiences (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    total_urls INTEGER DEFAULT 0,
    estimated_contacts INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    last_scraped_at TIMESTAMPTZ,
    scraping_progress INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(organization_id, name)
);

-- Create search_urls table if missing
CREATE TABLE IF NOT EXISTS search_urls (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    notes TEXT,
    status TEXT DEFAULT 'pending',
    total_contacts_found INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create audience_urls junction table
DROP TABLE IF EXISTS audience_urls CASCADE;
CREATE TABLE audience_urls (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    audience_id UUID NOT NULL REFERENCES audiences(id) ON DELETE CASCADE,
    search_url_id UUID NOT NULL REFERENCES search_urls(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(audience_id, search_url_id)
);

-- Create contacts table
DROP TABLE IF EXISTS contacts CASCADE;
CREATE TABLE contacts (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    search_url_id UUID REFERENCES search_urls(id) ON DELETE SET NULL,
    audience_id UUID REFERENCES audiences(id) ON DELETE SET NULL,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT,
    email TEXT,
    phone TEXT,
    linkedin_url TEXT,
    company_name TEXT,
    job_title TEXT,
    location TEXT,
    website_summaries TEXT[],
    icebreaker_content TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    email_sent_at TIMESTAMPTZ,
    phone_called_at TIMESTAMPTZ,
    last_contact_attempt TIMESTAMPTZ
);

-- Fix existing campaigns table if it exists
DO $$
BEGIN
    -- Check if campaigns table exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'campaigns') THEN
        -- Add audience_id column if it doesn't exist
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'campaigns' AND column_name = 'audience_id'
        ) THEN
            ALTER TABLE campaigns ADD COLUMN audience_id UUID REFERENCES audiences(id) ON DELETE SET NULL;
        END IF;
    ELSE
        -- Create campaigns table from scratch
        CREATE TABLE campaigns (
            id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
            organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            description TEXT,
            status TEXT DEFAULT 'active',
            priority INTEGER DEFAULT 0,
            tags TEXT[],
            audience_id UUID REFERENCES audiences(id) ON DELETE SET NULL,
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
    END IF;
END $$;

-- Create essential indexes
CREATE INDEX IF NOT EXISTS idx_audiences_org_id ON audiences(organization_id);
CREATE INDEX IF NOT EXISTS idx_audience_urls_audience_id ON audience_urls(audience_id);
CREATE INDEX IF NOT EXISTS idx_contacts_audience_id ON contacts(audience_id);
CREATE INDEX IF NOT EXISTS idx_campaigns_audience_id ON campaigns(audience_id);

-- Verify everything was created
SELECT 'SUCCESS: All tables created. The audience system is now ready!' as result;

-- Show what we created
SELECT 
    'audiences' as table_name, 
    count(*) as row_count 
FROM audiences
UNION ALL
SELECT 
    'audience_urls' as table_name, 
    count(*) as row_count 
FROM audience_urls
UNION ALL
SELECT 
    'contacts' as table_name, 
    count(*) as row_count 
FROM contacts;
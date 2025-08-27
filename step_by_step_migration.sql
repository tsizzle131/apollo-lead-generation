-- Step-by-Step Migration for Audience System
-- Run each section separately in Supabase SQL Editor to isolate any issues

-- STEP 1: Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- STEP 2: Create base tables if they don't exist
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

-- STEP 3: Create the audiences table first (before referencing it)
CREATE TABLE IF NOT EXISTS audiences (
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

-- STEP 4: Now safely add audience_id to campaigns
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS audience_id UUID REFERENCES audiences(id) ON DELETE SET NULL;

-- STEP 5: Create remaining tables
CREATE TABLE IF NOT EXISTS audience_urls (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    audience_id UUID NOT NULL REFERENCES audiences(id) ON DELETE CASCADE,
    search_url_id UUID NOT NULL REFERENCES search_urls(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(audience_id, search_url_id)
);

CREATE TABLE IF NOT EXISTS contacts (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    search_url_id UUID REFERENCES search_urls(id) ON DELETE SET NULL,
    campaign_id UUID REFERENCES campaigns(id) ON DELETE SET NULL,
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

CREATE TABLE IF NOT EXISTS campaign_urls (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    search_url_id UUID REFERENCES search_urls(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(campaign_id, search_url_id)
);

-- STEP 6: Create indexes
CREATE INDEX IF NOT EXISTS idx_campaigns_audience_id ON campaigns(audience_id);
CREATE INDEX IF NOT EXISTS idx_audiences_org_id ON audiences(organization_id);
CREATE INDEX IF NOT EXISTS idx_audience_urls_audience_id ON audience_urls(audience_id);
CREATE INDEX IF NOT EXISTS idx_contacts_audience_id ON contacts(audience_id);

-- STEP 7: Insert default organization if needed
INSERT INTO organizations (name, slug, description)
SELECT 'Default Organization', 'default-org', 'Default organization for testing'
WHERE NOT EXISTS (SELECT 1 FROM organizations);

-- STEP 8: Success message
SELECT 'Migration completed successfully! All tables created.' as result;
-- Minimal Safe Migration - Run this in Supabase SQL Editor
-- This creates only what you absolutely need for the audience system

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create organizations table (if you don't have it)
CREATE TABLE IF NOT EXISTS organizations (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert default org if none exists
INSERT INTO organizations (name, slug) 
SELECT 'Default Org', 'default' 
WHERE NOT EXISTS (SELECT 1 FROM organizations);

-- Create audiences table
CREATE TABLE IF NOT EXISTS audiences (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    total_urls INTEGER DEFAULT 0,
    estimated_contacts INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    scraping_progress INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(organization_id, name)
);

-- Create search_urls table (for Apollo URLs)
CREATE TABLE IF NOT EXISTS search_urls (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    notes TEXT,
    status TEXT DEFAULT 'pending',
    total_contacts_found INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create audience_urls junction table
CREATE TABLE IF NOT EXISTS audience_urls (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    audience_id UUID NOT NULL REFERENCES audiences(id) ON DELETE CASCADE,
    search_url_id UUID NOT NULL REFERENCES search_urls(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(audience_id, search_url_id)
);

-- Create contacts table
CREATE TABLE IF NOT EXISTS contacts (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    audience_id UUID REFERENCES audiences(id) ON DELETE SET NULL,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    company_name TEXT,
    job_title TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    phone_called_at TIMESTAMPTZ,
    last_contact_attempt TIMESTAMPTZ
);

-- Create campaigns table (if you don't have it)
CREATE TABLE IF NOT EXISTS campaigns (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT DEFAULT 'active',
    audience_id UUID REFERENCES audiences(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create basic indexes
CREATE INDEX IF NOT EXISTS idx_audiences_org ON audiences(organization_id);
CREATE INDEX IF NOT EXISTS idx_campaigns_audience ON campaigns(audience_id);
CREATE INDEX IF NOT EXISTS idx_contacts_audience ON contacts(audience_id);

SELECT 'Minimal migration completed! Audience system is ready.' as result;
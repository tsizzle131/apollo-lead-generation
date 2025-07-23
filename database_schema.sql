-- Lead Generation Database Schema for Supabase
-- This script creates the complete schema for the lead generation system

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create enum types
CREATE TYPE search_status AS ENUM ('pending', 'running', 'completed', 'failed');
CREATE TYPE lead_status AS ENUM ('new', 'contacted', 'responded', 'converted', 'rejected');

-- Table 1: Search URLs
CREATE TABLE search_urls (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    status search_status DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    total_contacts_found INTEGER DEFAULT 0,
    notes TEXT
);

-- Table 2: Raw Contacts (All Apollo/Apify data)
CREATE TABLE raw_contacts (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    search_url_id UUID REFERENCES search_urls(id) ON DELETE CASCADE,
    
    -- Apollo/Apify specific fields
    apollo_id TEXT,
    last_name TEXT,
    name TEXT, -- Full name or first name
    linkedin_url TEXT,
    title TEXT, -- Job title/headline
    email_status TEXT, -- Apollo's email confidence indicator
    photo_url TEXT,
    twitter_url TEXT,
    github_url TEXT,
    facebook_url TEXT,
    extrapolated_email_confidence FLOAT, -- Apollo's confidence score (0.0-1.0)
    headline TEXT, -- Professional headline
    email TEXT,
    organization_id TEXT, -- Apollo's organization ID
    degree TEXT,
    grade_level TEXT,
    website_url TEXT,
    
    -- System fields
    raw_data_json JSONB, -- Complete original response for future fields
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    
    -- Create unique constraint to prevent duplicates
    UNIQUE(apollo_id, search_url_id)
);

-- Table 3: Processed Leads
CREATE TABLE processed_leads (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    raw_contact_id UUID REFERENCES raw_contacts(id) ON DELETE CASCADE,
    search_url_id UUID REFERENCES search_urls(id) ON DELETE CASCADE,
    
    -- Processed contact info
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    linkedin_url TEXT,
    headline TEXT, -- Job title/professional headline
    website_url TEXT,
    location TEXT, -- Derived/processed location
    
    -- AI-generated content
    icebreaker TEXT,
    website_summaries JSONB, -- Website content summaries used for icebreaker generation
    processing_settings_used JSONB, -- AI model, temperature, prompts used
    
    -- Lead management
    status lead_status DEFAULT 'new',
    notes TEXT,
    contacted_at TIMESTAMP WITH TIME ZONE,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_raw_contacts_search_url ON raw_contacts(search_url_id);
CREATE INDEX idx_raw_contacts_email ON raw_contacts(email) WHERE email IS NOT NULL;
CREATE INDEX idx_raw_contacts_linkedin ON raw_contacts(linkedin_url) WHERE linkedin_url IS NOT NULL;
CREATE INDEX idx_raw_contacts_apollo_id ON raw_contacts(apollo_id) WHERE apollo_id IS NOT NULL;
CREATE INDEX idx_raw_contacts_processed ON raw_contacts(processed);
CREATE INDEX idx_raw_contacts_confidence ON raw_contacts(extrapolated_email_confidence) WHERE extrapolated_email_confidence IS NOT NULL;

CREATE INDEX idx_processed_leads_search_url ON processed_leads(search_url_id);
CREATE INDEX idx_processed_leads_status ON processed_leads(status);
CREATE INDEX idx_processed_leads_email ON processed_leads(email) WHERE email IS NOT NULL;
CREATE INDEX idx_processed_leads_created ON processed_leads(created_at);

CREATE INDEX idx_search_urls_status ON search_urls(status);
CREATE INDEX idx_search_urls_created ON search_urls(created_at);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_processed_leads_updated_at
    BEFORE UPDATE ON processed_leads
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create views for common queries
CREATE VIEW v_contact_pipeline AS
SELECT 
    su.url as search_url,
    su.status as search_status,
    COUNT(rc.id) as total_raw_contacts,
    COUNT(CASE WHEN rc.processed THEN 1 END) as processed_contacts,
    COUNT(pl.id) as generated_leads,
    COUNT(CASE WHEN pl.status = 'contacted' THEN 1 END) as contacted_leads,
    COUNT(CASE WHEN pl.status = 'responded' THEN 1 END) as responded_leads,
    COUNT(CASE WHEN pl.status = 'converted' THEN 1 END) as converted_leads
FROM search_urls su
LEFT JOIN raw_contacts rc ON su.id = rc.search_url_id
LEFT JOIN processed_leads pl ON rc.id = pl.raw_contact_id
GROUP BY su.id, su.url, su.status;

CREATE VIEW v_high_confidence_contacts AS
SELECT 
    rc.*,
    su.url as search_url
FROM raw_contacts rc
JOIN search_urls su ON rc.search_url_id = su.id
WHERE rc.email IS NOT NULL 
    AND rc.website_url IS NOT NULL
    AND (rc.extrapolated_email_confidence IS NULL OR rc.extrapolated_email_confidence >= 0.7)
    AND rc.processed = FALSE;

-- Row Level Security (RLS) policies
ALTER TABLE search_urls ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE processed_leads ENABLE ROW LEVEL SECURITY;

-- For now, allow all operations (you can restrict this later with proper auth)
CREATE POLICY "Allow all operations on search_urls" ON search_urls FOR ALL USING (true);
CREATE POLICY "Allow all operations on raw_contacts" ON raw_contacts FOR ALL USING (true);
CREATE POLICY "Allow all operations on processed_leads" ON processed_leads FOR ALL USING (true);

-- Grant permissions to authenticated users
GRANT ALL ON search_urls TO authenticated;
GRANT ALL ON raw_contacts TO authenticated;
GRANT ALL ON processed_leads TO authenticated;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO authenticated;

-- Insert some sample data for testing
INSERT INTO search_urls (url, status) VALUES 
('https://www.linkedin.com/search/results/people/?keywords=marketing%20director', 'pending');

COMMENT ON TABLE search_urls IS 'LinkedIn search URLs and their processing status';
COMMENT ON TABLE raw_contacts IS 'Complete raw contact data from Apollo/Apify with all available fields';
COMMENT ON TABLE processed_leads IS 'Processed leads with AI-generated icebreakers ready for outreach';
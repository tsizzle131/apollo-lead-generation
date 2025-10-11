-- Google Maps Facebook Scraper Schema
-- This creates a completely separate schema for the Google Maps scraper
-- to keep it isolated from the existing lead generation system

-- Create the schema
CREATE SCHEMA IF NOT EXISTS gmaps_scraper;

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create enum types for the schema
CREATE TYPE gmaps_scraper.campaign_status AS ENUM ('draft', 'running', 'paused', 'completed', 'failed');
CREATE TYPE gmaps_scraper.coverage_profile AS ENUM ('aggressive', 'balanced', 'budget', 'custom');
CREATE TYPE gmaps_scraper.density_level AS ENUM ('very_high', 'high', 'medium', 'low');
CREATE TYPE gmaps_scraper.enrichment_status AS ENUM ('pending', 'enriched', 'no_facebook', 'failed');

-- 1. ZIP Codes table with density information
CREATE TABLE gmaps_scraper.zip_codes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    zip_code VARCHAR(10) UNIQUE NOT NULL,
    city VARCHAR(100),
    state VARCHAR(2),
    county VARCHAR(100),
    neighborhood VARCHAR(100),
    density_level gmaps_scraper.density_level,
    expected_businesses INTEGER DEFAULT 250,
    actual_businesses INTEGER,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    population INTEGER,
    median_income INTEGER,
    business_categories JSONB, -- Track which business types are common here
    last_scraped_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Campaigns table
CREATE TABLE gmaps_scraper.campaigns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    keywords TEXT[], -- Array of search keywords
    location VARCHAR(255), -- e.g., "Los Angeles, CA"
    coverage_profile gmaps_scraper.coverage_profile DEFAULT 'balanced',
    custom_zip_codes TEXT[], -- For custom profile
    status gmaps_scraper.campaign_status DEFAULT 'draft',
    
    -- Coverage settings
    target_zip_count INTEGER,
    actual_zip_count INTEGER,
    coverage_percentage DECIMAL(5,2),
    
    -- Cost tracking
    estimated_cost DECIMAL(10,2),
    actual_cost DECIMAL(10,2),
    google_maps_cost DECIMAL(10,2),
    facebook_cost DECIMAL(10,2),
    
    -- Results
    total_businesses_found INTEGER DEFAULT 0,
    total_emails_found INTEGER DEFAULT 0,
    total_facebook_pages_found INTEGER DEFAULT 0,
    
    -- Metadata
    organization_id UUID, -- Link to existing organizations table if needed
    created_by VARCHAR(255),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Campaign ZIP code selections
CREATE TABLE gmaps_scraper.campaign_coverage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    campaign_id UUID NOT NULL REFERENCES gmaps_scraper.campaigns(id) ON DELETE CASCADE,
    zip_code VARCHAR(10) NOT NULL,
    
    -- Search configuration
    keywords TEXT[], -- Keywords to search in this ZIP
    max_results INTEGER DEFAULT 250,
    
    -- Status tracking
    scraped BOOLEAN DEFAULT FALSE,
    scraped_at TIMESTAMPTZ,
    businesses_found INTEGER DEFAULT 0,
    emails_found INTEGER DEFAULT 0,
    
    -- Cost tracking for this ZIP
    estimated_cost DECIMAL(10,2),
    actual_cost DECIMAL(10,2),
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(campaign_id, zip_code)
);

-- 4. Google Maps businesses table
CREATE TABLE gmaps_scraper.businesses (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    campaign_id UUID REFERENCES gmaps_scraper.campaigns(id) ON DELETE CASCADE,
    zip_code VARCHAR(10),
    
    -- Google Maps data
    place_id VARCHAR(255) UNIQUE,
    name VARCHAR(255) NOT NULL,
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(2),
    postal_code VARCHAR(10),
    country VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    
    -- Contact information
    phone VARCHAR(50),
    website TEXT,
    email VARCHAR(255), -- Direct email if found on Google Maps
    
    -- Business details
    category VARCHAR(255),
    categories TEXT[],
    description TEXT,
    rating DECIMAL(2,1),
    reviews_count INTEGER,
    price_level VARCHAR(10),
    hours JSONB,
    
    -- Social media
    facebook_url TEXT,
    instagram_url TEXT,
    twitter_url TEXT,
    linkedin_url TEXT,
    
    -- Enrichment status
    needs_enrichment BOOLEAN DEFAULT TRUE,
    enrichment_status gmaps_scraper.enrichment_status DEFAULT 'pending',
    enrichment_attempts INTEGER DEFAULT 0,
    last_enrichment_attempt TIMESTAMPTZ,
    
    -- Raw data
    raw_data JSONB,
    
    -- Metadata
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. Facebook enrichment results
CREATE TABLE gmaps_scraper.facebook_enrichments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    business_id UUID NOT NULL REFERENCES gmaps_scraper.businesses(id) ON DELETE CASCADE,
    campaign_id UUID REFERENCES gmaps_scraper.campaigns(id) ON DELETE CASCADE,
    
    -- Facebook page data
    facebook_url TEXT,
    page_name VARCHAR(255),
    page_likes INTEGER,
    page_followers INTEGER,
    
    -- Extracted emails
    emails TEXT[], -- Array of emails found
    primary_email VARCHAR(255), -- Best email to use
    email_sources TEXT[], -- Where each email was found (about, contact, etc.)
    
    -- Additional contact info
    phone_numbers TEXT[],
    addresses TEXT[],
    
    -- Metadata
    success BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    raw_data JSONB,
    
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 6. API costs tracking
CREATE TABLE gmaps_scraper.api_costs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    campaign_id UUID REFERENCES gmaps_scraper.campaigns(id) ON DELETE CASCADE,
    
    -- Cost details
    service VARCHAR(50) NOT NULL, -- 'google_maps' or 'facebook'
    actor_name VARCHAR(255),
    run_id VARCHAR(255),
    
    -- Metrics
    items_processed INTEGER,
    cost_usd DECIMAL(10,4),
    credits_used INTEGER,
    
    -- Performance
    duration_seconds INTEGER,
    success_rate DECIMAL(5,2),
    
    -- Metadata
    metadata JSONB,
    incurred_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 7. Campaign analytics view
CREATE OR REPLACE VIEW gmaps_scraper.campaign_analytics AS
SELECT 
    c.id,
    c.name,
    c.status,
    c.coverage_profile,
    c.target_zip_count,
    c.actual_zip_count,
    c.coverage_percentage,
    
    -- Costs
    c.estimated_cost,
    c.actual_cost,
    CASE 
        WHEN c.total_emails_found > 0 
        THEN c.actual_cost / c.total_emails_found 
        ELSE NULL 
    END as cost_per_email,
    
    -- Results
    c.total_businesses_found,
    c.total_emails_found,
    c.total_facebook_pages_found,
    
    -- Success rates
    CASE 
        WHEN c.total_businesses_found > 0 
        THEN (c.total_emails_found::DECIMAL / c.total_businesses_found * 100)
        ELSE 0 
    END as email_success_rate,
    
    CASE 
        WHEN c.total_businesses_found > 0 
        THEN (c.total_facebook_pages_found::DECIMAL / c.total_businesses_found * 100)
        ELSE 0 
    END as facebook_match_rate,
    
    -- Timing
    c.created_at,
    c.started_at,
    c.completed_at,
    CASE 
        WHEN c.completed_at IS NOT NULL AND c.started_at IS NOT NULL
        THEN EXTRACT(EPOCH FROM (c.completed_at - c.started_at)) / 60
        ELSE NULL
    END as duration_minutes
FROM gmaps_scraper.campaigns c;

-- 8. ZIP code performance view
CREATE OR REPLACE VIEW gmaps_scraper.zip_performance AS
SELECT 
    z.zip_code,
    z.city,
    z.state,
    z.density_level,
    z.expected_businesses,
    COUNT(DISTINCT b.id) as actual_businesses,
    COUNT(DISTINCT CASE WHEN b.email IS NOT NULL OR fe.primary_email IS NOT NULL THEN b.id END) as businesses_with_email,
    AVG(b.rating) as avg_business_rating,
    
    -- Success metrics
    CASE 
        WHEN COUNT(DISTINCT b.id) > 0 
        THEN (COUNT(DISTINCT CASE WHEN b.email IS NOT NULL OR fe.primary_email IS NOT NULL THEN b.id END)::DECIMAL / COUNT(DISTINCT b.id) * 100)
        ELSE 0 
    END as email_success_rate
    
FROM gmaps_scraper.zip_codes z
LEFT JOIN gmaps_scraper.businesses b ON b.postal_code = z.zip_code
LEFT JOIN gmaps_scraper.facebook_enrichments fe ON fe.business_id = b.id
GROUP BY z.zip_code, z.city, z.state, z.density_level, z.expected_businesses;

-- Create indexes for performance
CREATE INDEX idx_businesses_campaign_id ON gmaps_scraper.businesses(campaign_id);
CREATE INDEX idx_businesses_place_id ON gmaps_scraper.businesses(place_id);
CREATE INDEX idx_businesses_zip_code ON gmaps_scraper.businesses(postal_code);
CREATE INDEX idx_businesses_enrichment_status ON gmaps_scraper.businesses(enrichment_status);
CREATE INDEX idx_businesses_needs_enrichment ON gmaps_scraper.businesses(needs_enrichment);

CREATE INDEX idx_facebook_enrichments_business_id ON gmaps_scraper.facebook_enrichments(business_id);
CREATE INDEX idx_facebook_enrichments_campaign_id ON gmaps_scraper.facebook_enrichments(campaign_id);

CREATE INDEX idx_campaign_coverage_campaign_id ON gmaps_scraper.campaign_coverage(campaign_id);
CREATE INDEX idx_campaign_coverage_zip_code ON gmaps_scraper.campaign_coverage(zip_code);

CREATE INDEX idx_api_costs_campaign_id ON gmaps_scraper.api_costs(campaign_id);
CREATE INDEX idx_api_costs_service ON gmaps_scraper.api_costs(service);

-- Enable Row Level Security (RLS) if needed
ALTER TABLE gmaps_scraper.campaigns ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmaps_scraper.businesses ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmaps_scraper.facebook_enrichments ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmaps_scraper.campaign_coverage ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmaps_scraper.api_costs ENABLE ROW LEVEL SECURITY;

-- Create update timestamp trigger function
CREATE OR REPLACE FUNCTION gmaps_scraper.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply update trigger to tables
CREATE TRIGGER update_campaigns_updated_at BEFORE UPDATE ON gmaps_scraper.campaigns
    FOR EACH ROW EXECUTE FUNCTION gmaps_scraper.update_updated_at_column();

CREATE TRIGGER update_businesses_updated_at BEFORE UPDATE ON gmaps_scraper.businesses
    FOR EACH ROW EXECUTE FUNCTION gmaps_scraper.update_updated_at_column();

CREATE TRIGGER update_campaign_coverage_updated_at BEFORE UPDATE ON gmaps_scraper.campaign_coverage
    FOR EACH ROW EXECUTE FUNCTION gmaps_scraper.update_updated_at_column();

CREATE TRIGGER update_zip_codes_updated_at BEFORE UPDATE ON gmaps_scraper.zip_codes
    FOR EACH ROW EXECUTE FUNCTION gmaps_scraper.update_updated_at_column();

-- Grant permissions (adjust as needed for your Supabase setup)
GRANT USAGE ON SCHEMA gmaps_scraper TO anon, authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA gmaps_scraper TO anon, authenticated;
GRANT ALL ON ALL SEQUENCES IN SCHEMA gmaps_scraper TO anon, authenticated;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA gmaps_scraper TO anon, authenticated;
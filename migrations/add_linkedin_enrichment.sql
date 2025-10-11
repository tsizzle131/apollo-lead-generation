-- LinkedIn Enrichment Tables Migration
-- Adds LinkedIn enrichment and email verification capabilities to the gmaps_scraper schema

-- 1. Add LinkedIn URL to businesses table if not exists
ALTER TABLE gmaps_scraper.businesses
ADD COLUMN IF NOT EXISTS linkedin_url TEXT,
ADD COLUMN IF NOT EXISTS linkedin_enriched BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS linkedin_enriched_at TIMESTAMPTZ;

-- 2. Create LinkedIn enrichments table
CREATE TABLE IF NOT EXISTS gmaps_scraper.linkedin_enrichments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    business_id UUID NOT NULL REFERENCES gmaps_scraper.businesses(id) ON DELETE CASCADE,
    campaign_id UUID REFERENCES gmaps_scraper.campaigns(id) ON DELETE CASCADE,

    -- LinkedIn profile data
    linkedin_url TEXT NOT NULL,
    profile_type VARCHAR(20) NOT NULL CHECK (profile_type IN ('company', 'personal', 'unknown')),

    -- Person details
    person_name VARCHAR(255),
    person_title VARCHAR(500),
    person_profile_url TEXT,
    company_name VARCHAR(255),
    location VARCHAR(255),
    connections INTEGER,

    -- Email data
    emails_found TEXT[], -- Emails directly found on LinkedIn
    emails_generated TEXT[], -- Emails generated from name + domain
    primary_email VARCHAR(255), -- Best email to use
    email_source VARCHAR(50), -- 'linkedin_direct', 'generated', etc.

    -- Bouncer verification results
    email_verified BOOLEAN DEFAULT FALSE,
    bouncer_status VARCHAR(50), -- 'deliverable', 'undeliverable', 'risky', 'unknown'
    bouncer_score DECIMAL(5,2), -- 0-100 score
    bouncer_reason TEXT,
    is_safe BOOLEAN DEFAULT FALSE, -- Safe to use for outreach
    is_disposable BOOLEAN DEFAULT FALSE,
    is_role_based BOOLEAN DEFAULT FALSE,
    is_free_email BOOLEAN DEFAULT FALSE,
    bouncer_verified_at TIMESTAMPTZ,

    -- Phone data
    phone VARCHAR(50),

    -- Metadata
    success BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    raw_profile_data JSONB,
    bouncer_raw_response JSONB,

    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Create email verification log table (track all verifications)
CREATE TABLE IF NOT EXISTS gmaps_scraper.email_verifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    business_id UUID REFERENCES gmaps_scraper.businesses(id) ON DELETE CASCADE,
    linkedin_enrichment_id UUID REFERENCES gmaps_scraper.linkedin_enrichments(id) ON DELETE CASCADE,

    -- Email being verified
    email VARCHAR(255) NOT NULL,

    -- Verification results
    status VARCHAR(50) NOT NULL, -- 'deliverable', 'undeliverable', 'risky', 'unknown', 'error'
    score DECIMAL(5,2),
    is_safe BOOLEAN DEFAULT FALSE,

    -- Detailed flags
    is_disposable BOOLEAN,
    is_role_based BOOLEAN,
    is_free_email BOOLEAN,
    is_gibberish BOOLEAN,

    -- Technical details
    domain VARCHAR(255),
    provider VARCHAR(255),
    mx_records BOOLEAN,
    smtp_check BOOLEAN,

    -- Error/suggestion
    reason TEXT,
    suggestion VARCHAR(255), -- Did you mean suggestion

    -- Raw response
    raw_response JSONB,

    -- Timestamps
    verified_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_businesses_linkedin_url ON gmaps_scraper.businesses(linkedin_url);
CREATE INDEX IF NOT EXISTS idx_businesses_linkedin_enriched ON gmaps_scraper.businesses(linkedin_enriched);
CREATE INDEX IF NOT EXISTS idx_linkedin_enrichments_business_id ON gmaps_scraper.linkedin_enrichments(business_id);
CREATE INDEX IF NOT EXISTS idx_linkedin_enrichments_campaign_id ON gmaps_scraper.linkedin_enrichments(campaign_id);
CREATE INDEX IF NOT EXISTS idx_linkedin_enrichments_primary_email ON gmaps_scraper.linkedin_enrichments(primary_email);
CREATE INDEX IF NOT EXISTS idx_linkedin_enrichments_bouncer_status ON gmaps_scraper.linkedin_enrichments(bouncer_status);
CREATE INDEX IF NOT EXISTS idx_email_verifications_email ON gmaps_scraper.email_verifications(email);
CREATE INDEX IF NOT EXISTS idx_email_verifications_status ON gmaps_scraper.email_verifications(status);

-- 5. Create function to update business linkedin_enriched status
CREATE OR REPLACE FUNCTION gmaps_scraper.update_business_linkedin_status()
RETURNS TRIGGER AS $$
BEGIN
    -- Update the business record when LinkedIn enrichment is added
    UPDATE gmaps_scraper.businesses
    SET
        linkedin_enriched = TRUE,
        linkedin_enriched_at = NEW.created_at,
        linkedin_url = COALESCE(linkedin_url, NEW.linkedin_url),
        updated_at = NOW()
    WHERE id = NEW.business_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 6. Create trigger for auto-updating business status
DROP TRIGGER IF EXISTS update_business_linkedin_status_trigger ON gmaps_scraper.linkedin_enrichments;
CREATE TRIGGER update_business_linkedin_status_trigger
AFTER INSERT ON gmaps_scraper.linkedin_enrichments
FOR EACH ROW
EXECUTE FUNCTION gmaps_scraper.update_business_linkedin_status();

-- 7. Update campaigns table to track LinkedIn enrichment stats
ALTER TABLE gmaps_scraper.campaigns
ADD COLUMN IF NOT EXISTS total_linkedin_profiles_found INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS total_verified_emails INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS linkedin_enrichment_cost DECIMAL(10,2) DEFAULT 0,
ADD COLUMN IF NOT EXISTS bouncer_verification_cost DECIMAL(10,2) DEFAULT 0;

-- 8. Create view for enrichment status overview
CREATE OR REPLACE VIEW gmaps_scraper.enrichment_overview AS
SELECT
    c.id as campaign_id,
    c.name as campaign_name,
    COUNT(DISTINCT b.id) as total_businesses,
    COUNT(DISTINCT b.id) FILTER (WHERE b.facebook_url IS NOT NULL) as with_facebook,
    COUNT(DISTINCT b.id) FILTER (WHERE b.linkedin_url IS NOT NULL) as with_linkedin,
    COUNT(DISTINCT le.id) as linkedin_profiles_found,
    COUNT(DISTINCT le.id) FILTER (WHERE le.bouncer_status = 'deliverable') as deliverable_emails,
    COUNT(DISTINCT le.id) FILTER (WHERE le.bouncer_status = 'risky') as risky_emails,
    COUNT(DISTINCT le.id) FILTER (WHERE le.bouncer_status = 'undeliverable') as undeliverable_emails,
    COUNT(DISTINCT le.id) FILTER (WHERE le.is_safe = TRUE) as safe_emails,
    AVG(le.bouncer_score) as avg_email_score
FROM gmaps_scraper.campaigns c
LEFT JOIN gmaps_scraper.businesses b ON b.campaign_id = c.id
LEFT JOIN gmaps_scraper.linkedin_enrichments le ON le.business_id = b.id
GROUP BY c.id, c.name;

-- 9. Create function to get best email for a business
CREATE OR REPLACE FUNCTION gmaps_scraper.get_best_email_for_business(business_uuid UUID)
RETURNS TABLE (
    email VARCHAR(255),
    source VARCHAR(50),
    status VARCHAR(50),
    score DECIMAL(5,2),
    is_safe BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COALESCE(le.primary_email, fe.primary_email, b.email) as email,
        CASE
            WHEN le.primary_email IS NOT NULL THEN 'linkedin'
            WHEN fe.primary_email IS NOT NULL THEN 'facebook'
            ELSE 'google_maps'
        END as source,
        COALESCE(le.bouncer_status, 'unverified') as status,
        le.bouncer_score as score,
        COALESCE(le.is_safe, FALSE) as is_safe
    FROM gmaps_scraper.businesses b
    LEFT JOIN gmaps_scraper.linkedin_enrichments le ON le.business_id = b.id
    LEFT JOIN gmaps_scraper.facebook_enrichments fe ON fe.business_id = b.id
    WHERE b.id = business_uuid
    ORDER BY
        le.is_safe DESC NULLS LAST,
        le.bouncer_score DESC NULLS LAST
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- 10. Add comment documentation
COMMENT ON TABLE gmaps_scraper.linkedin_enrichments IS 'Stores LinkedIn profile data and email verification results for businesses';
COMMENT ON TABLE gmaps_scraper.email_verifications IS 'Log of all email verification attempts through Bouncer API';
COMMENT ON COLUMN gmaps_scraper.linkedin_enrichments.profile_type IS 'Type of LinkedIn page: company (business page) or personal (individual profile)';
COMMENT ON COLUMN gmaps_scraper.linkedin_enrichments.bouncer_status IS 'Email deliverability status from Bouncer: deliverable, undeliverable, risky, or unknown';
COMMENT ON COLUMN gmaps_scraper.linkedin_enrichments.is_safe IS 'Whether email is safe to use for outreach (deliverable + high score)';
COMMENT ON FUNCTION gmaps_scraper.get_best_email_for_business IS 'Returns the best verified email for a business, prioritizing LinkedIn > Facebook > Google Maps';
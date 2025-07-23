-- Add missing columns to existing processed_leads table
-- Run this in your Supabase SQL Editor to fix the schema

ALTER TABLE processed_leads 
ADD COLUMN IF NOT EXISTS linkedin_url TEXT,
ADD COLUMN IF NOT EXISTS headline TEXT,
ADD COLUMN IF NOT EXISTS website_summaries JSONB;

-- Optional: Add indexes for the new columns
CREATE INDEX IF NOT EXISTS idx_processed_leads_linkedin ON processed_leads(linkedin_url) WHERE linkedin_url IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_processed_leads_headline ON processed_leads(headline) WHERE headline IS NOT NULL;
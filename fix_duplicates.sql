-- Fix duplicate processed leads issue
-- Run this in your Supabase SQL Editor

-- 1. First, clean up existing duplicates by keeping only the latest entry for each raw_contact_id
DELETE FROM processed_leads 
WHERE id NOT IN (
    SELECT DISTINCT ON (raw_contact_id) id 
    FROM processed_leads 
    ORDER BY raw_contact_id, created_at DESC
);

-- 2. Add unique constraint to prevent future duplicates
ALTER TABLE processed_leads 
ADD CONSTRAINT unique_raw_contact_id 
UNIQUE (raw_contact_id);

-- 3. Create index for better performance  
CREATE INDEX IF NOT EXISTS idx_processed_leads_raw_contact_id 
ON processed_leads(raw_contact_id);

-- 4. Verify the cleanup worked
SELECT 'Duplicate check complete. Run this to verify:' as message;
SELECT raw_contact_id, COUNT(*) as count 
FROM processed_leads 
GROUP BY raw_contact_id 
HAVING COUNT(*) > 1;
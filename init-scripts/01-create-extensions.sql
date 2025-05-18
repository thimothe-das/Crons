-- Enable PostgreSQL extensions that might be useful for geographic data
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- For text search

-- Create a function to check if the data is already imported
CREATE OR REPLACE FUNCTION check_data_imported() 
RETURNS boolean AS $$
DECLARE
    table_count integer;
BEGIN
    SELECT COUNT(*) INTO table_count FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'dvf_data';
    
    RETURN table_count > 0;
END;
$$ LANGUAGE plpgsql; 
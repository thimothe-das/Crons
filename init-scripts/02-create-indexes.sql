-- Create additional indexes for faster queries on dvf_data table

-- Ensure basic indexes are created (these are already in the data importer)
CREATE INDEX IF NOT EXISTS idx_id_parcelle ON dvf_data (id_parcelle);
CREATE INDEX IF NOT EXISTS idx_type_local ON dvf_data (type_local);
CREATE INDEX IF NOT EXISTS idx_date_mutation ON dvf_data (date_mutation);
CREATE INDEX IF NOT EXISTS idx_code_postal ON dvf_data (code_postal);

-- Additional indexes for query optimization

-- Create a composite index for the most common filter combination: type_local + surface_reelle_bati
-- This will help with queries filtering both apartment type and surface area
CREATE INDEX IF NOT EXISTS idx_type_local_surface ON dvf_data (type_local, surface_reelle_bati);

-- Create indexes for price range filtering
CREATE INDEX IF NOT EXISTS idx_valeur_fonciere ON dvf_data (valeur_fonciere);

-- Create a composite index for postal code and property type
-- Common filtering pattern seen in the code
CREATE INDEX IF NOT EXISTS idx_code_postal_type_local ON dvf_data (code_postal, type_local);

-- Create an index on commune names for filtering by location
CREATE INDEX IF NOT EXISTS idx_nom_commune ON dvf_data (nom_commune);

-- Create a partial index for Apartments, which seems to be the most commonly queried type
CREATE INDEX IF NOT EXISTS idx_apartments ON dvf_data (id_parcelle, valeur_fonciere, surface_reelle_bati) 
WHERE type_local = 'Appartement';

-- Create a functional index for price per square meter calculations
-- This can speed up queries that use price/m² for filtering or sorting
CREATE INDEX IF NOT EXISTS idx_prix_m2 ON dvf_data ((valeur_fonciere / NULLIF(surface_reelle_bati, 0)))
WHERE surface_reelle_bati > 0;

-- Add index for address searching, using trigram index for partial matching
CREATE INDEX IF NOT EXISTS idx_adresse_nom_voie_trgm ON dvf_data USING gin (adresse_nom_voie gin_trgm_ops);

-- Create a multi-column index for advanced filtering combinations
-- This covers the fields commonly filtered together in the queries
CREATE INDEX IF NOT EXISTS idx_combined_filters ON dvf_data 
(type_local, code_postal, surface_reelle_bati, valeur_fonciere);

-- Add an index on mutation ID used for joins and lookups
CREATE INDEX IF NOT EXISTS idx_id_mutation ON dvf_data (id_mutation); 
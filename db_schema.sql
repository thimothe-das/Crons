-- DVF Database Schema for Property Transaction Data
-- Optimized for low-resource servers

-- Create the main DVF data table
CREATE TABLE IF NOT EXISTS dvf_data (
    id SERIAL PRIMARY KEY,
    id_mutation VARCHAR(50),
    date_mutation DATE,
    numero_disposition VARCHAR(20),
    nature_mutation VARCHAR(50),
    valeur_fonciere NUMERIC(12,2),
    adresse_numero TEXT,
    adresse_suffixe TEXT,
    adresse_nom_voie TEXT,
    adresse_code_voie TEXT,
    code_postal VARCHAR(5),
    code_commune VARCHAR(10),
    nom_commune VARCHAR(100),
    code_departement VARCHAR(3),
    ancien_code_commune VARCHAR(10),
    ancien_nom_commune VARCHAR(100),
    id_parcelle VARCHAR(50),
    ancien_id_parcelle VARCHAR(50),
    numero_volume TEXT,
    lot1_numero TEXT,
    lot1_surface_carrez NUMERIC(12,2),
    lot2_numero TEXT,
    lot2_surface_carrez NUMERIC(12,2),
    lot3_numero TEXT,
    lot3_surface_carrez NUMERIC(12,2),
    lot4_numero TEXT,
    lot4_surface_carrez NUMERIC(12,2),
    lot5_numero TEXT,
    lot5_surface_carrez NUMERIC(12,2),
    nombre_lots TEXT,
    code_type_local VARCHAR(5),
    type_local VARCHAR(50),
    surface_reelle_bati NUMERIC(12,2),
    nombre_pieces_principales TEXT,
    code_nature_culture VARCHAR(5),
    nature_culture VARCHAR(100),
    code_nature_culture_speciale VARCHAR(5),
    nature_culture_speciale VARCHAR(100),
    surface_terrain NUMERIC(12,2),
    longitude NUMERIC(10,6),
    latitude NUMERIC(10,6),
    
    -- Add metadata columns for tracking imports
    import_year SMALLINT,
    import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Add unique constraint for deduplication
    CONSTRAINT unique_transaction UNIQUE (id_mutation, numero_disposition, id_parcelle, lot1_numero)
);

-- Create optimized indexes for common queries
-- Primary lookup indexes
CREATE INDEX IF NOT EXISTS idx_dvf_date_mutation ON dvf_data(date_mutation);
CREATE INDEX IF NOT EXISTS idx_dvf_code_postal ON dvf_data(code_postal);
CREATE INDEX IF NOT EXISTS idx_dvf_nom_commune ON dvf_data(nom_commune);
CREATE INDEX IF NOT EXISTS idx_dvf_type_local ON dvf_data(type_local);

-- Price and surface indexes for filtering
CREATE INDEX IF NOT EXISTS idx_dvf_valeur_fonciere ON dvf_data(valeur_fonciere) WHERE valeur_fonciere > 0;
CREATE INDEX IF NOT EXISTS idx_dvf_surface_bati ON dvf_data(surface_reelle_bati) WHERE surface_reelle_bati > 0;

-- Geographic indexes
CREATE INDEX IF NOT EXISTS idx_dvf_coordinates ON dvf_data(longitude, latitude) WHERE longitude IS NOT NULL AND latitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dvf_code_departement ON dvf_data(code_departement);

-- Import tracking indexes
CREATE INDEX IF NOT EXISTS idx_dvf_import_year ON dvf_data(import_year);
CREATE INDEX IF NOT EXISTS idx_dvf_import_date ON dvf_data(import_date);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_dvf_type_date ON dvf_data(type_local, date_mutation) WHERE valeur_fonciere > 0;
CREATE INDEX IF NOT EXISTS idx_dvf_postal_type ON dvf_data(code_postal, type_local) WHERE valeur_fonciere > 0;
CREATE INDEX IF NOT EXISTS idx_dvf_commune_type ON dvf_data(nom_commune, type_local) WHERE valeur_fonciere > 0;

-- Performance optimization settings are configured via docker-compose environment variables
-- No ALTER SYSTEM commands needed here

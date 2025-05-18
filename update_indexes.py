#!/usr/bin/env python3

import os
import time
import argparse
from sqlalchemy import create_engine, text

def main():
    parser = argparse.ArgumentParser(description='Create indexes on DVF database for query optimization')
    parser.add_argument('--host', default=os.environ.get('POSTGRES_HOST', 'localhost'),
                        help='PostgreSQL host (default: localhost or POSTGRES_HOST env var)')
    parser.add_argument('--port', default=os.environ.get('POSTGRES_PORT', '5432'),
                        help='PostgreSQL port (default: 5432 or POSTGRES_PORT env var)')
    parser.add_argument('--user', default=os.environ.get('POSTGRES_USER', 'dvf_user'),
                        help='PostgreSQL username (default: dvf_user or POSTGRES_USER env var)')
    parser.add_argument('--password', default=os.environ.get('POSTGRES_PASSWORD', 'dvf_password'),
                        help='PostgreSQL password (default: dvf_password or POSTGRES_PASSWORD env var)')
    parser.add_argument('--db', default=os.environ.get('POSTGRES_DB', 'dvf_data'),
                        help='PostgreSQL database name (default: dvf_data or POSTGRES_DB env var)')
    parser.add_argument('--table', default='dvf_data',
                        help='Table name to index (default: dvf_data)')
    
    args = parser.parse_args()
    
    # Database connection URI
    db_uri = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}"
    
    print(f"Connecting to PostgreSQL at {args.host}:{args.port}, database {args.db}...")
    
    try:
        # Create database engine
        engine = create_engine(db_uri)
        
        # Connect and check if table exists
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT to_regclass('public.{args.table}')"))
            table_exists = result.scalar() is not None
            
            if not table_exists:
                print(f"Error: Table '{args.table}' does not exist in the database.")
                return 1
            
            print(f"Found table '{args.table}', creating indexes...")
            
            # Start measuring time
            start_time = time.time()
            
            # Create all indexes (using IF NOT EXISTS to avoid errors if already present)
            index_commands = [
                # Basic indexes
                f"CREATE INDEX IF NOT EXISTS idx_id_parcelle ON {args.table} (id_parcelle)",
                f"CREATE INDEX IF NOT EXISTS idx_type_local ON {args.table} (type_local)",
                f"CREATE INDEX IF NOT EXISTS idx_date_mutation ON {args.table} (date_mutation)",
                f"CREATE INDEX IF NOT EXISTS idx_code_postal ON {args.table} (code_postal)",
                
                # Composite index for common filter combination
                f"CREATE INDEX IF NOT EXISTS idx_type_local_surface ON {args.table} (type_local, surface_reelle_bati)",
                
                # Index for price filtering
                f"CREATE INDEX IF NOT EXISTS idx_valeur_fonciere ON {args.table} (valeur_fonciere)",
                
                # Composite index for postal code and property type
                f"CREATE INDEX IF NOT EXISTS idx_code_postal_type_local ON {args.table} (code_postal, type_local)",
                
                # Index on commune names
                f"CREATE INDEX IF NOT EXISTS idx_nom_commune ON {args.table} (nom_commune)",
                
                # Partial index for Apartments (most common query)
                f"CREATE INDEX IF NOT EXISTS idx_apartments ON {args.table} (id_parcelle, valeur_fonciere, surface_reelle_bati) WHERE type_local = 'Appartement'",
                
                # Price per square meter functional index
                f"CREATE INDEX IF NOT EXISTS idx_prix_m2 ON {args.table} ((valeur_fonciere / NULLIF(surface_reelle_bati, 0))) WHERE surface_reelle_bati > 0",
                
                # Address search using trigram index - first check if pg_trgm extension is available
                f"CREATE EXTENSION IF NOT EXISTS pg_trgm",
                f"CREATE INDEX IF NOT EXISTS idx_adresse_nom_voie_trgm ON {args.table} USING gin (adresse_nom_voie gin_trgm_ops)",
                
                # Multi-column index for combined filtering
                f"CREATE INDEX IF NOT EXISTS idx_combined_filters ON {args.table} (type_local, code_postal, surface_reelle_bati, valeur_fonciere)",
                
                # Index on mutation ID
                f"CREATE INDEX IF NOT EXISTS idx_id_mutation ON {args.table} (id_mutation)"
            ]
            
            # Execute all index creation commands
            for idx, cmd in enumerate(index_commands):
                try:
                    print(f"Creating index {idx+1}/{len(index_commands)}...")
                    conn.execute(text(cmd))
                    print(f"  Success!")
                except Exception as e:
                    print(f"  Error creating index: {str(e)}")
            
            # Calculate elapsed time
            elapsed_time = time.time() - start_time
            print(f"\nIndex creation completed in {elapsed_time:.2f} seconds")
            
            # Analyze the table to update statistics
            print("\nRunning ANALYZE to update statistics...")
            conn.execute(text(f"ANALYZE {args.table}"))
            print("Statistics updated successfully")
            
            print("\nAll operations completed successfully!")
            return 0
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main()) 
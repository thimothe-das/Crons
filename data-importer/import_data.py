#!/usr/bin/env python3
import os
import time
import pandas as pd
import requests
import io
import gzip
from sqlalchemy import create_engine, text
from tqdm import tqdm
import numpy as np

# Configuration from environment variables
DB_USER = os.environ.get('POSTGRES_USER', 'dvf_user')
DB_PASS = os.environ.get('POSTGRES_PASSWORD', 'dvf_password')
DB_NAME = os.environ.get('POSTGRES_DB', 'dvf_data')
DB_HOST = os.environ.get('POSTGRES_HOST', 'postgres')

# Base URL for DVF data
BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv"

# Years to import
YEARS_TO_IMPORT = os.environ.get('YEARS_TO_IMPORT', '2020,2021,2022,2023,2024').split(',')

# Database connection string
DB_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

def wait_for_postgres(max_retries=10, retry_interval=5):
    """Wait for PostgreSQL to be ready"""
    print("Waiting for PostgreSQL to be ready...")
    engine = create_engine(DB_URI)
    
    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                print("PostgreSQL is ready!")
                return True
        except Exception as e:
            print(f"Attempt {i+1}/{max_retries}: PostgreSQL not ready yet. Error: {str(e)}")
            if i < max_retries - 1:
                print(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
    
    raise Exception("Could not connect to PostgreSQL")

def download_and_process_csv(year):
    """Download and process the gzipped CSV file for a specific year"""
    url = f"{BASE_URL}/{year}/full.csv.gz"
    print(f"Downloading data for year {year} from {url}...")
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Get total file size for progress bar
        total_size = int(response.headers.get('content-length', 0))
        
        # Download with progress bar
        chunks = []
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {year}") as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    chunks.append(chunk)
                    pbar.update(len(chunk))
        
        content = b''.join(chunks)
        
        print(f"Decompressing gzipped data for {year}...")
        csv_data = gzip.decompress(content)
        print(f"Data for {year} decompressed successfully")
        return csv_data
    except Exception as e:
        print(f"Error downloading or processing data for {year}: {str(e)}")
        print(f"Skipping year {year}")
        return None

def prepare_database():
    """Prepare the database by creating the table if it doesn't exist"""
    engine = create_engine(DB_URI)
    
    # Check if table exists
    with engine.connect() as conn:
        result = conn.execute(text("SELECT to_regclass('public.dvf_data')"))
        table_exists = result.scalar() is not None
    
    if not table_exists:
        print("Creating dvf_data table with improved schema...")
        # Create table with more comprehensive structure
        with engine.connect() as conn:
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dvf_data (
                id SERIAL PRIMARY KEY,
                id_mutation VARCHAR(255),
                date_mutation DATE,
                id_parcelle VARCHAR(255),
                valeur_fonciere NUMERIC,
                type_local VARCHAR(255),
                surface_reelle_bati NUMERIC,
                nombre_pieces_principales NUMERIC,
                surface_terrain NUMERIC,
                code_postal VARCHAR(5),
                code_commune VARCHAR(10),
                nom_commune VARCHAR(255),
                code_departement VARCHAR(5),
                code_type_local VARCHAR(10),
                type_local_detail VARCHAR(255),
                adresse_nom_voie VARCHAR(255),
                adresse_numero VARCHAR(50)
            )
            """))
            
            print("Table created successfully with comprehensive schema")
    else:
        print("Table dvf_data already exists")
        
        # Check for and add commonly missing columns
        missing_columns = {
            'code_postal': 'VARCHAR(5)',
            'surface_reelle_bati': 'NUMERIC',
            'nombre_pieces_principales': 'NUMERIC',
            'code_commune': 'VARCHAR(10)',
            'nom_commune': 'VARCHAR(255)',
            'code_departement': 'VARCHAR(5)',
            'adresse_nom_voie': 'VARCHAR(255)',
            'adresse_numero': 'VARCHAR(50)'
        }
        
        try:
            with engine.connect() as conn:
                # Get existing columns
                result = conn.execute(text("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'dvf_data'
                """))
                existing_columns = [row[0] for row in result.fetchall()]
                
                # Add missing columns
                for column_name, column_type in missing_columns.items():
                    if column_name not in existing_columns:
                        print(f"Adding {column_name} column to existing table...")
                        conn.execute(text(f"ALTER TABLE dvf_data ADD COLUMN {column_name} {column_type}"))
                        print(f"Column {column_name} added successfully")
        except Exception as e:
            print(f"Error checking/adding columns: {str(e)}")
    
    return table_exists

def import_data_to_postgres(csv_data, year, table_exists):
    """Import the CSV data into PostgreSQL for a specific year"""
    if csv_data is None:
        return
    
    print(f"Parsing CSV data for {year}...")
    df = pd.read_csv(io.BytesIO(csv_data), low_memory=False)
    
    print(f"CSV data for {year} loaded. Shape: {df.shape}")
    
    # Create SQLAlchemy engine
    engine = create_engine(DB_URI)
    
    # Convert column names to lowercase and replace spaces with underscores
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    # Handle string columns that should not be converted to numeric
    string_columns = ['code_postal', 'code_commune', 'code_departement', 'id_parcelle', 'id_mutation', 'nom_commune', 'adresse_nom_voie', 'adresse_numero']
    
    # Force string columns to be strings, not floats
    for col in string_columns:
        if col in df.columns:
            print(f"Ensuring {col} is treated as string...")
            # Convert to string and handle NaN values
            df[col] = df[col].astype(str)
            # Replace 'nan' strings with empty string
            df[col] = df[col].replace('nan', '')
            
            # For postal codes, ensure 5 digit format with leading zeros
            if col == 'code_postal':
                print("Formatting postal codes to 5 digits...")
                # Filter to only numeric strings
                mask = df[col].str.match(r'^\d+\.?\d*$', na=False)
                if mask.any():
                    # Convert numeric postal codes to 5-digit format with leading zeros
                    df.loc[mask, col] = df.loc[mask, col].apply(
                        lambda x: str(int(float(x))).zfill(5) if x and x != 'nan' else ''
                    )
    
    # Extract code_postal from code_commune if it exists and code_postal is empty
    if 'code_commune' in df.columns:
        print("Checking postal codes from commune codes...")
        # Fill missing postal codes with department code + "000"
        missing_mask = (df['code_postal'] == '') | df['code_postal'].isna()
        if missing_mask.any() and 'code_departement' in df.columns:
            print(f"Filling {missing_mask.sum()} missing postal codes from department codes...")
            df.loc[missing_mask, 'code_postal'] = df.loc[missing_mask, 'code_departement'].apply(
                lambda x: x.zfill(2) + "000" if x and x.isdigit() else ""
            )
        elif missing_mask.any():
            # Extract from commune code if department code not available
            print(f"Filling {missing_mask.sum()} missing postal codes from commune codes...")
            df.loc[missing_mask, 'code_postal'] = df.loc[missing_mask, 'code_commune'].apply(
                lambda x: x[:2].zfill(2) + "000" if x and len(x) >= 2 else ""
            )
    
    # Clean and convert numeric columns to avoid conversion errors
    print("Cleaning numeric columns...")
    numeric_columns = [
        'valeur_fonciere', 
        'surface_reelle_bati', 
        'nombre_pieces_principales',
        'surface_terrain'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            # Try to convert to numeric, forcing non-numeric values to NaN
            print(f"Converting {col} to numeric...")
            
            # If column is object type (string), clean it first
            if df[col].dtype == 'object':
                # Remove non-numeric characters and replace commas with periods
                df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                # Use regex to extract only valid numeric patterns
                df[col] = df[col].str.extract(r'([-+]?\d*\.?\d+)')[0]
            
            # Convert to numeric, coercing errors to NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Replace any infinite values with NaN
    df = df.replace([np.inf, -np.inf], np.nan)
    
    # Print column types for debugging
    print("\nColumn data types after cleaning:")
    for col, dtype in df.dtypes.items():
        print(f"  {col}: {dtype}")
    
    # Import data in chunks to handle large files
    chunk_size = 10000
    total_rows = len(df)
    
    print(f"Importing data for {year} to PostgreSQL...")
    
    # Count successful and failed chunks
    successful_chunks = 0
    failed_chunks = 0
    total_imported_rows = 0
    
    with tqdm(total=total_rows, desc=f"Importing {year}") as pbar:
        for i in range(0, total_rows, chunk_size):
            end = min(i + chunk_size, total_rows)
            chunk = df.iloc[i:end]
            
            try:
                # Write to PostgreSQL (append if table already has data)
                if_exists = 'append' if table_exists or i > 0 else 'replace'
                chunk.to_sql('dvf_data', engine, if_exists=if_exists, index=False)
                
                successful_chunks += 1
                total_imported_rows += len(chunk)
            except Exception as e:
                failed_chunks += 1
                print(f"\nError importing chunk {i//chunk_size + 1}: {str(e)}")
                # Try to continue with next chunk
            
            pbar.update(len(chunk))
    
    print(f"\nImport summary for {year}:")
    print(f"  Total chunks: {(total_rows + chunk_size - 1) // chunk_size}")
    print(f"  Successful chunks: {successful_chunks}")
    print(f"  Failed chunks: {failed_chunks}")
    print(f"  Total rows imported: {total_imported_rows} of {total_rows}")
    
    if total_imported_rows > 0:
        print(f"Data import for {year} completed with {successful_chunks} successful chunks!")
        return True
    else:
        print(f"Data import for {year} failed completely!")
        return False

def create_indices():
    """Create indices for faster queries"""
    print("Creating indices...")
    engine = create_engine(DB_URI)
    
    with engine.connect() as conn:
        # Basic indexes
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_id_parcelle ON dvf_data (id_parcelle)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_type_local ON dvf_data (type_local)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_date_mutation ON dvf_data (date_mutation)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_code_postal ON dvf_data (code_postal)"))
        
        # Additional indexes for improved query performance
        print("Creating additional performance indexes...")
        
        # Composite index for common filter combination
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_type_local_surface ON dvf_data (type_local, surface_reelle_bati)"))
        
        # Index for price filtering
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_valeur_fonciere ON dvf_data (valeur_fonciere)"))
        
        # Composite index for postal code and property type
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_code_postal_type_local ON dvf_data (code_postal, type_local)"))
        
        # Index on commune names
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_nom_commune ON dvf_data (nom_commune)"))
        
        # Partial index for Apartments (most common query)
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_apartments ON dvf_data (id_parcelle, valeur_fonciere, surface_reelle_bati) WHERE type_local = 'Appartement'"))
        
        # Price per square meter functional index
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_prix_m2 ON dvf_data ((valeur_fonciere / NULLIF(surface_reelle_bati, 0))) WHERE surface_reelle_bati > 0"))
        
        # Address search using trigram index
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_adresse_nom_voie_trgm ON dvf_data USING gin (adresse_nom_voie gin_trgm_ops)"))
        
        # Multi-column index for combined filtering
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_combined_filters ON dvf_data (type_local, code_postal, surface_reelle_bati, valeur_fonciere)"))
        
        # Index on mutation ID
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_id_mutation ON dvf_data (id_mutation)"))
    
    print("All indices created successfully")

def main():
    """Main function to orchestrate the import process"""
    try:
        # Wait for PostgreSQL to be ready
        wait_for_postgres()
        
        # Prepare database
        table_exists = prepare_database()
        
        # Import data for each year
        for year in YEARS_TO_IMPORT:
            year = year.strip()
            print(f"\n{'='*50}")
            print(f"Processing year {year}")
            print(f"{'='*50}")
            
            # Download and process the CSV data for this year
            csv_data = download_and_process_csv(year)
            
            # Import the data to PostgreSQL
            if csv_data:
                success = import_data_to_postgres(csv_data, year, table_exists)
                if success:
                    # If at least one year imported successfully, table exists for next year
                    table_exists = True
        
        # Create indices after all imports
        create_indices()
        
        print("\nMulti-year import process completed successfully!")
    
    except Exception as e:
        print(f"Error during import process: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main() 
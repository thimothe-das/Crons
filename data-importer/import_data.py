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
import gc
import psutil

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

# Memory management settings
CHUNK_SIZE = int(os.environ.get('CSV_CHUNK_SIZE', '5000'))  # Smaller chunks for memory efficiency
DOWNLOAD_CHUNK_SIZE = int(os.environ.get('DOWNLOAD_CHUNK_SIZE', '8192'))

def get_memory_usage():
    """Get current memory usage in MB"""
    try:
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024  # Convert to MB
    except:
        return 0

def log_memory(operation=""):
    """Log current memory usage"""
    memory_mb = get_memory_usage()
    print(f"Memory usage {operation}: {memory_mb:.1f} MB")
    return memory_mb

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

def stream_download_and_process_csv(year):
    """Stream download and process CSV data without loading entire file into memory"""
    url = f"{BASE_URL}/{year}/full.csv.gz"
    print(f"Streaming data for year {year} from {url}...")
    
    log_memory("before download")
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Get total file size for progress bar
        total_size = int(response.headers.get('content-length', 0))
        
        print(f"Starting streaming decompression for {year}...")
        
        # Create a streaming gzip decompressor
        decompressor = gzip.GzipFile(fileobj=response.raw)
        
        # Read the header first to determine column structure
        header_line = decompressor.readline().decode('utf-8').strip()
        columns = [col.lower().replace(' ', '_') for col in header_line.split(',')]
        
        print(f"Detected {len(columns)} columns for {year}")
        log_memory("after header read")
        
        # Return the decompressor and metadata for streaming processing
        return {
            'decompressor': decompressor,
            'columns': columns,
            'total_size': total_size,
            'response': response  # Keep reference to prevent closure
        }
        
    except Exception as e:
        print(f"Error setting up streaming for {year}: {str(e)}")
        return None

def process_csv_chunk(chunk_data, columns):
    """Process a chunk of CSV data with proper type conversion"""
    try:
        # Create DataFrame from chunk
        lines = chunk_data.strip().split('\n')
        if not lines or not lines[0]:
            return None
            
        # Parse CSV lines
        rows = []
        for line in lines:
            if line.strip():
                # Simple CSV parsing (assumes no commas in quoted fields)
                row = [field.strip().strip('"') for field in line.split(',')]
                if len(row) == len(columns):
                    rows.append(row)
        
        if not rows:
            return None
            
        df = pd.DataFrame(rows, columns=columns)
        
        # Handle string columns that should not be converted to numeric
        string_columns = ['code_postal', 'code_commune', 'code_departement', 'id_parcelle', 'id_mutation', 'nom_commune', 'adresse_nom_voie', 'adresse_numero']
        
        # Force string columns to be strings, not floats
        for col in string_columns:
            if col in df.columns:
                # Convert to string and handle NaN values
                df[col] = df[col].astype(str)
                # Replace 'nan' strings with empty string
                df[col] = df[col].replace('nan', '')
                
                # For postal codes, ensure 5 digit format with leading zeros
                if col == 'code_postal':
                    # Filter to only numeric strings
                    mask = df[col].str.match(r'^\d+\.?\d*$', na=False)
                    if mask.any():
                        # Convert numeric postal codes to 5-digit format with leading zeros
                        df.loc[mask, col] = df.loc[mask, col].apply(
                            lambda x: str(int(float(x))).zfill(5) if x and x != 'nan' else ''
                        )
        
        # Extract code_postal from code_commune if it exists and code_postal is empty
        if 'code_commune' in df.columns and 'code_postal' in df.columns:
            # Fill missing postal codes with department code + "000"
            missing_mask = (df['code_postal'] == '') | df['code_postal'].isna()
            if missing_mask.any() and 'code_departement' in df.columns:
                df.loc[missing_mask, 'code_postal'] = df.loc[missing_mask, 'code_departement'].apply(
                    lambda x: x.zfill(2) + "000" if x and x.isdigit() else ""
                )
            elif missing_mask.any():
                # Extract from commune code if department code not available
                df.loc[missing_mask, 'code_postal'] = df.loc[missing_mask, 'code_commune'].apply(
                    lambda x: x[:2].zfill(2) + "000" if x and len(x) >= 2 else ""
                )
        
        # Clean and convert numeric columns
        numeric_columns = [
            'valeur_fonciere', 
            'surface_reelle_bati', 
            'nombre_pieces_principales',
            'surface_terrain'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
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
        
        return df
        
    except Exception as e:
        print(f"Error processing chunk: {str(e)}")
        return None

def import_streaming_data_to_postgres(stream_info, year, table_exists):
    """Import CSV data using streaming to minimize memory usage"""
    if not stream_info:
        return False
    
    print(f"Starting streaming import for {year}...")
    log_memory("before streaming import")
    
    engine = create_engine(DB_URI)
    decompressor = stream_info['decompressor']
    columns = stream_info['columns']
    
    chunk_count = 0
    total_imported_rows = 0
    successful_chunks = 0
    failed_chunks = 0
    
    try:
        # Read and process data in chunks
        chunk_buffer = ""
        lines_in_chunk = 0
        
        with tqdm(desc=f"Streaming {year}", unit="rows") as pbar:
            while True:
                try:
                    # Read a line from the decompressed stream
                    line = decompressor.readline()
                    if not line:  # End of file
                        break
                        
                    line = line.decode('utf-8', errors='ignore').strip()
                    if not line:
                        continue
                    
                    chunk_buffer += line + '\n'
                    lines_in_chunk += 1
                    
                    # Process chunk when we have enough lines
                    if lines_in_chunk >= CHUNK_SIZE:
                        df_chunk = process_csv_chunk(chunk_buffer, columns)
                        if df_chunk is not None and len(df_chunk) > 0:
                            try:
                                # Write to PostgreSQL
                                if_exists = 'append' if table_exists or chunk_count > 0 else 'replace'
                                df_chunk.to_sql('dvf_data', engine, if_exists=if_exists, index=False)
                                
                                successful_chunks += 1
                                total_imported_rows += len(df_chunk)
                                pbar.update(len(df_chunk))
                                
                            except Exception as e:
                                failed_chunks += 1
                                print(f"\nError importing chunk {chunk_count + 1}: {str(e)}")
                        
                        # Clear chunk buffer and force garbage collection
                        chunk_buffer = ""
                        lines_in_chunk = 0
                        chunk_count += 1
                        del df_chunk
                        gc.collect()
                        
                        # Log memory usage periodically
                        if chunk_count % 10 == 0:
                            memory_mb = log_memory(f"after chunk {chunk_count}")
                            # If memory usage is getting high, force more aggressive cleanup
                            if memory_mb > 400:  # 400MB threshold
                                print("High memory usage detected, performing aggressive cleanup...")
                                gc.collect()
                                
                except Exception as e:
                    print(f"Error reading stream: {str(e)}")
                    break
        
        # Process remaining data in buffer
        if chunk_buffer and lines_in_chunk > 0:
            df_chunk = process_csv_chunk(chunk_buffer, columns)
            if df_chunk is not None and len(df_chunk) > 0:
                try:
                    if_exists = 'append' if table_exists or chunk_count > 0 else 'replace'
                    df_chunk.to_sql('dvf_data', engine, if_exists=if_exists, index=False)
                    successful_chunks += 1
                    total_imported_rows += len(df_chunk)
                    pbar.update(len(df_chunk))
                except Exception as e:
                    failed_chunks += 1
                    print(f"\nError importing final chunk: {str(e)}")
                    
                del df_chunk
                gc.collect()
        
    except Exception as e:
        print(f"Error during streaming import: {str(e)}")
        return False
    
    finally:
        # Clean up resources
        try:
            decompressor.close()
            stream_info['response'].close()
        except:
            pass
        gc.collect()
    
    print(f"\nStreaming import summary for {year}:")
    print(f"  Total chunks processed: {chunk_count + 1}")
    print(f"  Successful chunks: {successful_chunks}")
    print(f"  Failed chunks: {failed_chunks}")
    print(f"  Total rows imported: {total_imported_rows}")
    
    log_memory("after streaming import complete")
    
    return total_imported_rows > 0

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
    total_years_processed = 0
    successful_years = 0
    
    try:
        print(f"Starting DVF data import with streaming optimization")
        log_memory("at startup")
        
        # Wait for PostgreSQL to be ready
        wait_for_postgres()
        
        # Prepare database
        table_exists = prepare_database()
        
        # Import data for each year
        for year in YEARS_TO_IMPORT:
            year = year.strip()
            total_years_processed += 1
            
            print(f"\n{'='*50}")
            print(f"Processing year {year} ({total_years_processed}/{len(YEARS_TO_IMPORT)})")
            print(f"{'='*50}")
            
            try:
                # Download and process the CSV data for this year
                stream_info = stream_download_and_process_csv(year)
                
                # Import the data to PostgreSQL
                if stream_info:
                    success = import_streaming_data_to_postgres(stream_info, year, table_exists)
                    if success:
                        successful_years += 1
                        # If at least one year imported successfully, table exists for next year
                        table_exists = True
                        print(f"✓ Year {year} completed successfully")
                    else:
                        print(f"✗ Year {year} failed to import")
                else:
                    print(f"✗ Year {year} failed to download/stream")
                
                # Force cleanup between years
                gc.collect()
                log_memory(f"after processing year {year}")
                
            except Exception as year_error:
                print(f"✗ Error processing year {year}: {str(year_error)}")
                # Continue with next year instead of failing completely
                continue
        
        # Create indices after all imports if we have any data
        if successful_years > 0:
            print(f"\nCreating database indices...")
            create_indices()
            print("Database indices created successfully")
        
        # Final cleanup
        gc.collect()
        log_memory("at completion")
        
        print(f"\n{'='*60}")
        print(f"IMPORT PROCESS COMPLETED")
        print(f"Years processed: {total_years_processed}")
        print(f"Years successful: {successful_years}")
        print(f"Years failed: {total_years_processed - successful_years}")
        print(f"{'='*60}")
        
        if successful_years == 0:
            print("ERROR: No years were successfully imported!")
            exit(1)
        elif successful_years < total_years_processed:
            print(f"WARNING: Only {successful_years}/{total_years_processed} years imported successfully")
            exit(2)  # Partial success
        else:
            print("SUCCESS: All years imported successfully!")
            exit(0)
    
    except Exception as e:
        print(f"CRITICAL ERROR during import process: {str(e)}")
        log_memory("at error")
        import traceback
        traceback.print_exc()
        exit(1)
    
    finally:
        # Final cleanup
        print("Performing final cleanup...")
        gc.collect()
        print("Import process terminated.")

if __name__ == "__main__":
    main() 
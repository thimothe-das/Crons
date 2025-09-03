#!/usr/bin/env python3
"""
DVF Data Importer - Multi-year property transaction data import
Optimized for low-resource servers with PostgreSQL Docker setup
"""

import os
import sys
import gzip
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import argparse
import logging
from datetime import datetime
import time
from io import BytesIO, StringIO
from tqdm import tqdm
import signal

# Database configuration from environment variables
DB_USER = os.environ.get('POSTGRES_USER', 'dvf_user')
DB_PASS = os.environ.get('POSTGRES_PASSWORD', 'dvf_password')
DB_NAME = os.environ.get('POSTGRES_DB', 'dvf_data')
DB_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
DB_PORT = os.environ.get('POSTGRES_PORT', '5432')

# Configure logging
try:
    os.makedirs('logs', exist_ok=True)
    log_handlers = [
        logging.FileHandler('logs/dvf_import.log'),
        logging.StreamHandler()
    ]
except PermissionError:
    # Fall back to console-only logging if file logging fails
    print("Warning: Cannot create log file, using console logging only")
    log_handlers = [logging.StreamHandler()]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=log_handlers
)
logger = logging.getLogger(__name__)

class DVFImporter:
    def __init__(self, base_url_template="https://files.data.gouv.fr/geo-dvf/latest/csv/{year}", 
                 chunk_size=10000, max_memory_mb=128):
        """
        Initialize DVF data importer
        
        Args:
            base_url_template (str): URL template with {year} placeholder
            chunk_size (int): Number of rows to process at once
            max_memory_mb (int): Maximum memory usage in MB
        """
        self.base_url_template = base_url_template
        self.chunk_size = chunk_size
        self.max_memory_mb = max_memory_mb
        self.connection = None
        self.stop_import = False
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Received shutdown signal. Finishing current chunk...")
        self.stop_import = True
        
    def connect_to_database(self):
        """Connect to PostgreSQL database with retry logic"""
        connection_configs = [
            {'host': DB_HOST, 'port': DB_PORT},
            {'host': 'localhost', 'port': DB_PORT},
            {'host': '127.0.0.1', 'port': DB_PORT}
        ]
        
        for config in connection_configs:
            try:
                logger.info(f"Attempting connection to {config['host']}:{config['port']}")
                self.connection = psycopg2.connect(
                    host=config['host'],
                    port=config['port'],
                    user=DB_USER,
                    password=DB_PASS,
                    database=DB_NAME,
                    connect_timeout=10
                )
                self.connection.autocommit = False
                logger.info(f"✅ Connected to database at {config['host']}:{config['port']}")
                return True
                
            except Exception as e:
                logger.warning(f"❌ Connection failed for {config['host']}: {str(e)}")
                continue
                
        logger.error("Failed to connect to database. Make sure Docker containers are running.")
        return False
        
    def initialize_database(self):
        """Initialize database schema"""
        if not self.connection:
            if not self.connect_to_database():
                return False
                
        try:
            with self.connection.cursor() as cursor:
                # Read and execute schema file
                with open('db_schema.sql', 'r') as f:
                    schema_sql = f.read()
                    
                logger.info("Creating database schema...")
                cursor.execute(schema_sql)
                self.connection.commit()
                logger.info("✅ Database schema initialized successfully")
                return True
                
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            self.connection.rollback()
            return False
            
    def download_year_data(self, year):
        """
        Download and stream process data for a specific year
        
        Args:
            year (int): Year to download (e.g., 2020, 2021)
            
        Returns:
            bool: Success status
        """
        url = self.base_url_template.format(year=year) + "/full.csv.gz"
        logger.info(f"Starting download for year {year} from {url}")
        
        try:
            # Stream download with progress tracking
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Get file size for progress tracking
            total_size = int(response.headers.get('content-length', 0))
            logger.info(f"Downloading {total_size / (1024*1024):.1f} MB for year {year}")
            
            # Download with progress bar
            downloaded_data = BytesIO()
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {year}") as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if self.stop_import:
                        logger.info("Import stopped by user")
                        return False
                    downloaded_data.write(chunk)
                    pbar.update(len(chunk))
            
            # Reset stream position
            downloaded_data.seek(0)
            
            # Process the downloaded gzipped data
            return self.process_gzipped_data(downloaded_data, year)
            
        except requests.RequestException as e:
            logger.error(f"Error downloading data for year {year}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error processing year {year}: {str(e)}")
            return False
            
    def process_gzipped_data(self, gzipped_data, year):
        """
        Process gzipped CSV data with true streaming to avoid memory issues
        
        Args:
            gzipped_data (BytesIO): Gzipped CSV data
            year (int): Year being processed
            
        Returns:
            bool: Success status
        """
        try:
            logger.info(f"Starting streaming processing for year {year}")
            
            # Use streaming decompression instead of loading entire file
            gzipped_data.seek(0)
            
            total_rows = 0
            successful_rows = 0
            chunk_data = []
            
            # Stream process the gzipped file line by line
            with gzip.GzipFile(fileobj=gzipped_data, mode='rb') as gz_file:
                # Read header line
                header_line_bytes = gz_file.readline()
                if not header_line_bytes:
                    logger.error("Empty file or no header found")
                    return False
                    
                header_line = header_line_bytes.decode('utf-8').strip()
                
                # Parse header using proper CSV parsing
                import csv
                csv_reader = csv.reader([header_line])
                headers = next(csv_reader)
                logger.info(f"Found {len(headers)} columns in CSV")
                
                # Process data line by line in chunks
                chunk_num = 0
                for line_num, line_bytes in enumerate(gz_file):
                    if self.stop_import:
                        logger.info("Import stopped by user")
                        break
                    
                    try:
                        line = line_bytes.decode('utf-8').strip()
                    except UnicodeDecodeError:
                        logger.warning(f"Unicode decode error on line {line_num}, skipping")
                        continue
                        
                    if not line:
                        continue
                        
                    # Parse CSV line using proper CSV parsing
                    try:
                        import csv
                        csv_reader = csv.reader([line])
                        values = next(csv_reader)
                        if len(values) == len(headers):
                            row_dict = dict(zip(headers, values))
                            row_dict['import_year'] = year
                            chunk_data.append(row_dict)
                    except Exception as e:
                        logger.warning(f"Error parsing line {line_num}: {e}")
                        continue
                    
                    # Process chunk when it reaches the desired size
                    if len(chunk_data) >= self.chunk_size:
                        chunk_start_time = time.time()
                        
                        # Convert to DataFrame for processing
                        chunk_df = pd.DataFrame(chunk_data)
                        rows_inserted = self.insert_chunk(chunk_df, year, chunk_num)
                        
                        successful_rows += rows_inserted
                        total_rows += len(chunk_data)
                        
                        chunk_time = time.time() - chunk_start_time
                        logger.info(f"Year {year} - Chunk {chunk_num}: {rows_inserted}/{len(chunk_data)} rows inserted in {chunk_time:.2f}s")
                        
                        # Clear chunk data and force garbage collection
                        chunk_data = []
                        del chunk_df
                        import gc
                        gc.collect()
                        
                        chunk_num += 1
                
                # Process remaining data in final chunk
                if chunk_data and not self.stop_import:
                    chunk_start_time = time.time()
                    chunk_df = pd.DataFrame(chunk_data)
                    rows_inserted = self.insert_chunk(chunk_df, year, chunk_num)
                    successful_rows += rows_inserted
                    total_rows += len(chunk_data)
                    
                    chunk_time = time.time() - chunk_start_time
                    logger.info(f"Year {year} - Final chunk {chunk_num}: {rows_inserted}/{len(chunk_data)} rows inserted in {chunk_time:.2f}s")
            
            logger.info(f"✅ Year {year} completed: {successful_rows}/{total_rows} rows imported")
            return successful_rows > 0
            
        except Exception as e:
            logger.error(f"Error processing gzipped data for year {year}: {str(e)}")
            return False
            
    def insert_chunk(self, chunk, year, chunk_num):
        """
        Insert a chunk of data into the database with error handling
        
        Args:
            chunk (pd.DataFrame): Data chunk to insert
            year (int): Year being processed
            chunk_num (int): Chunk number for logging
            
        Returns:
            int: Number of rows successfully inserted
        """
        if not self.connection:
            logger.error("No database connection available")
            return 0
            
        try:
            # Clean and validate data
            chunk_clean = self.clean_chunk_data(chunk)
            
            if len(chunk_clean) == 0:
                logger.warning(f"No valid data in chunk {chunk_num} for year {year}")
                return 0
            
            # Prepare data for bulk insert
            columns = list(chunk_clean.columns)
            data_tuples = [tuple(row) for row in chunk_clean.values]
            
            # Create parameterized INSERT statement for execute_values
            columns_str = ','.join(columns)
            insert_query = f"""
                INSERT INTO dvf_data ({columns_str}) 
                VALUES %s
                ON CONFLICT (id_mutation, numero_disposition, id_parcelle, lot1_numero) 
                DO NOTHING
            """
            
            # Execute bulk insert
            with self.connection.cursor() as cursor:
                execute_values(
                    cursor, 
                    insert_query, 
                    data_tuples,
                    template=None,  # Let execute_values create the template
                    page_size=1000  # Smaller page size for memory efficiency
                )
                
                rows_affected = cursor.rowcount
                self.connection.commit()
                
                return rows_affected
                
        except Exception as e:
            logger.error(f"Error inserting chunk {chunk_num} for year {year}: {str(e)}")
            self.connection.rollback()
            return 0
            
    def clean_chunk_data(self, chunk):
        """
        Clean and validate chunk data before database insertion
        
        Args:
            chunk (pd.DataFrame): Raw data chunk
            
        Returns:
            pd.DataFrame: Cleaned data chunk
        """
        try:
            # Make a copy to avoid modifying original
            clean_chunk = chunk.copy()
            
            # Replace empty strings and 'NaN' strings with None
            clean_chunk = clean_chunk.replace(['', 'NaN', 'nan', 'NULL'], None)
            
            # Convert date_mutation to proper date format
            if 'date_mutation' in clean_chunk.columns:
                clean_chunk['date_mutation'] = pd.to_datetime(
                    clean_chunk['date_mutation'], 
                    errors='coerce'
                ).dt.date
            
            # Convert numeric columns with proper handling of large numbers
            numeric_columns = [
                'valeur_fonciere', 'surface_reelle_bati', 'surface_terrain',
                'longitude', 'latitude',
                'lot1_surface_carrez', 'lot2_surface_carrez', 'lot3_surface_carrez',
                'lot4_surface_carrez', 'lot5_surface_carrez'
            ]
            
            for col in numeric_columns:
                if col in clean_chunk.columns:
                    # Convert to numeric, handling large numbers
                    clean_chunk[col] = pd.to_numeric(clean_chunk[col], errors='coerce')
                    
                    # Cap values at reasonable maximums to prevent overflow
                    if col == 'valeur_fonciere':
                        # Cap property values at reasonable maximum (100 million euros)
                        clean_chunk[col] = clean_chunk[col].where(
                            (clean_chunk[col] >= 0) & (clean_chunk[col] <= 100000000), 
                            None
                        )
                    elif col in ['surface_reelle_bati', 'surface_terrain', 'lot1_surface_carrez', 'lot2_surface_carrez', 'lot3_surface_carrez', 'lot4_surface_carrez', 'lot5_surface_carrez']:
                        # Cap surface at reasonable maximum (10 million m² for very large properties)
                        clean_chunk[col] = clean_chunk[col].where(
                            (clean_chunk[col] >= 0) & (clean_chunk[col] <= 10000000), 
                            None
                        )
            
            # Replace remaining NaN values with None for PostgreSQL compatibility
            clean_chunk = clean_chunk.where(pd.notnull(clean_chunk), None)
            
            # Filter out rows with invalid essential data
            before_count = len(clean_chunk)
            clean_chunk = clean_chunk[
                (clean_chunk['id_mutation'].notna()) & 
                (clean_chunk['date_mutation'].notna())
            ]
            after_count = len(clean_chunk)
            
            if before_count != after_count:
                logger.warning(f"Filtered out {before_count - after_count} rows with missing essential data")
            
            return clean_chunk
            
        except Exception as e:
            logger.error(f"Error cleaning chunk data: {str(e)}")
            return pd.DataFrame()  # Return empty dataframe on error
            
    def get_import_status(self):
        """Get current import status from database"""
        if not self.connection:
            return {}
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        import_year,
                        COUNT(*) as record_count,
                        MIN(import_date) as first_import,
                        MAX(import_date) as last_import
                    FROM dvf_data 
                    GROUP BY import_year 
                    ORDER BY import_year DESC
                """)
                
                results = cursor.fetchall()
                status = {}
                for row in results:
                    status[row[0]] = {
                        'record_count': row[1],
                        'first_import': row[2],
                        'last_import': row[3]
                    }
                return status
                
        except Exception as e:
            logger.error(f"Error getting import status: {str(e)}")
            return {}
            
    def import_year_range(self, start_year, end_year):
        """
        Import data for a range of years
        
        Args:
            start_year (int): Starting year (inclusive)
            end_year (int): Ending year (inclusive)
            
        Returns:
            dict: Import results by year
        """
        if not self.connection:
            if not self.connect_to_database():
                return {}
                
        results = {}
        total_start_time = time.time()
        
        logger.info(f"Starting import for years {start_year} to {end_year}")
        
        for year in range(start_year, end_year + 1):
            if self.stop_import:
                logger.info("Import stopped by user")
                break
                
            year_start_time = time.time()
            logger.info(f"Processing year {year}...")
            
            # Check if year already imported
            status = self.get_import_status()
            if year in status:
                logger.info(f"Year {year} already imported ({status[year]['record_count']} records)")
                results[year] = {'status': 'already_imported', 'records': status[year]['record_count']}
                continue
            
            # Import the year
            success = self.download_year_data(year)
            year_time = time.time() - year_start_time
            
            if success:
                # Get final count for this year
                final_status = self.get_import_status()
                record_count = final_status.get(year, {}).get('record_count', 0)
                results[year] = {
                    'status': 'success', 
                    'records': record_count,
                    'time_seconds': round(year_time, 2)
                }
                logger.info(f"✅ Year {year} imported successfully: {record_count} records in {year_time:.2f}s")
            else:
                results[year] = {
                    'status': 'failed',
                    'time_seconds': round(year_time, 2)
                }
                logger.error(f"❌ Year {year} import failed after {year_time:.2f}s")
        
        total_time = time.time() - total_start_time
        logger.info(f"Import completed in {total_time:.2f}s")
        
        return results
        
    def clear_year_data(self, year):
        """Remove all data for a specific year"""
        if not self.connection:
            return False
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("DELETE FROM dvf_data WHERE import_year = %s", (year,))
                rows_deleted = cursor.rowcount
                self.connection.commit()
                logger.info(f"Deleted {rows_deleted} records for year {year}")
                return True
                
        except Exception as e:
            logger.error(f"Error clearing year {year}: {str(e)}")
            self.connection.rollback()
            return False
            
    def get_database_stats(self):
        """Get database statistics"""
        if not self.connection:
            return None
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT import_year) as years_imported,
                        MIN(date_mutation) as earliest_date,
                        MAX(date_mutation) as latest_date,
                        COUNT(*) FILTER (WHERE type_local = 'Appartement') as apartments,
                        COUNT(*) FILTER (WHERE type_local = 'Maison') as houses,
                        pg_size_pretty(pg_total_relation_size('dvf_data')) as table_size
                    FROM dvf_data
                """)
                
                result = cursor.fetchone()
                if result:
                    return {
                        'total_records': result[0],
                        'years_imported': result[1],
                        'earliest_date': result[2],
                        'latest_date': result[3],
                        'apartments': result[4],
                        'houses': result[5],
                        'table_size': result[6]
                    }
                return None
                
        except Exception as e:
            logger.error(f"Error getting database stats: {str(e)}")
            return None
            
    def close_connection(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")


def main():
    parser = argparse.ArgumentParser(description='Import DVF property transaction data')
    parser.add_argument('--start-year', type=int, default=2020, 
                       help='Starting year for import (default: 2020)')
    parser.add_argument('--end-year', type=int, default=2024,
                       help='Ending year for import (default: 2024)')
    parser.add_argument('--base-url', type=str, 
                       default="https://files.data.gouv.fr/geo-dvf/latest/csv/{year}",
                       help='Base URL template with {year} placeholder')
    parser.add_argument('--chunk-size', type=int, default=10000,
                       help='Number of rows to process at once (default: 10000)')
    parser.add_argument('--max-memory', type=int, default=128,
                       help='Maximum memory usage in MB (default: 128)')
    parser.add_argument('--init-db', action='store_true',
                       help='Initialize database schema before import')
    parser.add_argument('--clear-year', type=int,
                       help='Clear all data for a specific year before import')
    parser.add_argument('--stats', action='store_true',
                       help='Show database statistics')
    parser.add_argument('--status', action='store_true',
                       help='Show import status by year')
    
    args = parser.parse_args()
    
    # Create importer instance
    importer = DVFImporter(
        base_url_template=args.base_url,
        chunk_size=args.chunk_size,
        max_memory_mb=args.max_memory
    )
    
    try:
        # Connect to database
        if not importer.connect_to_database():
            logger.error("Failed to connect to database. Exiting.")
            return 1
        
        # Initialize database if requested
        if args.init_db:
            if not importer.initialize_database():
                logger.error("Failed to initialize database. Exiting.")
                return 1
        
        # Clear specific year if requested
        if args.clear_year:
            importer.clear_year_data(args.clear_year)
            
        # Show statistics if requested
        if args.stats:
            stats = importer.get_database_stats()
            if stats:
                print("\n📊 Database Statistics:")
                print("=" * 50)
                print(f"Total records: {stats['total_records']:,}")
                print(f"Years imported: {stats['years_imported']}")
                print(f"Date range: {stats['earliest_date']} to {stats['latest_date']}")
                print(f"Apartments: {stats['apartments']:,}")
                print(f"Houses: {stats['houses']:,}")
                print(f"Table size: {stats['table_size']}")
            return 0
            
        # Show import status if requested
        if args.status:
            status = importer.get_import_status()
            if status:
                print("\n📈 Import Status by Year:")
                print("=" * 60)
                for year in sorted(status.keys(), reverse=True):
                    info = status[year]
                    print(f"Year {year}: {info['record_count']:,} records (imported {info['last_import']})")
            else:
                print("No data imported yet")
            return 0
        
        # Perform the import
        logger.info(f"Starting DVF import for years {args.start_year}-{args.end_year}")
        results = importer.import_year_range(args.start_year, args.end_year)
        
        # Print summary
        print("\n📋 Import Summary:")
        print("=" * 50)
        total_records = 0
        for year, result in results.items():
            status_emoji = "✅" if result['status'] == 'success' else "⚠️" if result['status'] == 'already_imported' else "❌"
            records = result.get('records', 0)
            time_info = f" ({result.get('time_seconds', 0)}s)" if 'time_seconds' in result else ""
            print(f"{status_emoji} Year {year}: {records:,} records{time_info}")
            total_records += records
            
        print(f"\nTotal records in database: {total_records:,}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("Import interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return 1
    finally:
        importer.close_connection()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

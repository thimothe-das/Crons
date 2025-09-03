#!/usr/bin/env python3
"""
Test script for DVF importer using local example.csv file
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
import gzip
from io import StringIO

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dvf_importer import DVFImporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestDVFImporter(DVFImporter):
    """Test version of DVF importer that works with local files"""
    
    def __init__(self, test_file="example.csv"):
        super().__init__()
        self.test_file = test_file
        
    def test_local_import(self):
        """Test import using local example.csv file"""
        if not os.path.exists(self.test_file):
            logger.error(f"Test file {self.test_file} not found")
            return False
            
        logger.info(f"Testing import with local file: {self.test_file}")
        
        try:
            # Read the local CSV file
            df = pd.read_csv(
                self.test_file,
                low_memory=False,
                dtype={
                    'id_mutation': 'str',
                    'code_postal': 'str', 
                    'code_commune': 'str',
                    'id_parcelle': 'str',
                    'adresse_numero': 'str'
                }
            )
            
            logger.info(f"Loaded {len(df)} test records from {self.test_file}")
            
            # Add test year metadata
            df['import_year'] = 2024
            
            # Process in chunks (even for small test file)
            chunk_size = 10  # Small chunks for testing
            total_inserted = 0
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size].copy()
                rows_inserted = self.insert_chunk(chunk, 2024, i//chunk_size)
                total_inserted += rows_inserted
                logger.info(f"Test chunk {i//chunk_size}: {rows_inserted}/{len(chunk)} rows inserted")
            
            logger.info(f"✅ Test import completed: {total_inserted}/{len(df)} rows imported")
            return total_inserted > 0
            
        except Exception as e:
            logger.error(f"Error in test import: {str(e)}")
            return False
            
    def validate_data_integrity(self):
        """Validate that imported data matches expected structure"""
        if not self.connection:
            return False
            
        try:
            with self.connection.cursor() as cursor:
                # Check table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'dvf_data'
                    )
                """)
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    logger.error("Table dvf_data does not exist")
                    return False
                    
                # Check data count
                cursor.execute("SELECT COUNT(*) FROM dvf_data WHERE import_year = 2024")
                count = cursor.fetchone()[0]
                logger.info(f"Found {count} test records in database")
                
                # Check data samples
                cursor.execute("""
                    SELECT id_mutation, date_mutation, valeur_fonciere, type_local, surface_reelle_bati, prix_m2
                    FROM dvf_data 
                    WHERE import_year = 2024 
                    LIMIT 5
                """)
                
                samples = cursor.fetchall()
                logger.info("Sample records from database:")
                for sample in samples:
                    logger.info(f"  ID: {sample[0]}, Date: {sample[1]}, Price: {sample[2]}, Type: {sample[3]}, Surface: {sample[4]}, Price/m²: {sample[5]}")
                
                # Validate computed column (prix_m2)
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM dvf_data 
                    WHERE import_year = 2024 
                    AND surface_reelle_bati > 0 
                    AND prix_m2 IS NOT NULL
                """)
                prix_m2_count = cursor.fetchone()[0]
                logger.info(f"Records with computed prix_m2: {prix_m2_count}")
                
                return count > 0
                
        except Exception as e:
            logger.error(f"Error validating data integrity: {str(e)}")
            return False


def main():
    """Run tests for DVF importer"""
    print("🧪 Testing DVF Importer")
    print("=" * 50)
    
    # Create test importer
    test_importer = TestDVFImporter()
    
    try:
        # Connect to database
        if not test_importer.connect_to_database():
            logger.error("Failed to connect to database for testing")
            return 1
            
        # Initialize database schema
        logger.info("Initializing database schema for testing...")
        if not test_importer.initialize_database():
            logger.error("Failed to initialize database schema")
            return 1
            
        # Clear any existing test data
        logger.info("Clearing existing test data...")
        test_importer.clear_year_data(2024)
        
        # Run test import
        logger.info("Running test import...")
        if not test_importer.test_local_import():
            logger.error("Test import failed")
            return 1
            
        # Validate data integrity
        logger.info("Validating data integrity...")
        if not test_importer.validate_data_integrity():
            logger.error("Data integrity validation failed")
            return 1
            
        # Show final statistics
        stats = test_importer.get_database_stats()
        if stats:
            print("\n📊 Test Results:")
            print("=" * 30)
            print(f"Total records: {stats['total_records']}")
            print(f"Test records (2024): {stats.get('test_records', 'N/A')}")
            print(f"Table size: {stats['table_size']}")
            
        logger.info("✅ All tests passed!")
        return 0
        
    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}")
        return 1
    finally:
        test_importer.close_connection()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

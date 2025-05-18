#!/usr/bin/env python3
"""
Generic Field Addition Utility

This script adds specified fields to the DVF database if they don't already exist.
You can specify multiple fields and their data types as command-line arguments.

Usage:
  python add_fields.py field1:type1 field2:type2 field3:type3
  python add_fields.py --common  (adds all common missing fields)

Examples:
  python add_fields.py adresse_nom_voie:VARCHAR(255) nom_commune:VARCHAR(255)
  python add_fields.py adresse_numero:VARCHAR(50)
  python add_fields.py --common
"""
import os
import sys
import argparse
import time
from sqlalchemy import create_engine, text

# Common DVF fields that might be missing (with their data types)
COMMON_FIELDS = {
    'code_postal': 'VARCHAR(5)',
    'surface_reelle_bati': 'NUMERIC',
    'nombre_pieces_principales': 'NUMERIC', 
    'code_commune': 'VARCHAR(10)',
    'nom_commune': 'VARCHAR(255)',
    'code_departement': 'VARCHAR(5)',
    'adresse_nom_voie': 'VARCHAR(255)',
    'adresse_numero': 'VARCHAR(50)'
}

# Database connection settings - can be overridden via environment variables
DB_USER = os.environ.get('POSTGRES_USER', 'dvf_user')
DB_PASS = os.environ.get('POSTGRES_PASSWORD', 'dvf_password')
DB_NAME = os.environ.get('POSTGRES_DB', 'dvf_data')
DB_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
DB_PORT = os.environ.get('POSTGRES_PORT', '5432')

def wait_for_postgres(db_uri, max_retries=10, retry_interval=5):
    """Wait for PostgreSQL to be ready"""
    print("Connecting to PostgreSQL database...")
    engine = create_engine(db_uri)
    
    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                print("✅ Successfully connected to PostgreSQL!")
                return True
        except Exception as e:
            print(f"❌ Attempt {i+1}/{max_retries}: Connection failed: {str(e)}")
            if i < max_retries - 1:
                print(f"   Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
    
    print("⚠️ All connection attempts failed.")
    return False

def get_database_engine():
    """Get a connection to the PostgreSQL database"""
    # Try different connection configurations
    connection_configs = [
        # Docker service name
        {
            'host': DB_HOST,
            'port': DB_PORT,
            'user': DB_USER,
            'password': DB_PASS,
            'db': DB_NAME
        },
        # Localhost
        {
            'host': 'localhost',
            'port': DB_PORT,
            'user': DB_USER,
            'password': DB_PASS,
            'db': DB_NAME
        },
        # Explicit IP
        {
            'host': '127.0.0.1',
            'port': DB_PORT,
            'user': DB_USER,
            'password': DB_PASS,
            'db': DB_NAME
        }
    ]
    
    for config in connection_configs:
        # Create connection string
        db_uri = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['db']}"
        print(f"Trying to connect to: {config['host']}:{config['port']}/{config['db']} as {config['user']}")
        
        if wait_for_postgres(db_uri):
            engine = create_engine(db_uri)
            return engine
    
    print("❌ Failed to connect to any database.")
    return None

def test_table_existence(engine, table_name='dvf_data'):
    """Test if the specified table exists in the database"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT to_regclass('public.{table_name}')")).fetchone()
            if result and result[0] is not None:
                print(f"✅ Table '{table_name}' exists in the database.")
                return True
            else:
                print(f"❌ Table '{table_name}' does not exist in the database.")
                return False
    except Exception as e:
        print(f"❌ Error checking table existence: {str(e)}")
        return False

def get_existing_columns(engine, table_name='dvf_data'):
    """Get list of existing columns in the table"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            """))
            return [row[0] for row in result.fetchall()]
    except Exception as e:
        print(f"❌ Error getting existing columns: {str(e)}")
        return []

def add_fields(engine, fields_to_add, table_name='dvf_data'):
    """Add fields to the database table if they don't exist"""
    # First check if the table exists
    if not test_table_existence(engine, table_name):
        print(f"❌ Table '{table_name}' does not exist. Please create it first.")
        return False
    
    # Get existing columns
    existing_columns = get_existing_columns(engine, table_name)
    if not existing_columns:
        print("❌ Could not retrieve existing columns.")
        return False
    
    print(f"✅ Found {len(existing_columns)} existing columns in table '{table_name}'.")
    
    # Determine which columns need to be added
    columns_to_add = {}
    for field_name, field_type in fields_to_add.items():
        if field_name in existing_columns:
            print(f"✓ Column '{field_name}' already exists - skipping.")
        else:
            columns_to_add[field_name] = field_type
    
    if not columns_to_add:
        print("✅ All requested fields already exist in the table!")
        return True
    
    # Add the missing columns
    success_count = 0
    failure_count = 0
    
    with engine.connect() as conn:
        for column_name, column_type in columns_to_add.items():
            try:
                print(f"➕ Adding column '{column_name}' with type '{column_type}'...")
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"))
                success_count += 1
                print(f"✅ Successfully added column '{column_name}'.")
            except Exception as e:
                failure_count += 1
                print(f"❌ Error adding column '{column_name}': {str(e)}")
    
    print(f"\nAddition summary:")
    print(f"- Columns to add: {len(columns_to_add)}")
    print(f"- Successfully added: {success_count}")
    print(f"- Failed to add: {failure_count}")
    
    return success_count > 0

def parse_field_arg(field_arg):
    """Parse a field argument in the format 'name:type'"""
    if ':' not in field_arg:
        return field_arg, 'VARCHAR(255)'  # Default type if not specified
    
    parts = field_arg.split(':', 1)
    return parts[0], parts[1]

def main():
    """Main function to orchestrate the field addition process"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Add fields to the DVF database')
    parser.add_argument('fields', nargs='*', help='Fields to add in format field:type')
    parser.add_argument('--common', action='store_true', help='Add common missing fields')
    parser.add_argument('--table', type=str, default='dvf_data', help='Table name to modify')
    args = parser.parse_args()
    
    # Determine which fields to add
    fields_to_add = {}
    
    if args.common:
        fields_to_add.update(COMMON_FIELDS)
        print(f"Adding common fields: {', '.join(fields_to_add.keys())}")
    
    if args.fields:
        for field_arg in args.fields:
            field_name, field_type = parse_field_arg(field_arg)
            fields_to_add[field_name] = field_type
        
        if not args.common:  # Only print if we didn't already print for common fields
            print(f"Adding specified fields: {', '.join(fields_to_add.keys())}")
    
    if not fields_to_add:
        print("❌ No fields specified. Use positional arguments or --common flag.")
        parser.print_help()
        sys.exit(1)
    
    # Connect to database
    engine = get_database_engine()
    if not engine:
        sys.exit(1)
    
    # Add the fields
    success = add_fields(engine, fields_to_add, args.table)
    
    if success:
        print("\n✅ Field addition process completed successfully!")
    else:
        print("\n❌ Field addition process completed with errors.")
        sys.exit(1)

if __name__ == "__main__":
    main() 
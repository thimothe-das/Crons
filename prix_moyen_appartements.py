#!/usr/bin/env python3
import pandas as pd
import os
import numpy as np
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import sqlalchemy
from sqlalchemy import create_engine, text

app = Flask(__name__)


CORS(app, resources={r"/*": {"origins": "*"}})

# PostgreSQL connection settings from environment variables
DB_USER = os.environ.get('POSTGRES_USER', 'dvf_user')
DB_PASS = os.environ.get('POSTGRES_PASSWORD', 'dvf_password')
DB_NAME = os.environ.get('POSTGRES_DB', 'dvf_data')
DB_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
DB_PORT = os.environ.get('POSTGRES_PORT', '5432')

# Create the database URI
DB_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

class AnalyseDVF:
    def __init__(self, url_csv=None, chemin_fichier=None):
        """
        Initialise l'analyseur de données DVF (Demande de Valeurs Foncières)
        
        Args:
            url_csv (str): URL du fichier CSV distant à analyser
            chemin_fichier (str): Chemin vers un fichier DVF déjà téléchargé
        """
        self.url_csv = url_csv
        self.chemin_fichier = chemin_fichier
        self.donnees = None
        

            

        
    def filtrer_donnees(self, parcelles=None, type_local=None, min_m2=None, max_m2=None, option_garage='tous'):
        """
        Filtre les données selon différents critères
        
        Args:
            parcelles (list): Liste des IDs de parcelles cadastrales
            type_local (str): Type de local (Appartement, Maison, etc.)
            min_m2 (float): Superficie minimale en m²
            max_m2 (float): Superficie maximale en m²
            option_garage (str): Option pour les garages ('tous', 'avec', 'sans')
            
        Returns:
            pandas.DataFrame: Données filtrées
        """
        if self.donnees is None:
            raise Exception("No data loaded - AnalyseDVF class is deprecated, use PostgreSQL API instead")
            
        # Copie des données pour ne pas modifier l'original
        donnees_filtrees = self.donnees.copy()
        
        # Filtre par parcelles cadastrales
        if parcelles:
            donnees_filtrees = donnees_filtrees[donnees_filtrees['id_parcelle'].isin(parcelles)]
            print(f"Après filtre parcelles '{', '.join(parcelles)}': {len(donnees_filtrees)} transactions")
            
        # Filtre par type de local
        if type_local:
            donnees_filtrees = donnees_filtrees[donnees_filtrees['type_local'] == type_local]
            print(f"Après filtre type_local '{type_local}': {len(donnees_filtrees)} transactions")
            
        # Filtre par superficie
        if min_m2 is not None:
            donnees_filtrees = donnees_filtrees[donnees_filtrees['surface_reelle_bati'] >= min_m2]
            print(f"Après filtre surface min {min_m2}m²: {len(donnees_filtrees)} transactions")
            
        if max_m2 is not None:
            donnees_filtrees = donnees_filtrees[donnees_filtrees['surface_reelle_bati'] <= max_m2]
            print(f"Après filtre surface max {max_m2}m²: {len(donnees_filtrees)} transactions")
            
        # Filtre par présence de garage
        if option_garage != 'tous':
            # Récupérer les ID de mutations qui ont une dépendance
            id_mutations_avec_garage = self.donnees[
                (self.donnees['type_local'] == 'Dépendance') &
                (self.donnees['id_mutation'].isin(donnees_filtrees['id_mutation']))
            ]['id_mutation'].unique()
            
            if option_garage == 'avec':
                # Conserver uniquement les biens avec garage
                donnees_filtrees = donnees_filtrees[donnees_filtrees['id_mutation'].isin(id_mutations_avec_garage)]
                print(f"Après filtre avec garage: {len(donnees_filtrees)} transactions")
            elif option_garage == 'sans':
                # Exclure les biens avec garage
                donnees_filtrees = donnees_filtrees[~donnees_filtrees['id_mutation'].isin(id_mutations_avec_garage)]
                print(f"Après filtre sans garage: {len(donnees_filtrees)} transactions")
            
        return donnees_filtrees
        
    def analyser_prix(self, donnees_filtrees=None, parcelles=None, type_local=None, min_m2=None, max_m2=None, option_garage='tous'):
        """
        Analyse les prix des biens après filtrage
        
        Args:
            donnees_filtrees (pandas.DataFrame): Données déjà filtrées (optionnel)
            parcelles (list): Liste des IDs de parcelles cadastrales
            type_local (str): Type de local (Appartement, Maison, etc.)
            min_m2 (float): Superficie minimale en m²
            max_m2 (float): Superficie maximale en m²
            option_garage (str): Option pour les garages ('tous', 'avec', 'sans')
            
        Returns:
            dict: Résultats de l'analyse
        """
        if donnees_filtrees is None:
            donnees_filtrees = self.filtrer_donnees(parcelles, type_local, min_m2, max_m2, option_garage)
            
        if len(donnees_filtrees) == 0:
            return {
                'nombre_transactions': 0,
                'prix_moyen': None,
                'prix_median': None,
                'prix_m2_moyen': None,
                'prix_m2_median': None
            }
            
        # Calculer les statistiques
        prix_moyen = donnees_filtrees['valeur_fonciere'].mean()
        prix_median = donnees_filtrees['valeur_fonciere'].median()
        
        # Calculer le prix au m²
        donnees_filtrees_avec_surface = donnees_filtrees[donnees_filtrees['surface_reelle_bati'] > 0].copy()
        if len(donnees_filtrees_avec_surface) > 0:
            donnees_filtrees_avec_surface['prix_m2'] = donnees_filtrees_avec_surface['valeur_fonciere'] / donnees_filtrees_avec_surface['surface_reelle_bati']
            prix_m2_moyen = donnees_filtrees_avec_surface['prix_m2'].mean()
            prix_m2_median = donnees_filtrees_avec_surface['prix_m2'].median()
        else:
            prix_m2_moyen = None
            prix_m2_median = None
            
        return {
            'nombre_transactions': len(donnees_filtrees),
            'prix_moyen': prix_moyen,
            'prix_median': prix_median,
            'prix_m2_moyen': prix_m2_moyen,
            'prix_m2_median': prix_m2_median
        }
        
    def analyser_par_parcelle(self, parcelles, type_local=None, min_m2=None, max_m2=None, option_garage='tous'):
        """
        Analyse les prix des biens par parcelle
        
        Args:
            parcelles (list): Liste des IDs de parcelles cadastrales
            type_local (str): Type de local (Appartement, Maison, etc.)
            min_m2 (float): Superficie minimale en m²
            max_m2 (float): Superficie maximale en m²
            option_garage (str): Option pour les garages ('tous', 'avec', 'sans')
            
        Returns:
            dict: Résultats de l'analyse par parcelle
        """
        resultats_par_parcelle = {}
        
        # Analyse globale sur toutes les parcelles
        resultats_par_parcelle['global'] = self.analyser_prix(
            parcelles=parcelles,
            type_local=type_local,
            min_m2=min_m2,
            max_m2=max_m2,
            option_garage=option_garage
        )
        
        # Analyse détaillée par parcelle
        for parcelle in parcelles:
            resultats_par_parcelle[parcelle] = self.analyser_prix(
                parcelles=[parcelle],
                type_local=type_local,
                min_m2=min_m2,
                max_m2=max_m2,
                option_garage=option_garage
            )
            
        return resultats_par_parcelle





def get_database_engine():
    """Get a connection to the PostgreSQL database via Docker network"""
    print("Connecting to PostgreSQL database via Docker network...")
    
    try:
        # Create connection string using Docker service name
        conn_uri = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        print(f"Connecting to: {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER}")
        
        # Create engine with connection timeout
        engine = create_engine(conn_uri, connect_args={"connect_timeout": 10})
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
            if result and result[0] == 1:
                print(f"✅ Successfully connected to database at {DB_HOST}:{DB_PORT}/{DB_NAME}")
                return engine
                
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        print("Make sure PostgreSQL container is running with: docker-compose up -d postgres")
        return None
    
    print("⚠️ Database connection failed.")
    return None

def find_dvf_table(engine):
    """Find the DVF data table in the database"""
    try:
        with engine.connect() as conn:
            # Check for possible table names based on import scripts
            tables_to_check = ['dvf_data', 'transactions', 'dvf']
            
            for table in tables_to_check:
                result = conn.execute(text(f"SELECT to_regclass('public.{table}')")).fetchone()
                if result and result[0] is not None:
                    print(f"Found data table: {table}")
                    return table
            
            print("No DVF data table found in the database")
            return None
    except Exception as e:
        print(f"Error finding DVF table: {str(e)}")
        return None

def build_postgres_query(table_name, filters=None, max_price=10000000, limit=30):
    """Build SQL query with filters for DVF data with performance optimizations"""
    # Only select columns that are actually needed
    needed_columns = [
        'id_mutation', 
        'date_mutation', 
        'id_parcelle',
        'valeur_fonciere',
        'type_local',
        'surface_reelle_bati'
    ]
    
    # Add optional columns if they're likely to be used (based on code analysis)
    optional_columns = ['code_postal', 'adresse_nom_voie', 'nom_commune', 'adresse_numero']
    selected_columns = needed_columns + optional_columns
    
    # Start with optimized base query using column list instead of * 
    query = f"""
    SELECT /*+ PARALLEL */
        {", ".join(selected_columns)}
    FROM 
        {table_name}
    WHERE 
        valeur_fonciere > 0 
        AND valeur_fonciere <= {max_price}
        AND surface_reelle_bati > 0
    """
    
    # Apply type filter directly in the query - most common filter, first in WHERE clause
    if filters and 'type_local' in filters and filters['type_local']:
        query += f" AND type_local = '{filters['type_local']}'"
    
    # Apply year filter if provided
    if filters and 'years' in filters and filters['years']:
        years_list = []
        for y in filters['years']:
            try:
                year_clean = int(y.strip())
                years_list.append(str(year_clean))
            except (ValueError, TypeError):
                print(f"Invalid year value: {y}, skipping")
                
        if years_list:
            years_str = ", ".join(years_list)
            # Cast date_mutation to date type first, in case it's stored as text
            query += f" AND EXTRACT(YEAR FROM date_mutation::date) IN ({years_str})"
            print(f"Filtering by years: {years_str}")
    
    # Apply the most specific filters first (for query planner optimization)
    # Apply parcelles filter - use SQL IN clause with limited set of values
    if filters and 'parcelles' in filters and filters['parcelles']:
        parcelles_list = []
        for p in filters['parcelles']:
            p_clean = p.strip()
            # SQL injection prevention for user-provided values
            if p_clean and "'" not in p_clean:
                parcelles_list.append(f"'{p_clean}'")
                
        if parcelles_list:
            parcelles_str = ", ".join(parcelles_list)
            query += f" AND id_parcelle IN ({parcelles_str})"
    
    # Apply postal codes filter - use SQL IN clause with limited set of values
    if filters and 'codes_postaux' in filters and filters['codes_postaux']:
        codes_list = []
        for code in filters['codes_postaux']:
            code_clean = code.strip()
            # SQL injection prevention for user-provided values
            if code_clean and "'" not in code_clean:
                codes_list.append(f"'{code_clean}'")
                
        if codes_list:
            codes_str = ", ".join(codes_list)
            query += f" AND code_postal IN ({codes_str})"
    
    # Apply numeric range filters
    if filters and 'min_surface' in filters and filters['min_surface'] is not None:
        try:
            min_surface = float(filters['min_surface'])
            query += f" AND surface_reelle_bati >= {min_surface}"
        except (ValueError, TypeError):
            # Skip invalid values rather than failing
            print(f"Invalid min_surface value: {filters['min_surface']}, skipping filter")
    
    if filters and 'max_surface' in filters and filters['max_surface'] is not None:
        try:
            max_surface = float(filters['max_surface'])
            query += f" AND surface_reelle_bati <= {max_surface}"
        except (ValueError, TypeError):
            # Skip invalid values rather than failing
            print(f"Invalid max_surface value: {filters['max_surface']}, skipping filter")
    
    # Add ordering and limit for performance
    query += f"""
    ORDER BY date_mutation DESC, valeur_fonciere DESC
    LIMIT {limit}
    """
    
    return query

def execute_postgres_query(engine, query):
    """Execute SQL query with performance optimizations and return pandas DataFrame"""
    try:
        # Set PostgreSQL session parameters optimized for 2GB system
        config_query = text("""
            SET work_mem = '1MB';  -- Conservative memory for 2GB system
            SET temp_buffers = '8MB';  -- Reduced temp buffer size
            SET random_page_cost = 1.1;  -- Optimize for SSDs
            SET effective_io_concurrency = 50;  -- Reduced for limited system
            SET enable_seqscan = on;  -- Allow sequence scans
            SET enable_hashagg = on;  -- Enable hash aggregation
            SET enable_hashjoin = on;  -- Enable hash joins
            SET enable_bitmapscan = on;  -- Enable bitmap scans
        """)
        
        # SQLAlchemy connection with cursor options
        print("Configuring database session for optimal performance...")
        with engine.connect() as conn:
            # Apply performance settings
            conn.execute(config_query)
            
            # Start timing the actual query
            import time
            query_start = time.time()
            
            # Create SQLAlchemy text query
            sql_query = text(query)
            
            # Create a server-side cursor to efficiently fetch rows
            print("Executing query with enhanced performance settings...")
            
            # Execute query and fetch results
            result = conn.execute(sql_query)
            
            # Load data into pandas DataFrame with efficiency options
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            query_time = time.time() - query_start
            print(f"Raw query execution completed in {query_time:.2f}s")
            print(f"Retrieved {len(df)} rows from database")
            
        return df
        
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return None

def process_dataframe(df):
    """Post-process the dataframe to prepare for analysis with optimized operations"""
    if df is None or len(df) == 0:
        return None
    
    # Record starting time
    import time
    start_time = time.time()
    
    # Convert decimal.Decimal columns to float for pandas compatibility
    numeric_columns = ['valeur_fonciere', 'surface_reelle_bati', 'surface_terrain', 'longitude', 'latitude',
                      'lot1_surface_carrez', 'lot2_surface_carrez', 'lot3_surface_carrez', 
                      'lot4_surface_carrez', 'lot5_surface_carrez']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Check for required columns and handle missing columns gracefully
    required_columns = ['valeur_fonciere', 'surface_reelle_bati']
    for col in required_columns:
        if col not in df.columns:
            print(f"Error: Required column '{col}' missing from query results")
            return None
    
    # Process date column only if needed and exists
    if 'date_mutation' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['date_mutation']):
        # Use efficient categorical dtype for dates to reduce memory
        df['date_mutation'] = pd.to_datetime(df['date_mutation'], errors='coerce')
    
    # Calculate price per square meter using vectorized operations
    # Only calculate on rows with valid values to avoid unnecessary operations
    valid_rows = (df['valeur_fonciere'] > 0) & (df['surface_reelle_bati'] > 0)
    df.loc[valid_rows, 'prix_m2'] = df.loc[valid_rows, 'valeur_fonciere'] / df.loc[valid_rows, 'surface_reelle_bati']
    
    # Handle any NaN values resulting from division
    if 'prix_m2' in df.columns:
        df['prix_m2'] = df['prix_m2'].fillna(0)
    
    process_time = time.time() - start_time
    print(f"Dataframe processing completed in {process_time:.2f}s")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
    
    return df

def load_data_from_postgres(filters=None, max_price=10000000):
    """Load DVF data from PostgreSQL database with filters"""
    try:
        import time
        start_time = time.time()
        
        # Step 1: Connect to database
        engine = get_database_engine()
        if not engine:
            print("Failed to connect to PostgreSQL via Docker network")
            return None
        
        connect_time = time.time() - start_time
        print(f"Database connection time: {connect_time:.2f}s")
        
        # Step 2: Find DVF data table
        table_check_start = time.time()
        table_name = find_dvf_table(engine)
        if not table_name:
            return None
            
        table_check_time = time.time() - table_check_start
        print(f"Table check time: {table_check_time:.2f}s")
        
        # Step 3: Build query with filters (limit to 30 records for 2GB system)
        query_build_start = time.time()
        query = build_postgres_query(table_name, filters, max_price, limit=30)
        query_build_time = time.time() - query_build_start
        print(f"Query build time: {query_build_time:.2f}s")
        print(f"Executing database query with 30 record limit for 2GB system...")
        
        # Step 4: Execute query
        query_exec_start = time.time()
        df = execute_postgres_query(engine, query)
        query_exec_time = time.time() - query_exec_start
        print(f"Query execution time: {query_exec_time:.2f}s")
        
        # Step 5: Post-process data
        if df is not None:
            process_start = time.time()
            df = process_dataframe(df)
            process_time = time.time() - process_start
            print(f"Post-processing time: {process_time:.2f}s")
        
        total_time = time.time() - start_time
        print(f"Total database load time: {total_time:.2f}s")
        return df
    
    except Exception as e:
        print(f"Error loading data from database: {str(e)}")
        print("Database connection failed - make sure PostgreSQL container is running")
        return None

def get_database_connection():
    """Legacy function for backward compatibility"""
    return get_database_engine()

@app.route('/dvf', methods=['GET'])
def get_dvf_data():
    try:
        print("API call received to /api/dvf")
        
        # Get filter parameters from the request
        parcelles = request.args.get('parcelles')
        type_local = request.args.get('type')
        min_surface = request.args.get('min')
        max_surface = request.args.get('max')
        option_garage = request.args.get('garage')
        codes_postaux = request.args.get('codes_postaux')
        max_price = request.args.get('max_price', default=10000000, type=int)  # Default max price of 10 million euros
        years = request.args.get('years')  # Multiple years, comma-separated
        
        print(f"Filters: parcelles={parcelles}, type={type_local}, min={min_surface}, max={max_surface}, garage={option_garage}, codes_postaux={codes_postaux}, max_price={max_price}, years={years}")
        
        # Default type_local to 'Appartement' if none provided
        if not type_local:
            type_local = 'Appartement'
        
        # Prepare filters for PostgreSQL query
        filters = {
            'type_local': type_local
        }
        
        if parcelles:
            filters['parcelles'] = parcelles.split(',')
        
        if min_surface:
            try:
                filters['min_surface'] = float(min_surface)
            except ValueError:
                print(f"Invalid min_surface value: {min_surface}")
        
        if max_surface:
            try:
                filters['max_surface'] = float(max_surface)
            except ValueError:
                print(f"Invalid max_surface value: {max_surface}")
        
        if codes_postaux:
            filters['codes_postaux'] = codes_postaux.split(',')
            print(f"Filtering by postal codes: {filters['codes_postaux']}")
            
        if years:
            filters['years'] = years.split(',')
            print(f"Filtering by years: {filters['years']}")
        
        # Load data from PostgreSQL via Docker network
        print("Loading data from PostgreSQL...")
        df = load_data_from_postgres(filters, max_price=max_price)
        
        # If PostgreSQL connection failed, return database error
        if df is None:
            error_message = "Database connection failed. Make sure PostgreSQL container is running."
            print(error_message)
            return jsonify({
                'error': error_message,
                'suggestion': "Run: docker-compose up -d postgres && docker-compose up dvf-importer",
                'nombre_transactions': 0,
                'nombre_transactions_affiches': 0,
                'prix_moyen': 0,
                'prix_median': 0,
                'prix_m2_moyen': 0,
                'prix_m2_median': 0,
                'transactions': []
            }), 500
        
        # If no results found, continue with empty dataset (this is valid)
        if len(df) == 0:
            print("No transactions found matching the specified criteria")
            return jsonify({
                'nombre_transactions': 0,
                'nombre_transactions_affiches': 0,
                'prix_moyen': 0,
                'prix_median': 0,
                'prix_m2_moyen': 0,
                'prix_m2_median': 0,
                'transactions': [],
                'message': 'No transactions found matching the specified criteria'
            })
        
        # Option garage filter (applies to both data sources)
        if option_garage == 'avec':
            # This would need mutation IDs with garage dependencies
            # Simplified version - assuming we don't have this data structure in the sample
            print("Garage filter not implemented yet")
            
        # Total number of transactions
        nombre_transactions = len(df)
        print(f"Total filtered transactions: {nombre_transactions}")
        
        # Calculate statistics
        nombre_transactions_affiches = min(len(df), 100)  # Limit displayed transactions to 100
        
        # Detect and handle outliers for more accurate statistics
        if not df.empty:
            # Log basic statistics before outlier removal
            print(f"Before outlier removal - Min price: {df['valeur_fonciere'].min()}, Max price: {df['valeur_fonciere'].max()}")
            print(f"Before outlier removal - 10th percentile: {df['valeur_fonciere'].quantile(0.1)}, 90th percentile: {df['valeur_fonciere'].quantile(0.9)}")
            
            # Calculate quartiles for outlier detection
            Q1 = df['valeur_fonciere'].quantile(0.25)
            Q3 = df['valeur_fonciere'].quantile(0.75)
            IQR = Q3 - Q1
            
            # Define outlier bounds (standard is 1.5*IQR)
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            print(f"Outlier detection - Q1: {Q1}, Q3: {Q3}, IQR: {IQR}")
            print(f"Outlier detection - Lower bound: {lower_bound}, Upper bound: {upper_bound}")
            
            # Filter outliers for statistics calculation
            df_filtered = df[(df['valeur_fonciere'] >= lower_bound) & (df['valeur_fonciere'] <= upper_bound)]
            outliers_count = len(df) - len(df_filtered)
            
            if outliers_count > 0:
                print(f"Filtered {outliers_count} outlier prices for statistics calculation")
                # Log the outliers for analysis
                outliers = df[~df.index.isin(df_filtered.index)]
                print(f"Sample of outlier values: {outliers['valeur_fonciere'].sample(min(5, len(outliers))).tolist()}")
                
            # After filtering, log statistics again
            if not df_filtered.empty:
                print(f"After outlier removal - Min price: {df_filtered['valeur_fonciere'].min()}, Max price: {df_filtered['valeur_fonciere'].max()}")
                
            # Use filtered data for statistics but keep all data for display
            prix_moyen = int(df_filtered['valeur_fonciere'].mean()) if not df_filtered.empty else 0
            prix_median = int(df_filtered['valeur_fonciere'].median()) if not df_filtered.empty else 0
            prix_m2_moyen = int(df_filtered['prix_m2'].mean()) if not df_filtered.empty else 0
            prix_m2_median = int(df_filtered['prix_m2'].median()) if not df_filtered.empty else 0
        else:
            prix_moyen = 0
            prix_median = 0
            prix_m2_moyen = 0
            prix_m2_median = 0
        
        print(f"Statistics calculated: prix_moyen={prix_moyen}, prix_median={prix_median}")
        
        # Sample data for display if needed
        display_df = df
     
            
        # Format transactions data
        transactions = []
        for _, row in display_df.iterrows():
            # Handle NaN values safely by providing defaults
            prix = 0 if pd.isna(row['valeur_fonciere']) else int(row['valeur_fonciere'])
            surface = 0 if pd.isna(row['surface_reelle_bati']) else int(row['surface_reelle_bati'])
            prix_m2 = 0 if pd.isna(row['prix_m2']) else int(row['prix_m2'])
            
            # Create transaction object with all available fields
            transaction = {
                'date': row['date_mutation'].strftime('%d/%m/%Y'),
                'prix': prix,
                'surface': surface,
                'prix_m2': prix_m2
            }
            
            # Add optional fields if they exist in the dataframe
            if 'code_postal' in row and not pd.isna(row['code_postal']):
                transaction['code_postal'] = row['code_postal']
                
            # Add address and commune if they exist in the dataframe
            if 'adresse_nom_voie' in row and not pd.isna(row['adresse_nom_voie']):
                transaction['adresse'] = row['adresse_nom_voie']
                
            # Add street number if it exists
            if 'adresse_numero' in row and not pd.isna(row['adresse_numero']) and str(row['adresse_numero']).strip():
                transaction['numero'] = str(row['adresse_numero']).strip()
                # If both number and street name exist, create a full address field
                if 'adresse' in transaction:
                    transaction['adresse_complete'] = f"{transaction['numero']} {transaction['adresse']}"
                
            if 'nom_commune' in row and not pd.isna(row['nom_commune']):
                transaction['commune'] = row['nom_commune']
                
            transactions.append(transaction)
        
        # Create response
        response = {
            'nombre_transactions': nombre_transactions,
            'nombre_transactions_affiches': len(transactions),
            'prix_moyen': prix_moyen,
            'prix_median': prix_median,
            'prix_m2_moyen': prix_m2_moyen,
            'prix_m2_median': prix_m2_median,
            'transactions': transactions
        }
        
        # Add filter information to response
        if years:
            try:
                years_list = [int(y.strip()) for y in years.split(',') if y.strip().isdigit()]
                if years_list:
                    response['years'] = years_list
            except Exception as e:
                print(f"Error adding years to response: {str(e)}")
        
        # Ensure all values are JSON serializable (convert numpy types to Python types)
        response = {k: int(v) if isinstance(v, np.integer) else float(v) if isinstance(v, np.floating) else v 
                   for k, v in response.items() if k != 'transactions'}
        response['transactions'] = transactions
        
        print("Sending response to client")
        return jsonify(response)
    
    except Exception as e:
        error_message = f"Error processing request: {str(e)}"
        print(error_message)
        import traceback
        traceback.print_exc()
        return jsonify({'error': error_message}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint with debug information"""
    debug_info = {
        'status': 'healthy',
        'is_secure': request.is_secure,
        'scheme': request.scheme,
        'host': request.host,
        'url': request.url,
        'headers': {
            'X-Forwarded-Proto': request.headers.get('X-Forwarded-Proto'),
            'X-Forwarded-For': request.headers.get('X-Forwarded-For'),
            'X-Forwarded-Host': request.headers.get('X-Forwarded-Host'),
            'Host': request.headers.get('Host'),
        }
    }
    
    return jsonify(debug_info)

def main():
    parser = argparse.ArgumentParser(description='Analyse des prix des biens immobiliers à partir des données DVF')
    parser.add_argument('--url', type=str, help='URL du fichier CSV distant contenant les données DVF')
    parser.add_argument('--fichier', type=str, help='Chemin vers un fichier DVF déjà téléchargé')
    parser.add_argument('--parcelles', type=str, help='Liste des IDs de parcelles cadastrales séparés par des virgules')
    parser.add_argument('--type', type=str, help='Type de local (Appartement, Maison, etc.)')
    parser.add_argument('--min', type=float, help='Superficie minimale en m²')
    parser.add_argument('--max', type=float, help='Superficie maximale en m²')
    parser.add_argument('--garage', choices=['tous', 'avec', 'sans'], default='tous', 
                        help='Option pour les garages: tous, avec, sans')
    parser.add_argument('--detail', action='store_true', help='Afficher le détail par parcelle')
    
    args = parser.parse_args()
    
    analyseur = AnalyseDVF(url_csv=args.url, chemin_fichier=args.fichier)
    
    # Convertir la chaîne de parcelles en liste
    parcelles = None
    if args.parcelles:
        parcelles = [p.strip() for p in args.parcelles.split(',')]
    
    if args.detail and parcelles and len(parcelles) > 1:
        # Analyse détaillée par parcelle
        resultats = analyseur.analyser_par_parcelle(
            parcelles=parcelles,
            type_local=args.type,
            min_m2=args.min,
            max_m2=args.max,
            option_garage=args.garage
        )
        
        # Affichage des résultats globaux
        print("\nRésultat global sur toutes les parcelles:")
        print("=" * 60)
        afficher_resultats(resultats['global'], parcelles, args.type, args.min, args.max, args.garage)
        
        # Affichage des résultats par parcelle
        print("\nRésultats détaillés par parcelle:")
        print("=" * 60)
        for parcelle, res in resultats.items():
            if parcelle != 'global' and res['nombre_transactions'] > 0:
                print(f"\nParcelle: {parcelle}")
                print("-" * 40)
                afficher_resultats(res, [parcelle], args.type, args.min, args.max, args.garage)
    else:
        # Analyse simple
        resultats = analyseur.analyser_prix(
            parcelles=parcelles,
            type_local=args.type,
            min_m2=args.min,
            max_m2=args.max,
            option_garage=args.garage
        )
        
        print("\nRésultat de l'analyse:")
        print("=" * 50)
        afficher_resultats(resultats, parcelles, args.type, args.min, args.max, args.garage)

def afficher_resultats(resultats, parcelles, type_local, min_m2, max_m2, option_garage):
    """Affiche les résultats de l'analyse"""
    if resultats['nombre_transactions'] > 0:
        print(f"Nombre de transactions: {resultats['nombre_transactions']}")
        if parcelles:
            if len(parcelles) == 1:
                print(f"Parcelle: {parcelles[0]}")
            else:
                print(f"Parcelles: {', '.join(parcelles)}")
        if type_local:
            print(f"Type de local: {type_local}")
        if min_m2 is not None or max_m2 is not None:
            min_str = str(min_m2) if min_m2 is not None else "0"
            max_str = str(max_m2) if max_m2 is not None else "infini"
            print(f"Superficie: {min_str} à {max_str} m²")
        print(f"Option garage: {option_garage}")
        print(f"Prix moyen: {resultats['prix_moyen']:,.2f} €")
        print(f"Prix médian: {resultats['prix_median']:,.2f} €")
        if resultats['prix_m2_moyen'] and resultats['prix_m2_median']:
            print(f"Prix moyen au m²: {resultats['prix_m2_moyen']:,.2f} €/m²")
            print(f"Prix médian au m²: {resultats['prix_m2_median']:,.2f} €/m²")
    else:
        print("Aucune transaction trouvée correspondant aux critères")

if __name__ == "__main__":
    import os
    # Always start as API when container runs
    port = int(os.getenv("PORT", 6644))
    debug = os.getenv("FLASK_DEBUG", "false").lower() in ("1", "true", "yes")

    print(f"Starting Flask API on port {port} (debug={debug})")
    app.run(host="0.0.0.0", port=port, debug=debug)
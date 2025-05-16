#!/usr/bin/env python3
import pandas as pd
import argparse
import requests
import io
import os
import numpy as np
from flask import Flask, jsonify, request
from flask_cors import CORS
import json
from datetime import datetime

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

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
        
    def telecharger_donnees(self):
        """Télécharge les données DVF depuis l'URL fournie ou utilise un fichier local"""
        if self.chemin_fichier and os.path.exists(self.chemin_fichier):
            print(f"Utilisation du fichier local: {self.chemin_fichier}")
            return
            
        if not self.url_csv:
            raise Exception("Aucune URL ou fichier local spécifié pour les données DVF")
            
        print(f"Téléchargement des données depuis {self.url_csv}...")
        
        try:
            response = requests.get(self.url_csv)
            if response.status_code == 200:
                # Si aucun fichier local n'est spécifié, on crée un nom par défaut
                if not self.chemin_fichier:
                    self.chemin_fichier = "dvf_data.csv"
                
                with open(self.chemin_fichier, 'wb') as f:
                    f.write(response.content)
                print(f"Données téléchargées et sauvegardées dans {self.chemin_fichier}")
            else:
                raise Exception(f"Erreur lors du téléchargement: {response.status_code}")
        except Exception as e:
            raise Exception(f"Erreur lors du téléchargement: {str(e)}")
            
    def charger_donnees(self):
        """Charge les données DVF dans un DataFrame pandas"""
        if not self.chemin_fichier or not os.path.exists(self.chemin_fichier):
            self.telecharger_donnees()
            
        print(f"Chargement des données depuis {self.chemin_fichier}...")
        
        # Déterminer le type de compression si nécessaire
        if self.chemin_fichier.endswith('.gz'):
            self.donnees = pd.read_csv(self.chemin_fichier, compression='gzip', low_memory=False)
        else:
            self.donnees = pd.read_csv(self.chemin_fichier, low_memory=False)
            
        print(f"Données chargées: {len(self.donnees)} transactions")
        
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
            self.charger_donnees()
            
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

def load_data(file_path):
    df = pd.read_csv(file_path, delimiter=',')
    # Filter for only apartments (type_local = 'Appartement')
    df_apparts = df[df['type_local'] == 'Appartement']
    
    # Convert date_mutation to datetime
    df_apparts['date_mutation'] = pd.to_datetime(df_apparts['date_mutation'])
    
    # Make sure valeur_fonciere and surface_reelle_bati are numeric
    df_apparts['valeur_fonciere'] = pd.to_numeric(df_apparts['valeur_fonciere'], errors='coerce')
    df_apparts['surface_reelle_bati'] = pd.to_numeric(df_apparts['surface_reelle_bati'], errors='coerce')
    
    # Calculate price per square meter
    df_apparts['prix_m2'] = df_apparts['valeur_fonciere'] / df_apparts['surface_reelle_bati']
    
    return df_apparts

def load_data_from_url(url):
    """Load DVF data directly from a URL, supporting gzipped files"""
    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from URL: {response.status_code}")
        
        # Determine if the content is gzipped
        if url.endswith('.gz'):
            import gzip
            # Load CSV data from gzipped content
            csv_data = gzip.decompress(response.content)
            df = pd.read_csv(io.BytesIO(csv_data), delimiter=',')
        else:
            # Load regular CSV data from response content
            csv_data = io.StringIO(response.content.decode('utf-8'))
            df = pd.read_csv(csv_data, delimiter=',')
        
        # Filter for only apartments (type_local = 'Appartement')
        df_apparts = df[df['type_local'] == 'Appartement']
        
        # Convert date_mutation to datetime
        df_apparts['date_mutation'] = pd.to_datetime(df_apparts['date_mutation'])
        
        # Make sure valeur_fonciere and surface_reelle_bati are numeric
        df_apparts['valeur_fonciere'] = pd.to_numeric(df_apparts['valeur_fonciere'], errors='coerce')
        df_apparts['surface_reelle_bati'] = pd.to_numeric(df_apparts['surface_reelle_bati'], errors='coerce')
        
        # Calculate price per square meter
        df_apparts['prix_m2'] = df_apparts['valeur_fonciere'] / df_apparts['surface_reelle_bati']
        
        return df_apparts
    except Exception as e:
        raise Exception(f"Error loading data from URL: {str(e)}")

@app.route('/api/dvf', methods=['GET'])
def get_dvf_data():
    try:
        print("API call received to /api/dvf")
        # Set default URL to the new one provided
        dvf_url = os.getenv('DVF_API_URL', 'https://files.data.gouv.fr/geo-dvf/latest/csv/2024/full.csv.gz')
        print(f"Using data source: {dvf_url}")
        
        # Fetch data from URL
        print("Loading data from URL...")
        df = load_data_from_url(dvf_url)
        print(f"Data loaded successfully, {len(df)} records found")
        
        # Apply filters based on query parameters
        # Get filter parameters from the request
        parcelles = request.args.get('parcelles')
        type_local = request.args.get('type')
        min_surface = request.args.get('min')
        max_surface = request.args.get('max')
        option_garage = request.args.get('garage')
        print(f"Filters: parcelles={parcelles}, type={type_local}, min={min_surface}, max={max_surface}, garage={option_garage}")
        
        # Apply filters
        if parcelles:
            parcelles_list = [p.strip() for p in parcelles.split(',')]
            df = df[df['id_parcelle'].isin(parcelles_list)]
            print(f"After parcelles filter: {len(df)} records")
            
        if type_local:
            df = df[df['type_local'] == type_local]
            print(f"After type filter: {len(df)} records")
            
        if min_surface:
            try:
                min_surface = float(min_surface)
                df = df[df['surface_reelle_bati'] >= min_surface]
                print(f"After min surface filter: {len(df)} records")
            except ValueError:
                print(f"Invalid min_surface value: {min_surface}")
                
        if max_surface:
            try:
                max_surface = float(max_surface)
                df = df[df['surface_reelle_bati'] <= max_surface]
                print(f"After max surface filter: {len(df)} records")
            except ValueError:
                print(f"Invalid max_surface value: {max_surface}")
                
        if option_garage == 'avec':
            # This would need mutation IDs with garage dependencies
            # Simplified version - assuming we don't have this data structure in the sample
            print("Garage filter not implemented yet")
            
        # Total number of transactions
        nombre_transactions = len(df)
        print(f"Total filtered transactions: {nombre_transactions}")
        
        # Calculate statistics
        nombre_transactions_affiches = min(len(df), 100000000000000)  # Limit displayed transactions to 100
        
        # Calculate statistics on the entire dataset
        prix_moyen = int(df['valeur_fonciere'].mean()) if not df.empty else 0
        prix_median = int(df['valeur_fonciere'].median()) if not df.empty else 0
        prix_m2_moyen = int(df['prix_m2'].mean()) if not df.empty else 0
        prix_m2_median = int(df['prix_m2'].median()) if not df.empty else 0
        
        print(f"Statistics calculated: prix_moyen={prix_moyen}, prix_median={prix_median}")
        
        # Sample data for display if needed
        display_df = df
        if len(df) > 100:
            display_df = df.sample(n=100, random_state=42)
            print(f"Sampled {len(display_df)} transactions for display")
            
        # Format transactions data
        transactions = []
        for _, row in display_df.iterrows():
            transaction = {
                'date': row['date_mutation'].strftime('%d/%m/%Y'),
                'prix': int(row['valeur_fonciere']),
                'surface': int(row['surface_reelle_bati']),
                'prix_m2': int(row['prix_m2'])
            }
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
        
        print("Sending response to client")
        return jsonify(response)
    
    except Exception as e:
        error_message = f"Error processing request: {str(e)}"
        print(error_message)
        import traceback
        traceback.print_exc()
        return jsonify({'error': error_message}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

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
    # Check if we should run the API or the CLI
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--api':
        # Run as API server
        port = int(os.getenv('PORT', 6644))
        app.run(debug=True, host='0.0.0.0', port=port)
    else:
        # Run as CLI tool
        main() 
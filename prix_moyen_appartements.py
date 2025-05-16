#!/usr/bin/env python3
import pandas as pd
import argparse
import requests
import io
import os

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
    main() 
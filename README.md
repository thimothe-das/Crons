# Analyse des Prix Immobiliers (DVF)

Ce script permet d'analyser les données cadastrales françaises des valeurs foncières (DVF) pour calculer le prix moyen et médian des biens immobiliers selon différents critères de filtrage.

## À propos des données DVF

Les données DVF (Demandes de Valeurs Foncières) sont des données ouvertes publiées par la Direction Générale des Finances Publiques (DGFiP) qui contiennent l'ensemble des transactions immobilières intervenues en France depuis 5 ans.

Source des données: [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/r/316795eb-a3fa-465d-b058-38ef8579da11)

## Prérequis

- Python 3.6+
- Connexion internet pour télécharger les données (si non fournies)

## Installation

1. Clonez ce dépôt ou téléchargez les fichiers
2. Installez les dépendances requises:

```bash
pip install -r requirements.txt
```

## Utilisation

### Chargement de données

Vous pouvez charger les données de deux manières:

1. À partir d'une URL:
```bash
python prix_moyen_appartements.py --url https://example.com/path/to/dvf.csv
```

2. À partir d'un fichier local:
```bash
python prix_moyen_appartements.py --fichier /chemin/vers/dvf.csv
```

### Filtrage et analyse

Le script offre plusieurs options de filtrage:

- `--url`: URL du fichier CSV distant à analyser
- `--fichier`: Chemin vers un fichier local CSV contenant les données DVF
- `--parcelles`: Liste des IDs de parcelles cadastrales séparés par des virgules (ex: "33281000BO0529,33281000BO0530")
- `--type`: Type de local ("Appartement", "Maison", "Dépendance", etc.)
- `--min`: Superficie minimale en m²
- `--max`: Superficie maximale en m²
- `--garage`: Option pour les garages ('tous', 'avec', 'sans')
- `--detail`: Afficher le détail par parcelle (utile pour comparer plusieurs parcelles)

### Exemples

Analyser les appartements d'une parcelle spécifique:
```bash
python prix_moyen_appartements.py --fichier dvf.csv --parcelles 33281000BO0529 --type Appartement
```

Analyser les appartements entre 60 et 80 m² avec garage:
```bash
python prix_moyen_appartements.py --fichier dvf.csv --type Appartement --min 60 --max 80 --garage avec
```

Analyser les biens sans garage dans plusieurs parcelles:
```bash
python prix_moyen_appartements.py --fichier dvf.csv --parcelles "33281000BO0529,33281000BO0530" --garage sans
```

Analyser plusieurs parcelles avec détail par parcelle:
```bash
python prix_moyen_appartements.py --fichier dvf.csv --parcelles "33281000BO0529,33281000BO0530" --type Appartement --detail
```

## Résultats

Le script calculera et affichera:
- Le nombre de transactions correspondant aux critères
- Le prix moyen
- Le prix médian
- Le prix moyen au m²
- Le prix médian au m²

Lorsque l'option `--detail` est utilisée avec plusieurs parcelles, le script affichera d'abord les résultats globaux puis les résultats détaillés pour chaque parcelle.

## Note

La première exécution avec une URL téléchargera les données et les stockera localement pour les utilisations ultérieures. 
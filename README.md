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

# DVF Data API and Viewer

This project provides tools for analyzing and visualizing property transaction data from the DVF (Demande de Valeurs Foncières) dataset.

## Components

1. **Python Backend API**:
   - Processes DVF CSV data
   - Calculates statistical information (average price, median price, etc.)
   - Exposes data through a RESTful API
   - Located in `prix_moyen_appartements.py`

2. **Next.js Frontend**:
   - Modern web interface for visualizing the data
   - Dashboard with key statistics
   - Detailed transaction listing
   - Located in the `dvf-viewer` directory

## Getting Started

### Prerequisites

- Python 3.8+
- Node.js and npm

### Setup

1. **Create a Python virtual environment**:
```bash
python3 -m venv venv
source venv/bin/activate
pip install flask pandas requests flask-cors
```

2. **Place your DVF data file**:
   - Ensure you have a `dvf.csv` file in the main directory

3. **Launch the application**:
```bash
./start.sh
```

This will:
- Start the Python API server
- Configure the Next.js app to use the API
- Start the Next.js development server

### Accessing the Application

- **Web Interface**: Open http://localhost:3000 in your browser
- **Dashboard**: http://localhost:3000/dashboard
- **Direct API Access**: http://localhost:6644/api/dvf

## API Endpoints

### GET /api/dvf

Returns statistics about apartment transactions from the DVF data.

Example response:

```json
{
  "nombre_transactions": 24,
  "prix_moyen": 285000,
  "prix_median": 265000,
  "prix_m2_moyen": 4250,
  "prix_m2_median": 4100,
  "transactions": [
    {
      "date": "20/01/2023",
      "prix": 255000,
      "surface": 64,
      "prix_m2": 3984
    },
    ...
  ]
}
```

### GET /api/health

Health check endpoint.

Example response:

```json
{
  "status": "healthy"
}
```

## Testing

You can test the API separately using:

```bash
python test_api.py
```

This will verify that the API can read the DVF data and return properly formatted statistics. 
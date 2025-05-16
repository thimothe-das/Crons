#!/bin/bash

# Script to start the DVF backend API server

# Set environment variables (customize as needed)
export DVF_API_URL="https://files.data.gouv.fr/geo-dvf/latest/csv/2024/full.csv.gz"
export PORT=6644
export FLASK_APP=prix_moyen_appartements.py
export FLASK_ENV=development

echo "Starting DVF Backend API server on port $PORT..."
echo "Using data from: $DVF_API_URL"

# Create and activate virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Check for Python dependencies and install if needed
echo "Installing dependencies in virtual environment..."
pip install --upgrade pip
pip install -r requirements.txt

# Verify pandas is installed
if ! python -c "import pandas" 2>/dev/null; then
    echo "Pandas not found, installing directly..."
    pip install pandas
    if ! python -c "import pandas" 2>/dev/null; then
        echo "ERROR: Failed to install pandas. Please install it manually:"
        echo "source venv/bin/activate && pip install pandas"
        exit 1
    fi
fi

# Show API endpoint information
echo ""
echo "API will be available at: http://localhost:$PORT/api/dvf"
echo "Health check endpoint: http://localhost:$PORT/api/health"
echo ""

# Run the Python script with the --api flag
python prix_moyen_appartements.py --api

# Note: Press Ctrl+C to stop the server 
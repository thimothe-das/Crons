#!/bin/bash

# Set the URL for the DVF API
export DVF_API_URL="https://files.data.gouv.fr/geo-dvf/latest/csv/2024/full.csv.gz"
export PORT=6644

# Start the API server in the background
echo "Starting DVF API server on port $PORT..."
echo "Using DVF data from: $DVF_API_URL"
source venv/bin/activate
python prix_moyen_appartements.py --api &
API_PID=$!

# Wait a moment for the API to start
sleep 2

# Check if we're in the dvf-viewer directory, if not cd into it
if [ ! -d "frontend" ]; then
  echo "Error: frontend directory not found!"
  echo "Make sure you're in the correct directory or clone the Next.js project."
  kill $API_PID
  exit 1
fi

# Go to the Next.js directory and start it
cd frontend
echo "Starting Next.js app..."
echo "API_URL=http://localhost:$PORT" > .env.local
npm run dev

# When Next.js is stopped, also stop the API server
echo "Stopping the API server..."
kill $API_PID 
#!/bin/bash

# Script to start the DVF frontend application

# Set environment variables
export NEXT_PUBLIC_API_URL="http://localhost:6644"

echo "======================================================"
echo "Starting DVF Frontend application..."
echo "Connecting to backend at: $NEXT_PUBLIC_API_URL"
echo ""
echo "IMPORTANT: Make sure the backend server is running first!"
echo "Run ./start_backend.sh in a separate terminal window"
echo "======================================================"
echo ""

# Navigate to the frontend directory
cd frontend

# Check if node_modules exists, install dependencies if not
if [ ! -d "node_modules" ]; then
  echo "Installing frontend dependencies..."
  npm install
  echo ""
fi

# Start the Next.js development server
echo "Starting Next.js development server..."
npm run dev

# Note: Press Ctrl+C to stop the server 
#!/usr/bin/env python3
import requests
import json
import os
import time
import subprocess
import signal
import sys

def start_api_server():
    """Start the API server in a subprocess"""
    print("Starting API server...")
    # Use the DVF API URL
    os.environ["DVF_API_URL"] = "https://dvf-api.data.gouv.fr/dvf/csv/?parcelle=33281000BO0519"
    os.environ["PORT"] = "5050"  # Use a different port for testing
    
    # Start the server
    process = subprocess.Popen(
        ["python", "prix_moyen_appartements.py", "--api"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Give the server time to start
    time.sleep(2)
    return process

def test_dvf_endpoint():
    """Test the /api/dvf endpoint"""
    response = requests.get("http://localhost:5050/api/dvf")
    if response.status_code == 200:
        data = response.json()
        print("API test successful! Response:")
        print(json.dumps(data, indent=2))
        
        # Validate response contains expected fields
        expected_fields = [
            'nombre_transactions', 'prix_moyen', 'prix_median', 
            'prix_m2_moyen', 'prix_m2_median', 'transactions'
        ]
        for field in expected_fields:
            if field not in data:
                print(f"ERROR: Missing expected field '{field}' in response")
                return False
                
        # Check if we have transactions
        if len(data['transactions']) == 0:
            print("WARNING: No transactions in response")
        
        return True
    else:
        print(f"API test failed with status code {response.status_code}")
        print(response.text)
        return False

def test_health_endpoint():
    """Test the /api/health endpoint"""
    response = requests.get("http://localhost:5050/api/health")
    if response.status_code == 200:
        data = response.json()
        if data.get('status') == 'healthy':
            print("Health check endpoint working correctly")
            return True
    
    print(f"Health check failed with status {response.status_code}")
    return False

if __name__ == "__main__":
    # Start the API server
    server_process = start_api_server()
    
    try:
        # Test the endpoints
        health_success = test_health_endpoint()
        dvf_success = test_dvf_endpoint()
        
        if health_success and dvf_success:
            print("\nAll tests passed! The API is working correctly with the URL data source.")
        else:
            print("\nSome tests failed. Please check the errors above.")
    finally:
        # Clean up: kill the server process
        print("\nStopping API server...")
        server_process.terminate()
        server_process.wait() 
#!/usr/bin/env python3

import requests
import time
import json
from tabulate import tabulate

# API endpoint
API_URL = "http://localhost:6644/api/dvf"

# Test cases with different parameters
test_cases = [
    {
        "name": "Paris 1,2 - Limit 10",
        "params": {"type": "Appartement", "codes_postaux": "75001,75002", "min": "50", "limit": "10"}
    },
    {
        "name": "Paris 1,2 - Limit 50",
        "params": {"type": "Appartement", "codes_postaux": "75001,75002", "min": "50", "limit": "50"}
    },
    {
        "name": "Paris 1,2 - Limit 100",
        "params": {"type": "Appartement", "codes_postaux": "75001,75002", "min": "50", "limit": "100"}
    },
    {
        "name": "Paris 1,2 - Without Limit",
        "params": {"type": "Appartement", "codes_postaux": "75001,75002", "min": "50"}
    },
    {
        "name": "All Apartments - Limit 10",
        "params": {"type": "Appartement", "limit": "10"}
    },
    {
        "name": "All Apartments - Limit 100",
        "params": {"type": "Appartement", "limit": "100"}
    }
]

def run_test(test_case, runs=3):
    """Run a test case multiple times and return average execution time"""
    name = test_case["name"]
    params = test_case["params"]
    
    total_time = 0
    results = {}
    
    print(f"Running test: {name}")
    for i in range(runs):
        start_time = time.time()
        response = requests.get(API_URL, params=params)
        end_time = time.time()
        
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            return None
        
        execution_time = end_time - start_time
        total_time += execution_time
        
        # Get the result data for the first run
        if i == 0:
            data = response.json()
            results["total_records"] = data.get("nombre_transactions", 0)
            results["displayed_records"] = data.get("nombre_transactions_affiches", 0)
        
        print(f"  Run {i+1}: {execution_time:.3f} seconds")
    
    avg_time = total_time / runs
    results["avg_time"] = avg_time
    
    print(f"  Average: {avg_time:.3f} seconds")
    print()
    
    return results

def main():
    results = []
    
    print("=== API Performance Test ===\n")
    
    # Check if API is running
    try:
        health_check = requests.get("http://localhost:6644/api/health")
        if health_check.status_code != 200:
            print("API is not available. Please make sure it's running.")
            return
    except requests.exceptions.ConnectionError:
        print("Could not connect to API. Please make sure it's running.")
        return
    
    # Run all test cases
    for test_case in test_cases:
        result = run_test(test_case)
        if result:
            result["name"] = test_case["name"]
            result["params"] = test_case["params"]
            results.append(result)
    
    # Print summary table
    summary = []
    for result in results:
        summary.append([
            result["name"], 
            result["total_records"],
            result["displayed_records"],
            f"{result['avg_time']:.3f}"
        ])
    
    print("\n=== Summary ===")
    print(tabulate(summary, headers=["Test Case", "Total Records", "Displayed Records", "Avg Time (s)"]))

if __name__ == "__main__":
    main() 
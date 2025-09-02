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
        "name": "Paris 1,2 - Base filter",
        "params": {"type": "Appartement", "codes_postaux": "75001,75002", "min": "50"}
    },
    {
        "name": "Paris 1,2 - Year 2020",
        "params": {"type": "Appartement", "codes_postaux": "75001,75002", "min": "50", "annee": "2020"}
    },
    {
        "name": "Paris 1,2 - Year 2021",
        "params": {"type": "Appartement", "codes_postaux": "75001,75002", "min": "50", "annee": "2021"}
    },
    {
        "name": "Paris 1,2 - Year range 2019-2020",
        "params": {"type": "Appartement", "codes_postaux": "75001,75002", "min": "50", "annee": "2019,2020"}
    },
    {
        "name": "Single postal code - 75001",
        "params": {"type": "Appartement", "codes_postaux": "75001", "min": "50"}
    },
    {
        "name": "Multiple postal codes - 75001,75002,75003",
        "params": {"type": "Appartement", "codes_postaux": "75001,75002,75003", "min": "50"}
    },
    {
        "name": "Year comparison - 2019",
        "params": {"type": "Appartement", "codes_postaux": "75001", "min": "50", "annee": "2019"}
    },
    {
        "name": "Year comparison - 2020",
        "params": {"type": "Appartement", "codes_postaux": "75001", "min": "50", "annee": "2020"}
    },
    {
        "name": "Year comparison - 2021",
        "params": {"type": "Appartement", "codes_postaux": "75001", "min": "50", "annee": "2021"}
    },
    {
        "name": "Maison - Paris region",
        "params": {"type": "Maison", "codes_postaux": "75001,75002,75003,75004,75005"}
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
        try:
            start_time = time.time()
            response = requests.get(API_URL, params=params, timeout=30)
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
        except requests.exceptions.RequestException as e:
            print(f"  Run {i+1}: Failed - {str(e)}")
            return None
    
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
        health_check = requests.get("http://localhost:6644/api/health", timeout=5)
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
        else:
            print(f"Test '{test_case['name']}' failed, skipping from summary.")
    
    # Print summary table
    if not results:
        print("\n=== Summary ===")
        print("All tests failed. Please check the API connection and try again.")
        return
    
    # Group by test type
    year_tests = [r for r in results if r["name"].startswith("Year comparison")]
    postal_code_tests = [r for r in results if r["name"].startswith("Single postal code") or r["name"].startswith("Multiple postal codes")]
    
    print("\n=== Summary ===")
    summary = []
    for result in results:
        # Extract key parameters for the summary
        params = result["params"]
        postal_codes = params.get("codes_postaux", "All")
        year = params.get("annee", "All")
        property_type = params.get("type", "All")
        
        summary.append([
            result["name"], 
            property_type,
            postal_codes,
            year,
            result["total_records"],
            result["displayed_records"],
            f"{result['avg_time']:.3f}"
        ])
    
    print(tabulate(summary, headers=["Test Case", "Type", "Postal Codes", "Year", "Total Records", "Displayed Records", "Avg Time (s)"]))
    
    # Print comparison insights
    if len(year_tests) > 1:
        print("\n=== Year Comparison ===")
        year_summary = [[r["name"], r.get("params", {}).get("annee", "All"), r["total_records"], f"{r['avg_time']:.3f}"] for r in year_tests]
        print(tabulate(year_summary, headers=["Test Case", "Year", "Total Records", "Avg Time (s)"]))
        
    if len(postal_code_tests) > 1:
        print("\n=== Postal Code Comparison ===")
        pc_summary = [[r["name"], r.get("params", {}).get("codes_postaux", "All"), r["total_records"], f"{r['avg_time']:.3f}"] for r in postal_code_tests]
        print(tabulate(pc_summary, headers=["Test Case", "Postal Codes", "Total Records", "Avg Time (s)"]))
    
    # Performance analysis
    print("\n=== Performance Analysis ===")
    
    # Analyze year filter performance
    if len(year_tests) > 1:
        year_times = [r["avg_time"] for r in year_tests]
        avg_year_time = sum(year_times) / len(year_times)
        fastest_year = min(year_tests, key=lambda x: x["avg_time"])
        slowest_year = max(year_tests, key=lambda x: x["avg_time"])
        print(f"Year filter analysis:")
        print(f"- Average response time: {avg_year_time:.3f}s")
        print(f"- Fastest year: {fastest_year['params']['annee']} ({fastest_year['avg_time']:.3f}s)")
        print(f"- Slowest year: {slowest_year['params']['annee']} ({slowest_year['avg_time']:.3f}s)")
        print(f"- Performance difference: {((slowest_year['avg_time'] - fastest_year['avg_time']) / fastest_year['avg_time'] * 100):.1f}%")
    
    # Analyze postal code filter performance
    if len(postal_code_tests) > 1:
        # Sort by the number of postal codes
        sorted_pc_tests = sorted(postal_code_tests, key=lambda x: len(x["params"]["codes_postaux"].split(",")))
        
        print(f"\nPostal code filter analysis:")
        for test in sorted_pc_tests:
            postal_codes = test["params"]["codes_postaux"]
            num_codes = len(postal_codes.split(","))
            records = test["total_records"]
            time = test["avg_time"]
            print(f"- {num_codes} postal code(s) ({postal_codes}): {records} records in {time:.3f}s")
        
        # Calculate performance impact per additional postal code
        if len(sorted_pc_tests) >= 2:
            base_test = sorted_pc_tests[0]
            multi_test = sorted_pc_tests[-1]
            
            base_time = base_test["avg_time"]
            multi_time = multi_test["avg_time"]
            base_codes = len(base_test["params"]["codes_postaux"].split(","))
            multi_codes = len(multi_test["params"]["codes_postaux"].split(","))
            
            time_increase = multi_time - base_time
            code_increase = multi_codes - base_codes
            
            if code_increase > 0:
                print(f"- Performance impact: +{(time_increase/code_increase):.3f}s per additional postal code")
                print(f"- Time increase: {(time_increase/base_time*100):.1f}% from {base_codes} to {multi_codes} postal codes")

if __name__ == "__main__":
    main() 
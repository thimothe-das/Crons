#!/usr/bin/env python3
import os
import time
import json
import argparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd

class ApartmentValueCalculator:
    def __init__(self, headless=True):
        """Initialize the web driver and configuration"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
    
    def __del__(self):
        """Clean up resources"""
        if hasattr(self, 'driver'):
            self.driver.quit()
    
    def calculate_value_zillow(self, address, zip_code):
        """Calculate apartment value using Zillow"""
        try:
            # Format the address for URL
            formatted_address = address.replace(' ', '-').lower()
            
            # Navigate to Zillow search page
            url = f"https://www.zillow.com/homes/{address}-{zip_code}_rb/"
            self.driver.get(url)
            
            # Wait for page to load
            time.sleep(5)
            
            # Extract the home value if available
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Try to find the Zestimate
            zestimate_element = soup.find('span', {'data-testid': 'zestimate-value'})
            if zestimate_element:
                return {
                    'source': 'Zillow Zestimate',
                    'value': zestimate_element.text.strip()
                }
            
            # If Zestimate not found, try the listing price
            price_element = soup.find('span', {'data-testid': 'price'})
            if price_element:
                return {
                    'source': 'Zillow Listing Price',
                    'value': price_element.text.strip()
                }
            
            return {'source': 'Zillow', 'value': 'Value not found'}
        
        except Exception as e:
            print(f"Error with Zillow calculation: {str(e)}")
            return {'source': 'Zillow', 'value': f'Error: {str(e)}'}
    
    def calculate_value_redfin(self, address, zip_code):
        """Calculate apartment value using Redfin"""
        try:
            # Navigate to Redfin search page
            url = "https://www.redfin.com/"
            self.driver.get(url)
            
            # Find and fill search input
            search_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "search-box-input"))
            )
            search_input.send_keys(f"{address}, {zip_code}")
            
            # Click search button
            search_button = self.driver.find_element(By.CLASS_NAME, "SearchButton")
            search_button.click()
            
            # Wait for search results
            time.sleep(5)
            
            # Parse the result page
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Try to find the Redfin estimate
            estimate_element = soup.find('div', {'data-rf-test-id': 'avm-price'})
            if estimate_element:
                return {
                    'source': 'Redfin Estimate',
                    'value': estimate_element.text.strip()
                }
            
            # If estimate not found, try the listing price
            price_element = soup.find('div', {'data-rf-test-id': 'abp-price'})
            if price_element:
                return {
                    'source': 'Redfin Listing Price',
                    'value': price_element.text.strip()
                }
            
            return {'source': 'Redfin', 'value': 'Value not found'}
        
        except Exception as e:
            print(f"Error with Redfin calculation: {str(e)}")
            return {'source': 'Redfin', 'value': f'Error: {str(e)}'}
    
    def calculate_value(self, address, zip_code):
        """Calculate apartment value from multiple sources"""
        results = []
        
        # Get value from Zillow
        zillow_value = self.calculate_value_zillow(address, zip_code)
        results.append(zillow_value)
        
        # Get value from Redfin
        redfin_value = self.calculate_value_redfin(address, zip_code)
        results.append(redfin_value)
        
        # You can add more sources here
        
        return results

def main():
    parser = argparse.ArgumentParser(description='Calculate apartment value from real estate websites')
    parser.add_argument('--address', type=str, required=True, help='Street address of the apartment')
    parser.add_argument('--zip', type=str, required=True, help='ZIP code of the apartment')
    parser.add_argument('--headless', action='store_true', default=True, help='Run browser in headless mode')
    
    args = parser.parse_args()
    
    calculator = ApartmentValueCalculator(headless=args.headless)
    results = calculator.calculate_value(args.address, args.zip)
    
    # Print results
    print("\nApartment Valuation Results:")
    print("===========================")
    
    for result in results:
        print(f"{result['source']}: {result['value']}")
    
    # Save results to file
    with open('valuation_results.json', 'w') as f:
        json.dump(results, f, indent=4)
    
    print(f"\nResults saved to valuation_results.json")

if __name__ == "__main__":
    main() 
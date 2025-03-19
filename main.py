import requests
import pandas as pd
import json
import sys
from pathlib import Path

# Add Processing_Scripts folder to the system path
script_dir = Path(__file__).resolve().parent
processing_scripts_path = script_dir / "Processing_Scripts"
sys.path.append(str(processing_scripts_path))

# Import cleaning functions from child scripts
from clean_unprocessed_data import clean_unprocessed_data  
from clean_processed_data import clean_processed_data  

# Server API endpoint
SERVER_URL = "http://localhost:4000/api/"

class DataProcessor:
    def __init__(self, server_url):
        self.server_url = server_url
        self.session = requests.Session()  # Reuse session for better performance

    def fetch_UnprocessedData(self):
        """Fetch unprocessed data from the server."""
        try:
            response = self.session.get(f"{self.server_url}fetch_UnprocessedData", timeout=5)
            response.raise_for_status()  # Raise error for HTTP issues
            return response.json().get("data", [])
        except requests.exceptions.RequestException as e:
            print(f" Error fetching unprocessed data: {e}")
            return []
        
    def fetch_ProcessedData(self):
        """Fetch processed data from the server."""
        try:
            response = self.session.get(f"{self.server_url}fetch_ProcessedData", timeout=5)
            response.raise_for_status()
            return response.json().get("data", [])
        except requests.exceptions.RequestException as e:
            print(f" Error fetching processed data: {e}")
            return []
        
    def fetch_PredictionsData(self):
        """Fetch predictions data from the server."""
        try:
            response = self.session.get(f"{self.server_url}fetch_PredictionsData", timeout=5)
            response.raise_for_status()
            return response.json().get("data", [])
        except requests.exceptions.RequestException as e:
            print(f" Error fetching predictions data: {e}")
            return []

    def exportProcessed_data(self, data):
        """Export processed data back to the server."""
        try:
            headers = {'Content-Type': 'application/json'}
            response = self.session.post(f"{self.server_url}uploadProcessedData", json=data, headers=headers, timeout=5)
            response.raise_for_status()
            print(" Processed data exported successfully!")
        except requests.exceptions.RequestException as e:
            print(f" Error exporting processed data: {e}")  
            
    def exportPredictions_data(self, data):
        """Export predictions data back to the server."""
        try:
            headers = {'Content-Type': 'application/json'}
            response = self.session.post(f"{self.server_url}uploadPredictionsData", json=data, headers=headers, timeout=5)
            response.raise_for_status()
            print(" Predictions data exported successfully!")
        except requests.exceptions.RequestException as e:
            print(f" Error exporting predictions data: {e}")       

    def run(self):
        """Main execution flow."""
        print("\nFetching unprocessed data from the server...")
        unprocessed_data = self.fetch_UnprocessedData()
        
        if unprocessed_data:
            df_unprocessed = pd.DataFrame(unprocessed_data)
            print(f" Unprocessed Data loaded successfully! {len(df_unprocessed)} records found.")
            print(df_unprocessed.head())

            # **Process the data using the imported function**
            cleaned_data = clean_unprocessed_data(unprocessed_data)

            # **Export the cleaned data**
            self.exportProcessed_data(cleaned_data)
        else:
            print(" No unprocessed data received.")
            
        print("\nFetching processed data from the server...")
        processed_data = self.fetch_ProcessedData()

        if processed_data:
            df_processed = pd.DataFrame(processed_data)
            print(f" Processed Data loaded successfully! {len(df_processed)} records found.")
            print(df_processed.head())

            # **Process the data using the new function**
            predictions_data = clean_processed_data(processed_data)

            # **Export predictions data**
            self.exportPredictions_data(predictions_data)
        else:
            print(" No processed data received.")

        print("\nFetching predictions data from the server...")
        predictions_data = self.fetch_PredictionsData()

        if predictions_data:
            df_predictions = pd.DataFrame(predictions_data)
            print(f" Predictions Data loaded successfully! {len(df_predictions)} records found.")
            print(df_predictions.head())
        else:
            print(" No predictions data received.")

if __name__ == "__main__":
    processor = DataProcessor(SERVER_URL)
    processor.run()

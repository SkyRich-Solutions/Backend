import requests
import pandas as pd
import json

# Server API endpoint
SERVER_URL = "http://localhost:4000/api/"

class DataProcessor:
    def __init__(self, server_url):
        self.server_url = server_url
        self.session = requests.Session()  # Reuse session for better performance

    def fetch_UnprocessedData(self):
        """Fetch unprocessed data from the server."""
        try:
            response = self.session.get(f"{self.server_url}unprocessedData", timeout=5)
            response.raise_for_status()  # Raise error for HTTP issues
            data = response.json()
            return data.get("data", [])
        except requests.exceptions.RequestException as e:
            print(f" Error fetching unprocessed data: {e}")
            return []
        
    def fetch_PredictionsData(self):
        """Fetch predictions data from the server."""
        try:
            response = self.session.get(f"{self.server_url}uploadPredictionData", timeout=5)
            response.raise_for_status()  # Raise error for HTTP issues
            data = response.json()
            return data.get("data", [])
        except requests.exceptions.RequestException as e:
            print(f" Error fetching predictions data: {e}")
            return []

    def export_data(self, data):
        """Export processed data back to the server."""
        try:
            headers = {'Content-Type': 'application/json'}
            response = self.session.post(f"{self.server_url}uploadData", json=data, headers=headers, timeout=5)
            response.raise_for_status()
            print(" Data exported successfully!")
        except requests.exceptions.RequestException as e:
            print(f" Error exporting data: {e}")

    def process_data(self, data):
        """Process data by adding a 'processed' flag."""
        return [{**item, "processed": True} for item in data]

    def run(self):
        """Main execution flow."""
        print("\nFetching unprocessed data from the server...")
        unprocessed_data = self.fetch_UnprocessedData()
        
        if unprocessed_data:
            df_unprocessed = pd.DataFrame(unprocessed_data)
            print(f" Unprocessed Data loaded successfully! {len(df_unprocessed)} records found.")
            print(df_unprocessed.head())  # Display first few rows
        else:
            print(" No unprocessed data received.")

        print("\nFetching predictions data from the server...")
        predictions_data = self.fetch_PredictionsData()

        if predictions_data:
            df_predictions = pd.DataFrame(predictions_data)
            print(f" Predictions Data loaded successfully! {len(df_predictions)} records found.")
            print(df_predictions.head())  # Display first few rows
        else:
            print(" No predictions data received.")

        # Process and export unprocessed data if available
        if unprocessed_data:
            processed_unprocessed_data = self.process_data(unprocessed_data)
            self.export_data(processed_unprocessed_data)

        # Process and export predictions data if available
        if predictions_data:
            processed_predictions_data = self.process_data(predictions_data)
            self.export_data(processed_predictions_data)

if __name__ == "__main__":
    processor = DataProcessor(SERVER_URL)
    processor.run()

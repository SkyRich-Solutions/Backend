import requests
import pandas as pd
import json

# Server API endpoint
SERVER_URL = "http://localhost:4000/api/"

class DataProcessor:
    def __init__(self, server_url):
        self.server_url = server_url
        self.session = requests.Session()  # Reuse session for better performance

    def fetch_data(self):
        """Fetch unprocessed data from the server."""
        try:
            response = self.session.get(f"{self.server_url}unprocessedData", timeout=5)
            response.raise_for_status()  # Raise error for HTTP issues
            data = response.json()
            return data.get("data", [])
        except requests.exceptions.RequestException as e:
            print(f" Error fetching data: {e}")
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
        print("Fetching data from the server...")
        data = self.fetch_data()

        if not data:
            print(" No data received. Exiting...")
            return

        df = pd.DataFrame(data)
        print(f" Data loaded successfully! {len(df)} records found.")
        print(df.head())  # Display first few rows for verification

        processed_data = self.process_data(data)
        self.export_data(processed_data)

if __name__ == "__main__":
    processor = DataProcessor(SERVER_URL)
    processor.run()

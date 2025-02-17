import requests
import pandas as pd
import json

# Server API endpoint
SERVER_URL = "http://localhost:4000/api/"

def fetch_data():
    response = requests.get(f"{SERVER_URL}unprocessedData")  # Assuming 'unprocessed' endpoint
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching data:", response.status_code, response.text)
        return None

def export_data(data):
    headers = {'Content-Type': 'application/json'}
    response = requests.post(f"{SERVER_URL}uploadData", data=json.dumps(data), headers=headers)  # Assuming 'export' endpoint
    if response.status_code in [200, 201]:
        print("Data exported successfully")
    else:
        print("Error exporting data:", response.status_code, response.text)

def load_data_from_server():
    """
    Fetch unprocessed data from the server's API.

    Returns:
        pd.DataFrame: DataFrame containing unprocessed data.
    """
    try:
        data = fetch_data()
        if not data or "data" not in data:
            print("No data received from the server.")
            return pd.DataFrame()

        df = pd.DataFrame(data["data"])
        return df
    except Exception as e:
        print(f"⚠️ Error connecting to server: {e}")
        return pd.DataFrame()

def main():
    print("📥 Fetching data from the server...")
    df = load_data_from_server()

    if not df.empty:
        print("✅ Data loaded successfully!")
        print(df.head())  # Display first few rows for verification
        processed_data = [{**item, "processed": True} for item in df.to_dict(orient='records')]
        export_data(processed_data)
    else:
        print("⚠️ No data found. Exiting...")

if __name__ == "__main__":
    main()

import requests
import json

# Server API endpoint
SERVER_URL = "http://localhost:4000/api/"

# Function to fetch unprocessed data
def fetch_data():
    response = requests.get(f"{SERVER_URL}unprocessedData")  # Assuming 'unprocessed' endpoint
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching data:", response.status_code, response.text)
        return None

# Function to export processed data
def export_data(data):
    headers = {'Content-Type': 'application/json'}
    response = requests.post(f"{SERVER_URL}uploadData", data=json.dumps(data), headers=headers)  # Assuming 'export' endpoint
    if response.status_code == 200:
        print("Data exported successfully")
    else:
        print("Error exporting data:", response.status_code, response.text)

if __name__ == "__main__":
    main()

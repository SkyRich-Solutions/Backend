import unittest
import requests
from processTurbineData import DataProcessor

class TestDataProcessor(unittest.TestCase):

    def test_fetch_unprocessed_data_success(self):
        # URL for the unprocessed data endpoint
        url = "http://localhost:4000/api/unprocessedData"

        # Fetch data from the server
        response = requests.get(url)
        
        # Check if the response is successful
        self.assertEqual(response.status_code, 200, "Failed to fetch data from the server")

        # Get the data from the response
        data = response.json().get("data", [])
        
        # Print the number of turbines and the turbines themselves
        print(f"Number of turbines: {len(data)}")
        for turbine in data:
            print(turbine)

        # Assert that the data is not empty (optional, can adjust based on your needs)
        self.assertGreater(len(data), 0, "No unprocessed data received")

if __name__ == '__main__':
    unittest.main()

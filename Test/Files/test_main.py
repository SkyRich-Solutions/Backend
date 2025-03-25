import unittest
import requests
from unittest.mock import patch, MagicMock
import json
from io import StringIO
import sys
from main import DataProcessor

class TestDataProcessor(unittest.TestCase):
    
    # Test fetching unprocessed turbine data and verifying the fields.
    @patch('main.requests.Session.get')
    def test_fetch_unprocessed_turbine_data(self, mock_get):
        
        # Simulate a successful response with mock data
        with open('Test/MockData/TurbineData.json', 'r') as f:
            mock_turbine_data = json.load(f)

        mock_response = MagicMock()
        mock_response.json.return_value = {"data": mock_turbine_data}
        mock_get.return_value = mock_response

        processor = DataProcessor("http://localhost:4000/api/")
        data = processor.fetch_UnprocessedTurbineData()

        self.assertEqual(len(data), len(mock_turbine_data))
        self.assertEqual(data[0]["FunctionalLoc"], "AR0003=G001")  # Check a specific field


    # Test handling errors when fetching unprocessed turbine data (network error).
    @patch('main.requests.Session.get')
    def test_fetch_unprocessed_turbine_data_error(self, mock_get):
        
        # Simulate a network error
        mock_get.side_effect = requests.exceptions.RequestException("Network Error")

        processor = DataProcessor("http://localhost:4000/api/")
        data = processor.fetch_UnprocessedTurbineData()

        # Verify that an empty list is returned
        self.assertEqual(data, [])


    # Test fetching unprocessed material data and verifying the material field.
    @patch('main.requests.Session.get')
    def test_fetch_unprocessed_material_data(self, mock_get):
        
        # Simulate a successful response with mock material data
        with open('Test/MockData/MaterialData.json', 'r') as f:
            mock_material_data = json.load(f)

        mock_response = MagicMock()
        mock_response.json.return_value = {"data": mock_material_data}
        mock_get.return_value = mock_response

        processor = DataProcessor("http://localhost:4000/api/")
        data = processor.fetch_UnprocessedMaterialData()

        self.assertEqual(len(data), len(mock_material_data))
        self.assertEqual(data[0]["Material"], 5130)  # Check a specific field


    # Test exporting processed turbine data and verifying the POST request.
    @patch('main.requests.Session.post')
    def test_export_processed_turbine_data(self, mock_post):
        
        # Simulate a successful response for exporting processed turbine data
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        processor = DataProcessor("http://localhost:4000/api/")
        
        # Sample processed turbine data
        processed_turbine_data = [{"FunctionalLoc": "AR0003=G001", "TurbineModel": "SG 2.1-114"}]
        
        processor.exportProcessed_Turbine_data(processed_turbine_data)

        mock_post.assert_called_once_with(
            "http://localhost:4000/api/uploadProcessedTurbineData", 
            json=processed_turbine_data, 
            headers={'Content-Type': 'application/json'},
            timeout=5
        )


    # Test handling errors during turbine data export (network error).
    @patch('main.requests.Session.post')
    def test_export_processed_turbine_data_error(self, mock_post):
        
        # Simulate a network error for exporting processed turbine data
        mock_post.side_effect = requests.exceptions.RequestException("Network Error")

        processor = DataProcessor("http://localhost:4000/api/")
        
        processed_turbine_data = [{"FunctionalLoc": "AR0003=G001", "TurbineModel": "SG 2.1-114"}]
        
        # Capture print output
        captured_output = StringIO()
        sys.stdout = captured_output

        processor.exportProcessed_Turbine_data(processed_turbine_data)

        # Ensure that the correct error message is printed
        self.assertIn('Error exporting processed TurbineData: Network Error', captured_output.getvalue())
        
        # Reset stdout to default
        sys.stdout = sys.__stdout__


    # Test exporting processed material data and verifying the POST request.
    @patch('main.requests.Session.post')
    def test_export_processed_material_data(self, mock_post):
        
        # Simulate a successful response for exporting processed material data
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        processor = DataProcessor("http://localhost:4000/api/")
        
        # Sample processed material data
        processed_material_data = [{"Material": 5130, "Description": "Other merchandise"}]
        
        processor.exportProcessed_Material_data(processed_material_data)

        mock_post.assert_called_once_with(
            "http://localhost:4000/api/uploadProcessedMaterialData", 
            json=processed_material_data, 
            headers={'Content-Type': 'application/json'},
            timeout=5
        )

if __name__ == '__main__':
    unittest.main()

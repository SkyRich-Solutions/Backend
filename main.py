import pandas as pd
import numpy as np
import json
import sys
import requests  # Missing import
from pathlib import Path

# Add Processing_Scripts folder to the system path
script_dir = Path(__file__).resolve().parent
processing_scripts_path = script_dir / "Processing_Scripts"
sys.path.append(str(processing_scripts_path))

# Import cleaning functions from child scripts
from clean_unprocessed_Material_data import clean_material_data
from clean_unprocessed_Turbine_data import clean_turbine_data
from clean_unprocessed_Turbine_data import update_coordinates
from clean_processed_material_data import clean_processed_material_data
from clean_processed_turbine_data import clean_processed_turbine_data

# Server API endpoint
SERVER_URL = "http://localhost:4000/api/"

class DataProcessor:
    def __init__(self, server_url):
        self.server_url = server_url
        self.session = requests.Session()  # Reuse session for better performance

    def safe_json_export(self, data):
        """Ensures JSON compliance by replacing NaN/Infinity with None and fixing column names."""
    
        def clean_floats(obj):
            if isinstance(obj, float):
                return None if np.isnan(obj) or np.isinf(obj) else obj
            elif isinstance(obj, dict):
                # Rename columns (modify if needed)
                column_mapping = {
                    "SerialNoProfile": "Serial_No_Profile"  # Rename before exporting
                }
                return {column_mapping.get(k, k): clean_floats(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_floats(i) for i in obj]
            return obj

        return clean_floats(data)


    def fetch_UnprocessedTurbineData(self):
        """Fetch unprocessed turbine data from the server."""
        try:
            response = self.session.get(f"{self.server_url}fetch_UnprocessedTurbineData", timeout=5)
            response.raise_for_status()  # Raise error for HTTP issues
            return response.json().get("data", [])
        except requests.exceptions.RequestException as e:
            print(f" Error fetching unprocessed TurbineData: {e}")
            return []
        
    def fetch_UnprocessedMaterialData(self):
        """Fetch unprocessed material data from the server."""
        try:
            response = self.session.get(f"{self.server_url}fetch_UnprocessedMaterialData", timeout=5)
            response.raise_for_status()  # Raise error for HTTP issues
            return response.json().get("data", [])
        except requests.exceptions.RequestException as e:
            print(f" Error fetching unprocessed MaterialData: {e}")
            return []

        
    def fetch_ProcessedMaterialData(self):
        """Fetch processed material data from the server."""
        try:
            response = self.session.get(f"{self.server_url}fetch_ProcessedMaterialData", timeout=5)
            response.raise_for_status()
            return response.json().get("data", [])
        except requests.exceptions.RequestException as e:
            print(f" Error fetching processed material data: {e}")
            return []
        
    def fetch_ProcessedTurbineData(self):
        """Fetch processed turbine data from the server."""
        try:
            response = self.session.get(f"{self.server_url}fetch_ProcessedTurbineData", timeout=5)
            response.raise_for_status()
            return response.json().get("data", [])
        except requests.exceptions.RequestException as e:
            print(f" Error fetching processed turbine data: {e}")
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

    def exportProcessed_Turbine_data(self, data):
        """Export processed turbine data back to the server."""
        try:
            headers = {'Content-Type': 'application/json'}
            clean_data = self.safe_json_export(data)  # Ensure JSON compliance
            response = self.session.post(f"{self.server_url}uploadProcessedTurbineData", json=clean_data, headers=headers, timeout=5)
            response.raise_for_status()
            print(" Processed TurbineData exported successfully!")
        except requests.exceptions.RequestException as e:
            print(f" Error exporting processed TurbineData: {e}")    
            
    def exportProcessed_Material_data(self, data):
        """Export processed material data back to the server."""
        try:
            headers = {'Content-Type': 'application/json'}
            clean_data = self.safe_json_export(data)  # Ensure JSON compliance
            response = self.session.post(f"{self.server_url}uploadProcessedMaterialData", json=clean_data, headers=headers, timeout=5)
            response.raise_for_status()
            print("Processed MaterialData exported successfully!")
        except requests.exceptions.RequestException as e:
            print(f"Error exporting processed MaterialData: {e}") 
            
    def exportMaterialPredictions_data(self, data):
        """Export material predictions data back to the server."""
        try:
            headers = {'Content-Type': 'application/json'}
            clean_data = self.safe_json_export(data)  # Ensure JSON compliance
            response = self.session.post(f"{self.server_url}uploadMaterialPredictionsData", json=clean_data, headers=headers, timeout=5)
            response.raise_for_status()
            print(" Material predictions data exported successfully!")
        except requests.exceptions.RequestException as e:
            print(f" Error exporting material predictions data: {e}") 
            
    def exportTurbinePredictions_data(self, data):
        """Export turbine predictions data back to the server."""
        try:
            headers = {'Content-Type': 'application/json'}
            clean_data = self.safe_json_export(data)  # Ensure JSON compliance
            response = self.session.post(f"{self.server_url}uploadTurbinePredictionsData", json=clean_data, headers=headers, timeout=5)
            response.raise_for_status()
            print(" Turbine predictions data exported successfully!")
        except requests.exceptions.RequestException as e:
            print(f" Error exporting turbine predictions data: {e}") 

    def run(self):
        """Main execution flow."""
        print("\nFetching unprocessed turbine data from the server...")
        unprocessed_turbine_data = self.fetch_UnprocessedTurbineData()
        
        if unprocessed_turbine_data:
            df_unprocessed_turbine = pd.DataFrame(unprocessed_turbine_data)
            print(f" Unprocessed Turbine Data loaded successfully! {len(df_unprocessed_turbine)} records found.")
            print(df_unprocessed_turbine.head(20))

            # **Process the data using the imported function**
            cleaned_turbine_data = clean_turbine_data(unprocessed_turbine_data)
            updated_turbine_data = update_coordinates(cleaned_turbine_data)

            # **Export the cleaned and updated data**
            self.exportProcessed_Turbine_data(updated_turbine_data)
        else:
            print(" No unprocessed turbine data received.")
            
        print("\nFetching unprocessed material data from the server...")
        unprocessed_material_data = self.fetch_UnprocessedMaterialData()

        if unprocessed_material_data:
            df_unprocessed_material = pd.DataFrame(unprocessed_material_data)
            print(f" Unprocessed Material Data loaded successfully! {len(df_unprocessed_material)} records found.")
            print(df_unprocessed_material.head())

            # **Process the data using the imported function**
            cleaned_material_data = clean_material_data(unprocessed_material_data)

            # **Export the cleaned data**
            self.exportProcessed_Material_data(cleaned_material_data)
        else:
            print(" No unprocessed material data received.")

        print("\nFetching processed material data from the server...")
        processed_material_data = self.fetch_ProcessedMaterialData()

        if processed_material_data:
            df_processed_material = pd.DataFrame(processed_material_data)
            print(f" Processed Material Data loaded successfully! {len(df_processed_material)} records found.")
            print(df_processed_material.head())

            # **Process the data using the new function**
            predictions_result = clean_processed_material_data(df_processed_material)

            if predictions_result["success"]:
                # **Export predictions data**
                self.exportMaterialPredictions_data(predictions_result["data"])
            else:
                print(" No predictions data to export.")
        else:
            print(" No processed data received.")
            
        print("\nFetching processed turbine data from the server...")
        processed_turbine_data = self.fetch_ProcessedTurbineData()

        if processed_turbine_data:
            df_processed_turbine = pd.DataFrame(processed_turbine_data)
            print(f" Processed Turbine Data loaded successfully! {len(df_processed_turbine)} records found.")
            print(df_processed_turbine.head())

            # **Process the data using the new function**
            predictions_result = clean_processed_turbine_data(df_processed_turbine)

            if predictions_result["success"]:
                # **Export predictions data**
                self.exportTurbinePredictions_data(predictions_result["data"])
            else:
                print(" No predictions data to export.")
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
            
            print("Exporting Data:", json.dumps(clean_data, indent=2))

if __name__ == "__main__":
    processor = DataProcessor(SERVER_URL)
    processor.run()

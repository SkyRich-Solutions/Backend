import unittest
import pandas as pd
from Processing_Scripts.clean_processed_data import clean_processed_data
from Processing_Scripts.clean_unprocessed_Material_data import clean_material_data
from Processing_Scripts.clean_unprocessed_Turbine_data import clean_turbine_data, update_coordinates


class TestCleanProcessedData(unittest.TestCase):
    def test_empty_data(self):
        result = clean_processed_data([])
        self.assertFalse(result["success"])
        self.assertEqual(result["message"], "No processed data available")

    def test_valid_data(self):
        input_data = [{"id": 1}]
        result = clean_processed_data(input_data)
        self.assertTrue(result["success"])
        self.assertIn("predicted", result["data"][0])
        self.assertTrue(result["data"][0]["predicted"])


class TestCleanMaterialData(unittest.TestCase):
    def test_empty_data(self):
        result = clean_material_data([])
        self.assertEqual(result, [])

    def test_missing_required_columns(self):
        data = [{"SerialNoProfile": "ZPP2"}]  # Missing ReplacementPart
        result = clean_material_data(data)
        self.assertEqual(result, [])

    def test_violation_correction(self):
        data = [{"SerialNoProfile": "ZPP2", "ReplacementPart": "A"}]
        result = clean_material_data(data)
        self.assertEqual(result[0]["Replacement_Part"], "B")
        self.assertEqual(result[0]["Violation"], 1)
        self.assertTrue(result[0]["cleaned(Unprocessed)"])


class TestCleanTurbineData(unittest.TestCase):
    def test_empty_data(self):
        result = clean_turbine_data([])
        self.assertEqual(result, [])

    def test_predicted_field_present(self):
        data = [{"FunctionalLoc": "XX0001=G001", "predicted": True}]
        result = clean_turbine_data(data)
        self.assertEqual(result[0]["predicted"], True)

    def test_predicted_field_missing(self):
        data = [{"FunctionalLoc": "XX0001=G001"}]
        result = clean_turbine_data(data)
        self.assertTrue(result[0]["predicted"])


class TestUpdateCoordinates(unittest.TestCase):
    def test_missing_maintplant_column(self):
        data = [{"Region": "Europe"}]
        result = update_coordinates(data)
        self.assertEqual(result, data)  # Should return unchanged

    def test_known_country_code(self):
        data = [{"MaintPlant": "DE01", "Latitude": None, "Longitude": None, "Region": "Europe"}]
        result = update_coordinates(data)
        self.assertAlmostEqual(result[0]["Latitude"], 51.1657, places=2)

    # def test_fallback_to_region(self):
    #     data = [{
    #         "MaintPlant": "XX01",
    #         "Region": "Europe",  # exact key
    #         "FunctionalLoc": None,
    #         "Latitude": None,
    #         "Longitude": None
    #     }]
    #     result = update_coordinates(data)
    #     print("Region used:", repr(data[0]["Region"]))
    #     print("Updated result:", result)
    #     self.assertAlmostEqual(result[0]["Latitude"], 50.0000, places=2)

if __name__ == '__main__':
    unittest.main()

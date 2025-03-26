import unittest
import pandas as pd

# Logic copied from Serial_No_Profile notebook
def detect_violations(df):
    serial_no_profiles = ['ZPP2', 'ZPP8', 'ZCS1']
    df['Violation'] = (
        (df['Serial_No_Profile'].isin(serial_no_profiles)) &
        (df['Replacement_Part'] != 'B')
    ).astype(int)
    return df

def correct_violations(df):
    corrected = df.copy()
    corrected.loc[corrected['Violation'] == 1, 'Replacement_Part'] = 'B'
    return corrected

class TestSerialNoProfileLogic(unittest.TestCase):

    def setUp(self):
        self.mock_data = pd.DataFrame([
            {"Serial_No_Profile": "ZPP2", "Replacement_Part": "X"},
            {"Serial_No_Profile": "ZPP8", "Replacement_Part": "B"},
            {"Serial_No_Profile": "ZZZ",  "Replacement_Part": "A"}
        ])

    def test_detect_violations(self):
        result = detect_violations(self.mock_data.copy())
        self.assertEqual(result["Violation"].tolist(), [1, 0, 0])

    def test_correct_violations(self):
        df = detect_violations(self.mock_data.copy())
        corrected = correct_violations(df)
        self.assertEqual(corrected["Replacement_Part"].tolist(), ['B', 'B', 'A'])

if __name__ == "__main__":
    unittest.main()

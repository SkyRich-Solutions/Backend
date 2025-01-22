from .data_loader import load_data_from_mongo
from .db_connection import connect_to_mongo

def process_data():
    """
    Process unprocessed data to resolve discrepancies and save the results.

    Returns:
        tuple: (processed DataFrame, discrepancies DataFrame)
    """
    try:
        # Load data
        df = load_data_from_mongo()
        if df.empty:
            print("No unprocessed data to process.")
            return df, pd.DataFrame()

        # Define serial number profiles for discrepancies
        serial_no_profiles = ['ZPP2', 'ZPP8', 'ZCS1']

        # Add a 'Violation' column
        df['Violation'] = (
            (df['Serial_No_Profile'].isin(serial_no_profiles)) &
            (df['Replacement_Part'] != 'B')
        ).astype(int)

        # Extract rows with violations
        discrepancies = df[df['Violation'] == 1]

        # Resolve discrepancies
        df.loc[
            (df['Serial_No_Profile'].isin(serial_no_profiles)) &
            (df['Replacement_Part'] != 'B'),
            'Replacement_Part'
        ] = 'B'

        # Save processed data to MongoDB
        _, processed_collection = connect_to_mongo()
        processed_collection.delete_many({})  # Clear existing processed data
        processed_collection.insert_many(df.to_dict('records'))

        return df, discrepancies
    except Exception as e:
        print(f"Error processing data: {e}")
        raise e

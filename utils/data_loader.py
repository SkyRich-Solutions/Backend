import pandas as pd
from .db_connection import connect_to_mongo

def load_data_from_mongo():
    """
    Load unprocessed data from the MongoDB collection into a Pandas DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing unprocessed data.
    """
    try:
        unprocessed_collection, _ = connect_to_mongo()
        cursor = unprocessed_collection.find()  # Fetch all documents
        data_list = list(cursor)
        if not data_list:
            print("No data found in the unprocessed collection.")
            return pd.DataFrame()
        df = pd.DataFrame(data_list)
        df.drop(columns=['_id'], inplace=True, errors='ignore')  # Exclude MongoDB's _id field
        return df
    except Exception as e:
        print(f"Error loading data from MongoDB: {e}")
        raise e

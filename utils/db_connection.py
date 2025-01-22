import pymongo
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

def connect_to_mongo():
    """
    Establish a connection to the MongoDB database.

    Returns:
        tuple: (unprocessed_collection, processed_collection)
    """
    try:
        client = pymongo.MongoClient(os.getenv("MONGO_DB_API"))
        db = client['Skyrich-Unprocessed']  # Replace with your database name
        unprocessed_collection = db['UnProcessed v1']  # Collection for unprocessed data
        processed_collection = db['Processed v1']  # Collection for processed data
        return unprocessed_collection, processed_collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        raise e

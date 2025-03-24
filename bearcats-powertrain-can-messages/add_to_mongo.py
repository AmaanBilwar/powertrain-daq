import pymongo
import os
import json
from dotenv import load_dotenv 
from csv_to_json import csv_to_json
load_dotenv()

MONDODB_CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING')  
if not MONDODB_CONNECTION_STRING:
    raise Exception('MONGODB_CONNECTION_STRING not found in .env file')

print(f"Connection string (partial): {MONDODB_CONNECTION_STRING[:15]}...")

try:
    if not os.path.exists("decoded_can_messages.json"):
        csv_to_json("decoded_can_messages.csv")
    if os.path.exists("decoded_can_messages.json"):

        client = pymongo.MongoClient(MONDODB_CONNECTION_STRING, serverSelectionTimeoutMS=5000)
        
        print("Attempting to connect to MongoDB...")
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        db = client['can-messsages']
        collection = db['messages']
        
        # Load the JSON file
        with open('decoded_can_messages.json', 'r') as file:
            data = json.load(file)
        
        # Check if it's a list (multiple records) or a single document
        if isinstance(data, list):
            # Insert many documents at once
            result = collection.insert_many(data)
            print(f"Successfully inserted {len(result.inserted_ids)} documents")
        else:
            # Insert single document
            result = collection.insert_one(data)
            print(f"Successfully inserted document with ID: {result.inserted_id}")
        
        print("Data insertion complete!")

except pymongo.errors.ConnectionFailure as e:
    print(f"Connection error: {e}")
except pymongo.errors.ServerSelectionTimeoutError as e:
    print(f"Server selection timeout: {e}")
except Exception as e:
    print(f"Error: {e}")



import pymongo
import os
from dotenv import load_dotenv 
load_dotenv()



MONDODB_CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING')  
if not MONDODB_CONNECTION_STRING:
    raise print('MONGODB_CONNECTION_STRING not found in .env file')

client =  pymongo.MongoClient()

db = client['can-messsages']
collection = client['can-messsages-01']
try:
    collection.db.insert_one({'message': 'Hello World!'})
    print('Message inserted successfully!')

except Exception as e:
    print('Error inserting message: ', e)


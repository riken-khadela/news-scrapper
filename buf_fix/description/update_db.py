from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError, ConnectionFailure, DuplicateKeyError
import pymongo
MONGO_URI = "mongodb://test_user:asdfghjkl@65.108.33.28:27017/STARTUPSCRAPERDATA-BACKUP?authSource=STARTUPSCRAPERDATA-BACKUP&tls=true&tlsAllowInvalidCertificates=true"

# Connect to MongoDB with timeout settings
try:
    masterclient = MongoClient(MONGO_URI, serverSelectionTimeoutMS=50000)
    masterclient.admin.command('ping')  

    print("MongoDB connection established successfully!")

except ConnectionFailure as e:
    print(f"Could not connect to MongoDB: {e}")
clientdb = masterclient['STARTUPSCRAPERDATA']
crunch_organization_details=clientdb.OrganiztionDetails

def update_db(documents : list):
    """
    Update the 'description' field of a document matching the given 'url' in MongoDB.

    Args:
        mongo_uri (str): MongoDB connection string
        db_name (str): Name of the database
        collection_name (str): Name of the collection
        url (str): The URL to match
        new_description (str): The new description to set
    """
    
    for document in documents:
        url = document['url']
        new_description = document['description']
        
        try:
            result = crunch_organization_details.update_one(
                {"url": url},
                {"$set": {"description": new_description}}
            )
            if result.matched_count == 0:
                print(f"No document found with url: {url}")
            elif result.modified_count == 0:
                print(f"Document with url '{url}' found, but description not updated (possibly same).")
            else:
                print(f"Successfully updated description for url: {url}")
        except Exception as e:
            print(f"Error updating document for {url}: {e}")
         
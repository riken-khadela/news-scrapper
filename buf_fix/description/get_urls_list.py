URLS = [
    "https://www.crunchbase.com/organization/pirate-land"
]
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError, ConnectionFailure, DuplicateKeyError
MONGO_URI = "mongodb://test_user:asdfghjkl@65.108.33.28:27017/STARTUPSCRAPERDATA-BACKUP?authSource=STARTUPSCRAPERDATA-BACKUP&tls=true&tlsAllowInvalidCertificates=true"

try:
    masterclient = MongoClient(MONGO_URI, serverSelectionTimeoutMS=50000)  # 50 seconds timeout
    masterclient.admin.command('ping')  # Ping the server to check if it is available
    print("MongoDB connection established successfully!")
except ConnectionFailure as e:
    print(f"Could not connect to MongoDB: {e}")

clientdb = masterclient['STARTUPSCRAPERDATA']
crunch_raw_urls = clientdb.Crunch_Link

def reset_all_statuses():
    result = crunch_raw_urls.update_many(
        {}, 
        {"$set": {"status": "pending"}}
    )
    print(f"Reset {result.modified_count} documents to status='pending'.")

def get_the_crunch_links(number_records = 1):
    query = {
        "$or": [
            {"status": {"$exists": False}},
            {"status": "pending"}
        ]
    }

    result = list(crunch_raw_urls.find(query).limit(number_records))
    
    for doc in result:
        crunch_raw_urls.update_one(
            {"_id": doc["_id"]},
            {"$set": {"status": "updated"}}
        )
    return result
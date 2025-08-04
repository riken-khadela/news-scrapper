from ast import main
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError, ConnectionFailure, DuplicateKeyError
import datetime
import time
import pymongo
import urllib, requests, time, re
import base64, pytz, os, json, random, time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from get_urls_list import get_the_crunch_links, reset_all_statuses, URLS
from summery import SUMMARY
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, ConnectionFailure
MONGO_URI = "mongodb://test_user:asdfghjkl@65.108.33.28:27017/STARTUPSCRAPERDATA-BACKUP?authSource=STARTUPSCRAPERDATA-BACKUP&tls=true&tlsAllowInvalidCertificates=true"

try:
    masterclient = MongoClient(MONGO_URI, serverSelectionTimeoutMS=50000)  # 50 seconds timeout
    masterclient.admin.command('ping')  # Ping the server to check if it is available
    print("MongoDB connection established successfully!")
except ConnectionFailure as e:
    print(f"Could not connect to MongoDB: {e}")

clientdb = masterclient['STARTUPSCRAPERDATA']
child_keywords = clientdb.Crunch_Keywords 
crunch_raw_urls = clientdb.CrunchURLS
crunch_organization_details=clientdb.OrganiztionDetails
blacklisted_keywords=clientdb.Blacklist_keywords

class Main(SUMMARY):
    
    def main(self):
        breakpoint()
        while True :
            data = []
            crunch_links = get_the_crunch_links()
            # crunch_links = URLS
            for link in crunch_links :
                print(f"Processing URL: {link['organization_url']}")
                desc_data = self.summary_process_logic(link['organization_url'])
                if desc_data :
                    data.append(desc_data)
                    
            self.update_db(data)
                
    def update_db(self, data_list : list):
        bulk_operations = []
    
        for data in data_list:
            url, description = data[0], data[-1]

            op = UpdateOne(
                {
                    "organization_url": url,
                    "summary.details.description": "",
                },
                {
                    "$set": {"summary.details.description": description}
                }
            )
            bulk_operations.append(op)

        if bulk_operations:
            try:
                result = crunch_organization_details.bulk_write(bulk_operations, ordered=False)
                print(f"Bulk update completed: {result.modified_count} documents updated.")
            except BulkWriteError as bwe:
                print(f"Bulk write error occurred: {bwe.details}")
        
Main().main()

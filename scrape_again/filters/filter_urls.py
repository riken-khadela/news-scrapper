import random, pymongo, logging, re, time, json
from pymongo import MongoClient, UpdateOne, InsertOne, ReplaceOne
from pymongo.errors import BulkWriteError
from scrape_again.logger import CustomLogger
import datetime
import pytz

tz = pytz.timezone('Asia/Kolkata')

CONFIG_FILE = "config.json"
with open(CONFIG_FILE, "r") as f: config = json.load(f)
mongo_db_connection = config["mongo_db_connection"]
BATCH_SIZE = 5000

logger = CustomLogger("logs/filter_urls.log")
masterclient = MongoClient(mongo_db_connection)
news_scrapper = masterclient.NEWSSCRAPER
sector_collection = news_scrapper.keywords

news_scrapper_data = masterclient.NEWSSCRAPERDATA
news_url = news_scrapper_data.news_url
news_details = news_scrapper_data.new_details
new_details_main_urls_only_temp = news_scrapper_data.new_details_main_urls_only_temp

def filter(data):
    return data

def upsert_data_from_one_collection_to_another(source_urls_list, target_collection):
    bulk_operations = []
    processed_count = 0
    
    for url in source_urls_list:
        if not url or url == "":
            continue
        
        tmp_data = {
            "url": url,
            "created_at": datetime.datetime.now(tz),
            "is_read": 0
        }
        bulk_operations.append(
            ReplaceOne(
                {"url": url},
                tmp_data,
                upsert=True
            )
        )
        
        processed_count += 1
        
        if len(bulk_operations) >= BATCH_SIZE:
            try:
                result = new_details_main_urls_only_temp.bulk_write(bulk_operations, ordered=False)
                inserted_count += result.upserted_count + result.modified_count
                print(f"Progress: {processed_count}/{len(unique_urls)} | Inserted: {inserted_count}")
            except Exception as e:
                print(f"Error: {e}")
            
            bulk_operations = []

    if bulk_operations:
        try:
            result = new_details_main_urls_only_temp.bulk_write(bulk_operations, ordered=False)
            inserted_count += result.upserted_count + result.modified_count
        except Exception as e:
            print(f"Error: {e}")
        

# refine the urls
def news_details_to_new_url():

    
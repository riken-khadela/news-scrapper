import inc42 
import tech_crunch
import your_story
import theverge
import digitaltrands

import random, pymongo, logging, re
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
logger = logging.getLogger(__name__)
masterclient = MongoClient("mongodb://vinayj:7x34gkm5@65.108.33.28:27017/?authSource=TRAKINTELSCRAPER&authMechanism=SCRAM-SHA-256&readPreference=primary&directConnection=true&tls=true&tlsAllowInvalidCertificates=true")
news_scrapper = masterclient.NEWSSCRAPER
sector_collection = news_scrapper.keywords

news_scrapper_data = masterclient.NEWSSCRAPERDATA
news_url_1 = news_scrapper_data.news_url_1
news_details_1 = news_scrapper_data.news_details_1

def collect_links(numberofrecords = 10):
    updates = []
    random_documents = []
    where_condition = {"is_read":0}
    pipeline = [{"$match": where_condition}, {"$sample": {"size": numberofrecords}}]
    random_documents = list(news_url_1.aggregate(pipeline))
    if len(random_documents) == 0:
        random_documents = list(news_url_1.aggregate(pipeline))
    for row in random_documents:
        updates.append(UpdateOne({"_id": row["_id"]}, {"$set": {"is_read": 1}}))

    # if updates :
    #     result = news_url_1.bulk_write(updates)
    return random_documents

def format_field(value):
    values = value.split("|")
    result = {}
    non_blank_index = 1  

    for val in values:
        val = val.strip()
        if val:  
            result[str(non_blank_index)] = val
            non_blank_index += 1

    return result

def insert_news_details(data):
    print("Total records received: %d", len(data))
    bulk_operations = []
    
    for obj in data:
        try:
            if not obj : continue
            existing_doc = news_details_1.find_one({"url": obj["url"]})
            url=obj["url"]
            regex_pattern = f'^{re.escape(url)}$'
            doc = news_url_1.find_one({"url": {"$regex": regex_pattern, "$options": 'i'}})
            
            if doc:
                append_fields = {
                    "count": doc["count"],
                    "google_page": doc["google_page"],
                    "index": doc["index"],
                    "search_tag": format_field(doc["tag"]),
                    "search_sector": format_field(doc["sector"])  
                }
                obj.update(append_fields)
                
            if not existing_doc:
                bulk_operations.append(pymongo.InsertOne(obj))
                
            if existing_doc:
                bulk_operations.append(UpdateOne(
                    {"url": obj["url"]},
                    {"$set": obj},
                    upsert=True
                ))
        except:
            print("Error processing URL: %s", obj.get("url", "Unknown"))
            continue
    try:
        print("Total numbers of records get: %d", len(bulk_operations))
        
        if bulk_operations:

            result = news_details_1.bulk_write(bulk_operations)
            print("Total inserted records: %d", len(bulk_operations))
    except BulkWriteError as e:
        print(e.details)
        print("An error occurred: %s", e.details)


def main():
    
    data = []
    print("Collecting links from MongoDB...",len(collect_links()))
    for link in collect_links():
        url = link['url']
        
        if "techcrunch" in url:
            data.append(tech_crunch.scrape(link['url']))
            
        elif "theverge" in url:
            data.append(theverge.scrape(url))
            
        elif "digitaltrends" in url:
            data.append(digitaltrands.scrape(url))
            
        elif "yourstory" in url :
            data.append(your_story.scrape(url))
            
        elif "inc42" in url :
            data.append(inc42.scrape(url))
            
            
    if data :
        insert_news_details(data)
        data = []
        
        
if __name__ == "__main__":
    while True :main()

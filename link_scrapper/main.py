import inc42
import your_story
import theverge
import digitaltrends
import tech_crunch
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

KEYWORDS = [
    "Funding & Investment",
    "Product & Technology Innovation",
    "Acquistions"
]

LOCATION = ["US"]

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

def insert_multiple_urls_from_google(documents):
    news_url_1.create_index([("url", 1)], unique=True)
    bulk_operations = []
    update_details=[]

    for obj in documents:
        try:
            existing_doc = news_url_1.find_one({"url": obj["url"]})
            if not existing_doc:
                bulk_operations.append(pymongo.InsertOne(obj))
                
            if existing_doc:
                logger.info("===[ Duplicate Found ]===")
                Sector = existing_doc.get('sector')
                Tag = existing_doc.get('tag')
                
                if obj['sector'] and str(obj['sector']).strip() not in  str(existing_doc.get('sector')).strip():
                    Sector = str(existing_doc.get('sector'))+'|'+str(obj['sector'])
                    
                if obj['tag'] and str(obj['tag']).lower().strip() not in  str(existing_doc.get('tag')).lower().strip():
                    Tag = str(existing_doc.get('tag'))+'|'+str(obj['tag'])
                
                COUNT =existing_doc.get('count') + 1
                update_query = {
                    "$set": {
                        'count': COUNT, 
                        'sector' : Sector,
                        'tag' : Tag,
                    },
                }
                bulk_operations.append(UpdateOne({"_id": existing_doc["_id"]}, update_query))
                
                url=obj["url"]
                regex_pattern = f'^{re.escape(obj["url"])}$'
                doc = news_details_1.find_one({"url": {"$regex": regex_pattern, "$options": 'i'}})
                if doc:
                    append_fields = {
                        "search_sector": format_field(Sector),
                        "search_tag": format_field(Tag),
                        "count": COUNT
                    }

                    update_details.append(UpdateOne({"_id": doc["_id"]}, {"$set": append_fields}))
        except Exception as e:
            logger.error("Error processing document: %s", str(e))

    try:
        if bulk_operations:
            result = news_url_1.bulk_write(bulk_operations)
            logger.info("===>>  Total Inserted Records: %d", result.inserted_count)
       
        if update_details:
            result = news_details_1.bulk_write(update_details)
            logger.info("===>>  Total Updated Records: %d", result.modified_count)

    except BulkWriteError as e:
        logger.error("Bulk write error: %s", e.details)

def get_sector_data():
    return sector_collection.find()

def main():
    all_urls = []
    for keyword in KEYWORDS :
        for sector in get_sector_data() :

            tech_crunch_ = tech_crunch.collect_page_details(sector, keyword, LOCATION)
            if tech_crunch_ :
                all_urls.extend(tech_crunch_)

            the_verge_ = theverge.collect_page_details(sector, keyword, LOCATION)
            if the_verge_ :
                all_urls.extend(the_verge_)

            digital_trends_ = digitaltrends.collect_page_details(sector, keyword, LOCATION)
            if digital_trends_ :
                all_urls.extend(digital_trends_)
            
            inc42_ = inc42.collect_page_details(sector, keyword, LOCATION)
            if inc42_ :
                all_urls.extend(inc42_)
            
            your_story_ = your_story.collect_page_details(sector, keyword, LOCATION)
            if your_story_ :
                all_urls.extend(your_story_)
            
            if all_urls :
                insert_multiple_urls_from_google(all_urls)
                all_urls = []
        

if __name__ == "__main__":
    main()
    
    
    
    """
    print(sector, keyword)
            inc42_ = digitaltrends.collect_page_details(sector, "", LOCATION)
            # inc42_ = inc42(sector, keyword, LOCATION)
            if inc42_ :
                breakpoint()
            # if inc42_ :
            #     all_urls.extend(inc42_)
            # """
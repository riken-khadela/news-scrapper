from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError, ConnectionFailure, DuplicateKeyError
import datetime
import time
import pymongo
import urllib, requests, time, re
import base64, pytz, os, json, random, time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from catch_coockies import CatchCookies

tz = pytz.timezone('Asia/Kolkata')
current_time = datetime.now(tz)

import pymongo.errors
from logger import CustomLogger

tz = pytz.timezone('Asia/Kolkata')
current_time = datetime.now(tz)

logger = CustomLogger(log_file_path="log/new_scrapping.log")
# Local MongoDB URI (assuming the default localhost connection)
MONGO_URI = "mongodb://test_user:asdfghjkl@65.108.33.28:27017/STARTUPSCRAPERDATA-BACKUP?authSource=STARTUPSCRAPERDATA-BACKUP&tls=true&tlsAllowInvalidCertificates=true"

# Connect to MongoDB with timeout settings
try:
    masterclient = MongoClient(MONGO_URI, serverSelectionTimeoutMS=50000)  # 50 seconds timeout
    # Verify the connection
    masterclient.admin.command('ping')  # Ping the server to check if it is available

    print("MongoDB connection established successfully!")

except ConnectionFailure as e:
    print(f"Could not connect to MongoDB: {e}")
    # Optionally, you can exit or handle the error based on your app requirements
    

masterdb = masterclient.STARTUPSCRAPERDATA 
master_keywords = masterdb.Crunch_Keywords 

### Prod DB ####
clientdb = masterclient['STARTUPSCRAPERDATA']
child_keywords = clientdb.Crunch_Keywords
crunch_raw_urls = clientdb.CrunchURLS
crunch_organization_details=clientdb.OrganiztionDetails
blacklisted_keywords=clientdb.Blacklist_keywords

# added 15-09-2023 
def format_location(value):
    values = value.split(",")
    result = {}
    non_blank_index = 1  # Initialize the index for non-blank values

    for val in values:
        val = val.strip()  # Remove leading and trailing whitespace
        if val:  # Check if the value is not empty after stripping
            result[str(non_blank_index)] = val
            non_blank_index += 1  # Increment the index for non-blank values
    return result

def format_field(value):
    values = value.split("|")
    result = {}
    non_blank_index = 1  # Initialize the index for non-blank values

    for val in values:
        val = val.strip()  # Remove leading and trailing whitespace
        if val:  # Check if the value is not empty after stripping
            result[str(non_blank_index)] = val
            non_blank_index += 1  # Increment the index for non-blank values

    return result
# end

#Function to reset all the keywords

def reset_all_keywords():
    print("Resetting the database")
    child_keywords.delete_many({})
    print("All old records deleted")
    
    # Get the list of blacklisted keywords as a set for efficient membership testing
    blacklist_keywords = set(blk['orgkeyword'] for blk in blacklisted_keywords.find())
    # Define the pipeline for selecting random documents
    number_of_records = 1000
    where_condition = {"is_read": 0}
    pipeline = [{"$match": where_condition}, {"$sample": {"size": number_of_records}}]
    
    # Iterate through random documents and filter out blacklisted keywords
    new_keywords = []
    for doc in master_keywords.aggregate(pipeline):
        if doc['orgkey'] not in blacklist_keywords:
            new_keywords.append({
                'sector': str(doc['sector']).lower(),
                'orgkey': doc['orgkey'],
                'tag': str(doc['tag']).lower(),
                'status': 0
            })
            # Mark the document as read
            master_keywords.update_one({"_id": doc["_id"]}, {"$set": {"is_read": 1}})
    
    # Insert new keywords into the child_keywords collection
    if new_keywords:
        try:
            child_keywords.insert_many(new_keywords, ordered=False)
        except BulkWriteError as e:
            # Handle any errors that occurred during bulk insertion
            pass

#keywords to read for google

def check_and_reset_keywords():
    numberofrecords = 1
    where_condition = {"status":0}
    pipeline = [{"$match": where_condition}, {"$sample": {"size": numberofrecords}}]
    random_documents = list(child_keywords.aggregate(pipeline))
    # update each document with a new value
    if len(random_documents) == 0:
        reset_all_keywords()
    for row in random_documents:
        child_keywords.update_one({"_id": row["_id"]}, {"$set": {"status": 1}})

# Function Used in crunch_link_scraper
def read_crunch_keywords():
    logger = CustomLogger(log_file_path="log/new_scrapping.log")
    
    bulk_operations = []
    numberofrecords = 1600
    where_condition = {"status":0}
    where_condition = {"$or": [{"is_read": 0}, {"status": "pending"}]} #{"is_read":0}
    # "$or": [{"is_read": 0}, {"status": "pending"}]
    pipeline = [{"$match": where_condition}, {"$sample": {"size": numberofrecords}}]
    random_documents = list(child_keywords.aggregate(pipeline))
    for row in random_documents:
        bulk_operations.append(UpdateOne({"_id": row["_id"]}, {"$set": {"status": 1}}))
    try:
        if bulk_operations:
            result = child_keywords.bulk_write(bulk_operations)
        
    except BulkWriteError as e:
        logger.error(e.details)
        
    # update each document with a new value
    return random_documents

def update_read_stat_keywords(row):
    child_keywords.update_one({"_id": row["_id"]}, {"$set": {"status": 1}})

def update_read_stat_urls(data):
    for row in data:
        crunch_raw_urls.update_one({"_id": row["_id"]}, {"$set": {"status": 'completed'}})
    
    
# def read_crunch_urls(numberofrecords):
#     # Santhosh: Query modified to read the records with "is_read=0 OR status=pending"
#     where_condition = {"$or": [{"is_read": 0}, {"status": "pending"}]} #{"is_read":0}
#     pipeline = [{"$match": where_condition}, {"$sample": {"size": numberofrecords}}]
#     random_documents = list(crunch_raw_urls.aggregate(pipeline))
#     # update each document with a new value
#     if len(random_documents) == 0:
#         random_documents = list(crunch_raw_urls.aggregate(pipeline))
#     # for row in random_documents:
#     #     crunch_raw_urls.update_one({"_id": row["_id"]}, {"$set": {"is_read": 1}})
#     return random_documents


def read_crunch_urls(numberofrecords):
    one_month_ago = current_time - timedelta(days=30)
    where_created_at_priority = {
        "$and": [
            {"$or": [{"is_read": 0}, {"status": "pending"}]},
            {"$or": [
                {"created_at": {"$lt": one_month_ago}},
                {"created_at": {"$exists": False}}
            ]}
        ]
    }
    pipeline = [{"$match": where_created_at_priority}, {"$sample": {"size": numberofrecords}}]
    random_documents = list(crunch_raw_urls.aggregate(pipeline))
    
    if len(random_documents) == 0:
        where_condition_fallback = {
            "$or": [{"is_read": 0}, {"status": "pending"}]
        }
        pipeline = [{"$match": where_condition_fallback}, {"$sample": {"size": numberofrecords}}]
        random_documents = list(crunch_raw_urls.aggregate(pipeline))
    
    bulk_updates = []
    for row in random_documents:
        bulk_updates.append(
            UpdateOne(
                {"_id": row["_id"]},
                {"$set": {
                    "is_read": 1,
                    "last_processed_at": current_time,
                    "status": "processing"
                }}
            )
        )
    
    if bulk_updates:
        crunch_raw_urls.bulk_write(bulk_updates)
    
    return random_documents

# Function Used in crunch_link_scraper        
#17-09-2023 made the necessary changes.

def insert_multiple_urls_from_google(documents):
    # set a unique index on the collection
    print("total records found: " +str(len(documents)))
    
    crunch_raw_urls.create_index([("url", 1)], unique=True)
    bulk_operations = []
    new_data_bulk_operations=[]
    update_org_details=[]
    for obj in documents:
        try:
            # Check if the URL already there
            existing_doc = crunch_raw_urls.find_one({"url": obj["url"]})
            if not existing_doc:
                new_data_bulk_operations.append(pymongo.InsertOne(obj))
            
            if existing_doc:
                print("found")
                
                Sector = existing_doc.get('sector')
                Tag = existing_doc.get('tag')
                OrgKey = existing_doc.get('orgkey')
                if str(obj['sector']).strip() not in  str(existing_doc.get('sector')).lower().strip():
                    Sector = str(existing_doc.get('sector'))+'|'+str(obj['sector'])
                    
                if str(obj['tag']).lower().strip() not in  str(existing_doc.get('tag')).lower().strip() :
                    Tag = str(existing_doc.get('tag'))+'|'+str(obj['tag'])
                
                if str(obj['orgkey']).strip() not in  str(existing_doc.get('orgkey')).lower().strip() :
                    OrgKey = str(existing_doc.get('orgkey'))+'|'+str(obj['orgkey'])
                COUNT= existing_doc.get('count') + 1 # added 15-09-2023
                update_query = {
                    "$set": {
                        'count': COUNT,# added 15-09-2023
                        'sector' : Sector,
                        'tag' : Tag,
                        'orgkey' : OrgKey,
                        'update_first' : 1
                    },
                }
                # added 15-09-2023
                regex_pattern = f'^{re.escape(obj["url"])}$'
                doc = crunch_organization_details.find_one({"url": {"$regex": regex_pattern, "$options": 'i'}})
                if doc:
                    append_fields = {
                        "search_sector": format_field(Sector),
                        "search_keyword": format_field(OrgKey),
                        "search_tag":format_field(OrgKey),
                        "count": COUNT,
                        'update_first' : 1
                    }

                    update_org_details.append(UpdateOne({"_id": doc["_id"]}, {"$set": append_fields}))
                # end
                bulk_operations.append(UpdateOne({"_id": existing_doc["_id"]}, update_query))
    
        except Exception as e:
                pass
    print("total new records found: " +str(len(new_data_bulk_operations)))
    
    print("total old records updated: " +str(len(bulk_operations)))
    
    try:
        if bulk_operations:
            result = crunch_raw_urls.bulk_write(bulk_operations)
        if new_data_bulk_operations:
            result = crunch_raw_urls.bulk_write(new_data_bulk_operations)
        # added 15-09-2023
        if update_org_details:
            result = crunch_organization_details.bulk_write(update_org_details)
    except BulkWriteError as e:
        print(e.details)

# Function Used in crunch_link_scraper
def insert_blacklist_keywords(documents):
    try:
        blacklisted_keywords.create_index([("orgkeyword", 1)], unique=True)
        result = blacklisted_keywords.insert_one(documents)
        print("No Links Found - Keyword inserted into Blacklist_keywords.")
    except pymongo.errors.DuplicateKeyError:
        print("Blacklist Keyword with the same key already exists. Skipping insertion.")
    except Exception as e:
        print("An error occurred:", str(e))
        
# def insert_organisation_details(data):
#     bulk_operations = []
    
#     for obj in data:
#         try :    
#             where_condition = {"$or": [{"url": data['organization_url']}]} 
#             pipeline = [{"$match": where_condition}, {"$sample": {"size": 10}}]
#             random_documents = list(crunch_organization_details.aggregate(pipeline))
#             if random_documents :
#                 continue
#         except Exception as e: 
#             print(f"Error in insert_organisation_details to finding same object exists or not details: {e}")
        
#         try:
#             regex_pattern = f'^{re.escape(obj["organization_url"])}$'
#             doc = crunch_raw_urls.find_one({"url": {"$regex":regex_pattern, "$options": 'i'}})
#             if doc:
#                 append_fields = {   
#                     "count": doc["count"],
#                     "google_page": doc["google_page"],
#                     "index": doc["index"],
#                     "search_keyword": format_field(doc["orgkey"]),
#                     "search_sector": format_field(doc["sector"]),
#                     "search_tag": format_field(doc["tag"]),
#                 }
#                 obj.update(append_fields)
#             bulk_operations.append(pymongo.InsertOne(obj))
#         except Exception as e:
#             print(f"Error in insert_organisation_details details: {e}")
#             continue
        
#     try:
#         if bulk_operations:
#             result = crunch_organization_details.bulk_write(bulk_operations)
#     except BulkWriteError as e:
#         print(e.details)
        
#     except Exception as e:
#         print(f"Error in insert_organisation_details details: {e}")
def insert_organisation_details(data):
    current_time = datetime.now(tz)
    
    bulk_operations = []
    stats = {
        'new_organizations': 0,
        'updated_organizations': 0,
        'errors': 0
    }

    for obj in data:
        try:
            existing_org = crunch_organization_details.find_one(
                {"organization_url": obj["organization_url"]}
            )
            
            url_doc = crunch_raw_urls.find_one(
                {"url": {"$regex": f'^{re.escape(obj["organization_url"])}$', "$options": 'i'}}
            )
            
            base_fields = {
                "updated_at": current_time,
                "is_updated": 0 
            }
            
            if url_doc:
                base_fields.update({
                    "count": url_doc.get("count"),
                    "google_page": url_doc.get("google_page"),
                    "index": url_doc.get("index"),
                    "search_keyword": format_field(url_doc.get("orgkey")),
                    "search_sector": format_field(url_doc.get("sector")),
                    "search_tag": format_field(url_doc.get("tag")),
                    "source_url_id": url_doc["_id"]  # Reference to original URL
                })
            
            if not existing_org:
                org_data = {
                    **obj,
                    **base_fields,
                    "created_at": current_time,
                    "is_updated": 1  
                }
                bulk_operations.append(InsertOne(org_data))
                stats['new_organizations'] += 1
            else:
                update_data = {
                    **obj,
                    **base_fields
                }
                bulk_operations.append(
                    UpdateOne(
                        {"_id": existing_org["_id"]},
                        {"$set": update_data}
                    )
                )
                stats['updated_organizations'] += 1
                
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error processing {obj.get('organization_url')}: {str(e)}")
            continue
    
    try:
        if bulk_operations:
            result = crunch_organization_details.bulk_write(bulk_operations)
            logger.log(f"Organization update stats: {stats}")
            return {
                'success': True,
                'stats': stats
            }
    except BulkWriteError as e:
        logger.error(f"Bulk write error: {e.details}")
        return {
            'success': False,
            'error': str(e),
            'stats': stats
        }
    
    return {
        'success': True,
        'stats': stats
    }
### New code added by Santhosh #########

def getToken():
    ''' 
        Author: Santhosh
        Description: This function reads the base64 encoded token for Scrapedo from
                    the config.json file in the script path and returns the 
                    base64 decoded tpoekn to be used the proxy URL
    '''
    try:
        script_path = os.path.dirname(os.path.realpath(__file__))
        config_file_path = os.path.join(script_path, "config.json")
        with open(config_file_path) as config_file:
            token = json.load(config_file)['token_1']
        return token.strip()
    except Exception as e:
        print("Error reading token:", e)
        return None

def getProxies():
    ''' 
        Author: Santhosh
        Description: Returns the proxy list
    '''
    token = getToken()
    TIMEOUT = 30000  # milliseconds
    proxy_url = f"http://proxy.scrape.do:8080?token={token}&timeout={TIMEOUT}"
    return {
        "http": proxy_url,
        "https": proxy_url,
    }


def proxies():
    plist = [
        '5.79.73.131:13150'
    ]
    prx = random.choice(plist)
    return {
        'http': 'http://' + prx,
        'https': 'http://' + prx
    }
    
 
CONFIG_FILE = "config.json"

with open(CONFIG_FILE, "r") as f: config = json.load(f)
    
TOKEN = config["token_1"]
COOKIE_FILE = "session_data.json"
OUTPUT_DIR = "results"

def load_session():
    with open(COOKIE_FILE, "r") as f: data = json.load(f)
    return data["session_id"], urllib.parse.quote_plus(data["cookies"])

time_sleep = [i / 100 for i in range(10, 991, 10)]

def get_scrpido_requests(company_name : str, session_id, cookies):
    with open(COOKIE_FILE, "r") as f: data = json.load(f)
    if not company_name.startswith("https://www.crunchbase.com/organization/") :
        
        if "/organization/" in company_name :
            company_name = company_name.split('organization')[-1]
        company_url  = f"https://www.crunchbase.com/organization/{company_name.replace(' ', '-').lower()}"
        
        if "organization//" in company_url :
            company_url = company_url.replace('organization//','organization/')
        
    else :
        company_url = company_name
        
    encoded_url = urllib.parse.quote_plus(company_url)
    for _ in range(4):
        url = f"https://api.scrape.do/?token={TOKEN}&url={encoded_url}&render=true&sessionid={session_id}&setCookies={cookies}"
        try:
            response = requests.get(url)
            if response.status_code  == 200 :
                
                bs4_data = BeautifulSoup(response.text, 'lxml')
                success_login = bs4_data.find_all('button',{'aria-label':'Account'})
                if not success_login :
                    with open(COOKIE_FILE, "w") as f:
                        data['status_update'] = False
                        json.dump(data, f, indent=2)
                    # refresh_session()
                    
                    session_id, cookies = load_session()
                    continue
                    
                logger.log(f"Scraped: {company_name}")
                return True, response
            else :
                logger.error(f"Scrapped: but statuscode : {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to scrape {company_name}: {e}")
        
    return False, False



class main_setting:
    
    def __init__(self):
        self.logger = CustomLogger(log_file_path="log/new_scrapping.log")
        self.session_manager = CatchCookies()

    def random_sleep(self,a = 3, b = 7, reason = ""):
        sleep_t = random.randint(a,b)
        self.logger.log(f"Time sleep : {sleep_t} secounds ") if not reason else self.logger.log(f"Time sleep : {sleep_t} secounds for {reason}") 
        
    def read_crunch_details_new(self, numberofrecords):
        
        current_time = datetime.utcnow()
        one_month_ago = current_time - timedelta(days=30)

        # Step 1: Get all organization_urls to avoid duplicates
        existing_urls = set(
            doc["organization_url"]
            for doc in crunch_organization_details.find({"organization_url": {"$ne": None}}, {"organization_url": 1})
        )

        def filter_out_existing(docs):
            return [doc for doc in docs if doc.get("url") not in existing_urls]

        final_docs = []

        # Priority 1: Only status = pending
        priority1_match = {"status": "pending"}
        docs1 = list(crunch_raw_urls.aggregate([
            {"$match": priority1_match},
            {"$sample": {"size": numberofrecords * 2}}  # sample more than needed to filter
        ]))
        docs1 = filter_out_existing(docs1)
        final_docs.extend(docs1[:numberofrecords])

        if len(final_docs) < numberofrecords:
            # Priority 2: is_read=0 or status=pending AND created_at < one_month_ago or not exists
            remaining = numberofrecords - len(final_docs)
            priority2_match = {
                "$and": [
                    {"$or": [{"is_read": 0}, {"status": "pending"}]},
                    {"$or": [
                        {"created_at": {"$lt": one_month_ago}},
                        {"created_at": {"$exists": False}}
                    ]}
                ]
            }
            docs2 = list(crunch_raw_urls.aggregate([
                {"$match": priority2_match},
                {"$sample": {"size": remaining * 2}}
            ]))
            docs2 = filter_out_existing(docs2)
            final_docs.extend(docs2[:remaining])

        if len(final_docs) < numberofrecords:
            # Priority 3: is_read = 0 or status = pending (no created_at filter)
            remaining = numberofrecords - len(final_docs)
            priority3_match = {
                "$or": [{"is_read": 0}, {"status": "pending"}]
            }
            docs3 = list(crunch_raw_urls.aggregate([
                {"$match": priority3_match},
                {"$sample": {"size": remaining * 2}}
            ]))
            docs3 = filter_out_existing(docs3)
            final_docs.extend(docs3[:remaining])

        # # Perform updates
        # bulk_updates = [
        #     UpdateOne(
        #         {"_id": row["_id"]},
        #         {"$set": {
        #             "is_read": 1,
        #             "last_processed_at": current_time,
        #             "status": "processing"
        #         }}
        #     ) for row in final_docs
        # ]

        # if bulk_updates:
        #     crunch_raw_urls.bulk_write(bulk_updates)
        # final_docs = list(crunch_raw_urls.aggregate([
        #                 {
        #                     "url": "https://www.crunchbase.com/organization/syby"
        #                 }
        #             ]))
        
        # print(final_docs)
        return final_docs
    
    def load_session(self):
        try:
            with open(COOKIE_FILE, "r") as f:
                data = json.load(f)
            return data["session_id"], urllib.parse.quote_plus(data["cookies"])
        except Exception as e:
            self.logger.error(f"Failed to load cookies: {e}")
            return None, None

    def get_scrpido_requests(self, company_name: str, session_id=None, cookies=None):
        if not session_id or not cookies:
            self.session_manager.refresh_session()
            session_id, cookies = self.load_session()

        if not company_name.startswith("https://www.crunchbase.com/organization/"):
            if "/organization/" in company_name:
                company_name = company_name.split('organization')[-1]
            company_url = f"https://www.crunchbase.com/organization/{company_name.replace(' ', '-').lower()}"
            if "organization//" in company_url:
                company_url = company_url.replace('organization//', 'organization/')
        else:
            company_url = company_name

        encoded_url = urllib.parse.quote_plus(company_url)

        for _ in range(4):
            url = f"https://api.scrape.do/?token={TOKEN}&url={encoded_url}&super=true&sessionid={session_id}&setCookies={cookies}"
            try:
                response = requests.get(url)
                if response.status_code == 429:
                    self.random_sleep(reason="Error status code 429")
                    
                elif response.status_code == 404:
                    self.logger.error(f"Company Not found : {company_name}")
                    self.logger.error(f"Company Not found : {company_name}")
                    file_name = "404_new.html"
                    with open(file_name, '+w') as f:f.write(response.text)
                    crunch_raw_urls.update_one(
                        {"url": company_name,},
                        {
                            "$set": {"status": "completed"},
                        }
                    )
                    return False, False
                elif response.status_code == 502:
                    self.logger.error(f"Company Not found : {company_name}")
                    file_name = "502_new.html"
                    with open(file_name, '+w') as f:f.write(response.text)
                
                elif response.status_code == 200:
                    success_login = BeautifulSoup(response.text, 'lxml').find_all('button',{'aria-label':'Account'})
                    if not success_login or self.session_manager.needs_refresh():
                        self.session_manager.refresh_session()
                        
                    self.logger.log(f"Scraped: {company_name}")
                    return True, response
                else:
                    self.logger.error(f"Scrapped but statuscode : {response.status_code} : company url --> {company_name}")
            except Exception as e:
                self.logger.error(f"Failed to scrape {company_name}: {e}")

        return False, False

    def update_read_stat_urls(self,data):
        for row in data:
            crunch_raw_urls.update_one({"_id": row["_id"]}, {"$set": {"status": 'completed'}})
        
    
    def insert_organisation_details(self,data):
        current_time = datetime.now(tz)
        
        bulk_operations = []
        stats = {
            'new_organizations': 0,
            'updated_organizations': 0,
            'errors': 0
        }
        for obj in data:
            try:
                existing_org = crunch_organization_details.find_one(
                    {"organization_url": obj["organization_url"]}
                )
                
                url_doc = crunch_raw_urls.find_one(
                    {"url": {"$regex": f'^{re.escape(obj["organization_url"])}$', "$options": 'i'}}
                )
                
                base_fields = {
                    "updated_at": current_time,
                    "is_updated": 0 
                }
                
                if url_doc:
                    base_fields.update({
                        "count": url_doc.get("count"),
                        "google_page": url_doc.get("google_page"),
                        "index": url_doc.get("index"),
                        "search_keyword": format_field(url_doc.get("orgkey")),
                        "search_sector": format_field(url_doc.get("sector")),
                        "search_tag": format_field(url_doc.get("tag")),
                        "source_url_id": url_doc["_id"]  # Reference to original URL
                    })
                
                if not existing_org:
                    org_data = {
                        **obj,
                        **base_fields,
                        "created_at": current_time,
                        "is_updated": 1  
                    }
                    bulk_operations.append(InsertOne(org_data))
                    stats['new_organizations'] += 1
                else:
                    update_data = {
                        **obj,
                        **base_fields
                    }
                    bulk_operations.append(
                        UpdateOne(
                            {"_id": existing_org["_id"]},
                            {"$set": update_data}
                        )
                    )
                    stats['updated_organizations'] += 1
                    
            except Exception as e:
                stats['errors'] += 1
                logger.error(f"Error processing {obj.get('organization_url')}: {str(e)}")
                continue
        
        try:
            if bulk_operations:
                result = crunch_organization_details.bulk_write(bulk_operations)
                logger.log(f"Organization update stats: {stats}")
                return {
                    'success': True,
                    'stats': stats
                }
        except BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
            return {
                'success': False,
                'error': str(e),
                'stats': stats
            }
        
        return {
            'success': True,
            'stats': stats
        }
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, ConnectionFailure
from bson.objectid import ObjectId
from bs4 import BeautifulSoup
import re, urllib.parse, requests
from logger import CustomLogger
import base64, pytz, os, json, random, time
from datetime import datetime, timedelta
from pymongo.errors import DuplicateKeyError
tz = pytz.timezone('Asia/Kolkata')
current_time = datetime.now(tz)
from catch_coockies import CatchCookies

logger = CustomLogger(log_file_path="log/update.log")
MONGO_URI = "mongodb://test_user:asdfghjkl@65.108.33.28:27017/STARTUPSCRAPERDATA-BACKUP?authSource=STARTUPSCRAPERDATA-BACKUP&tls=true&tlsAllowInvalidCertificates=true"

try:
    masterclient = MongoClient(MONGO_URI, serverSelectionTimeoutMS=50000)  # 50 seconds timeout
    masterclient.admin.command('ping')  # Ping the server to check if it is available
    logger.log("MongoDB connection established successfully!")

except ConnectionFailure as e:
    logger.error(f"Could not connect to MongoDB: {e}")

clientdb = masterclient['STARTUPSCRAPERDATA']
crunch_organization_details = clientdb['OrganiztionDetails']
patentdbclient = masterclient.PATENTSSCRAPERDATA
patent_details = patentdbclient.Patents_Details

clientdb = masterclient['STARTUPSCRAPERDATA']
child_keywords = clientdb.Crunch_Keywords 
crunch_raw_urls = clientdb.CrunchURLS
crunch_organization_details=clientdb.OrganiztionDetails
blacklisted_keywords=clientdb.Blacklist_keywords
    
CONFIG_FILE = "config.json"

with open(CONFIG_FILE, "r") as f: config = json.load(f)
    
TOKEN = config["token_1"]
COOKIE_FILE = "session_data.json"
OUTPUT_DIR = "results"

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

def reset_isupdate_status():
    numberofrecords = 1
    where_condition = {"is_updated": 0}
    pipeline = [{"$match": where_condition}, {"$sample": {"size": numberofrecords}}]
    random_documents = list(crunch_organization_details.aggregate(pipeline))
    if len(random_documents) == 0: 
        logger.log("resetting the Update status")
        filter_query = {'is_updated': 1}
        update_query = {'$set': {'is_updated': 0}}
        update_result = crunch_organization_details.update_many(filter_query, update_query)
        logger.log("resetting the Update statuss completed")
   
def read_crunch_details(numberofrecords):
    random_documents = []
    bulk_operations = []
    where_condition = { "organization_url": "https://www.crunchbase.com/organization/claimbuddy-in"}
    where_condition = {"financial.funding_round.table.1.money_raised":"obfuscatedobfuscated"}
    # pipeline = [{"$match": where_condition}, {"$sample": {"size": random_documents}}]
    # pipeline = [{"$match": where_condition}]
    pipeline = [{"$match": where_condition}, {"$sample": {"size": numberofrecords}}]
    random_documents = list(crunch_organization_details.aggregate(pipeline))
    crunch_organization_details.find(where_condition)
    if len(random_documents) > 0:
        
        # reset_isupdate_status()
        # random_documents = list(crunch_organization_details.aggregate(pipeline))
        for row in random_documents:
            bulk_operations.append(UpdateOne({"_id": row["_id"]}, {"$set": {"is_updated": 1}}))
            # crunch_organization_details.update_one({"_id": row["_id"]}, {"$set": {"is_updated": 1}})
        try:
            if bulk_operations:
                result = crunch_organization_details.bulk_write(bulk_operations)
            
        except BulkWriteError as e:
            logger.error(e.details)
    return random_documents

def check_records_updated():
    where_condition = {"is_updated": 0}
    pipeline = [{"$match": where_condition}, {"$sample": {"size": 1}}]
    random_documents = list(crunch_organization_details.aggregate(pipeline))
    if random_documents :
        return True
    return False

def update_crunch_detail(documents):
    bulk_operations = []
    bulk_url_operations = []
    
    result = ""
    update_stats = {
        'total_processed': 0,
        'patents_found': 0,
        'patents_not_found': 0,
        'errors': 0
    }

    for obj in documents:
        try:
            existing_doc = crunch_organization_details.find_one(
                {"organization_url": obj["organization_url"]}
            )
            
            if not existing_doc:
                continue
                
            # if description has any value then let the old description at the time of update organization details
            description = existing_doc.get('summary',{}).get('details','').get('description','')
            object_description = obj.get('summary',{}).get('details','').get('description','')
            if description and object_description: 
                obj['summary']['details']['description'] = description
            print(description)
            print(obj.get('summary',{}).get('details','').get('description',''))    
            
            update_stats['total_processed'] += 1
            
            update_doc = {
                **obj,
                "updated_at": current_time,
                "is_updated": 1,
                "last_processed_at": current_time
            }
            
            preserved_fields = [
                "count", "google_page", "index",
                "search_keyword", "search_sector", "search_tag"
            ]
            
            for field in preserved_fields:
                if field in existing_doc:
                    update_doc[field] = existing_doc[field]
            
            bulk_operations.append(
                UpdateOne(
                    {"_id": existing_doc["_id"]},
                    {"$set": update_doc}
                )
            )
            bulk_url_operations.append(
                UpdateOne(
                    {"url": obj["organization_url"]},
                    {"$set": {"update_first" : 0}}
                )
            )
            
        except Exception as e:
            update_stats['errors'] += 1
            logger.error(f"Error processing {obj.get('organization_url', 'unknown')}: {str(e)}")
            continue
        
    if bulk_url_operations :
        result = crunch_raw_urls.bulk_write(bulk_url_operations)

    if bulk_operations:
        try:
            result = crunch_organization_details.bulk_write(bulk_operations)
            logger.log(f"Successfully updated {len(bulk_operations)} documents")
            logger.log(f"Update stats: {update_stats}")
            return {
                'success': True,
                'updated_count': len(bulk_operations),
                'stats': update_stats
            }
        except BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
            return {
                'success': False,
                'error': str(e),
                'stats': update_stats
            }
    
    return {
        'success': True,
        'updated_count': 0,
        'stats': update_stats,
        'result' : result
    }
        
def get_patent_no(organization_name,legal_name):
    patents=[]
    organization_name = str(organization_name).replace('.',' ').replace('-',' ').replace(',','').strip()
    legal_name=str(legal_name).replace('.',' ').replace('-',' ').replace(',','').strip()
    
    patentdocuments = patent_details.find()
    for document in patentdocuments: 
        try:
            application_filed_by =''
            current_assignee = document['current_assignee']
            
            for event_key, event_value in document['application_events'].items():
                if 'Application filed by' in event_value['title']:
                    application_filed_by = str(event_value['title']).replace('Application filed by', '').strip()
                    break
                   
            if legal_name != '' and str(legal_name).lower() ==  str(current_assignee).lower():
                patent_no= document['patent_no']
                
            elif legal_name != '' and legal_name.lower() == str(application_filed_by).lower():
                patent_no= document['patent_no']
            
            elif organization_name != '' and organization_name.lower() == str(current_assignee).lower():
                patent_no= document['patent_no']
                
            elif organization_name != '' and organization_name.lower() == str(application_filed_by).lower():
                patent_no= document['patent_no']
            
            else:
                patent_no =""
            
            if patent_no:
                    patents.append(patent_no)
                
            
        except:
            pass
    return patents

def getToken():
    ''' 
        Author: Santhosh
        Description: This function reads the base64 encoded token for Scrapedo from
                    the config.json file in the script path and returns the 
                    base64 decoded tpoekn to be used the proxy URL
    '''
    script_path = os.path.dirname(os.path.realpath(__file__))
    config_file_path = os.path.join(script_path, "config.json")
    try:
        with open(config_file_path) as config_file:
            enc_token = json.load(config_file)['token_1']
            token = base64.b64decode(enc_token).decode("ascii").strip()
        return token
    except Exception as e:
        print(e)
        return None

def getProxies():
    ''' 
        Author: Santhosh
        Description: Returns the proxy list
        
        curl "http://api.scrape.do?token=50612111dbab405ca9c28aacbd4bf0e2dc7d7b4c269&url=https://httpbin.co/ip"
        
        
        curl --proxy http://50612111dbab405ca9c28aacbd4bf0e2dc7d7b4c269@proxy.scrape.do:8080 --insecure https://httpbin.co/ip
    '''
    TIMEOUT = 30000 #ms
    proxyModeUrl = "http://{}:timeout={}@proxy.scrape.do:8080".format(getToken(),TIMEOUT)
    return {
            "http": proxyModeUrl,
            "https": proxyModeUrl,
            }

def proxies():
    plist = [
        '5.79.73.131:13150',
    ]
    
    prx = random.choice(plist)
    return {
        'http': 'http://' + prx,
        'https': 'http://' + prx
    }

def load_session():
    with open(COOKIE_FILE, "r") as f: data = json.load(f)
    return data["session_id"], urllib.parse.quote_plus(data["cookies"])

def get_scrpido_requests(company_name : str, session_id, cookies):
    
    
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
        url = f"https://api.scrape.do/?token={TOKEN}&url={encoded_url}&render=true&super=true&sessionid={session_id}&setCookies={cookies}"
        try:
            # with open(COOKIE_FILE, "r") as f: data = json.load(f)
    
            # for _ in range(15):
            #     if data['updatting'] == True :
            #         time.sleep(random.randint(3,9))
            #     else :
            #         break
            response = requests.get(url)
            if response.status_code  == 200 :
                
                # success_login = BeautifulSoup(response.text, 'lxml').find_all('button',{'aria-label':'Account'})
                # if not success_login or needs_refresh():
                #     with open(COOKIE_FILE, "w") as f:
                #         data['status_update'] = False
                #         json.dump(data, f, indent=2)
                #     refresh_session()
                    
                #     session_id, cookies = load_session()
                #     continue
                    
                print(f"Scraped: {company_name}")
                return True, response
            else :
                print(f"Scrapped: but statuscode : {response.status_code}")
        except Exception as e:
            print(f"Failed to scrape {company_name}: {e}")
        
    return False, False


class main_setting:
    
    def __init__(self):
        self.logger = CustomLogger(log_file_path="log/new_scrapping.log")
        self.session_manager = CatchCookies()

    def random_sleep(self,a = 3, b = 7, reason = ""):
        sleep_t = random.randint(a,b)
        self.logger.log(f"Time sleep : {sleep_t} secounds ") if not reason else self.logger.log(f"Time sleep : {sleep_t} secounds for {reason}") 
        
    def read_crunch_details(self,numberofrecords):
        current_time = datetime.now(tz)
        one_month_ago = current_time - timedelta(days=30)
        
        # organizations_to_update = []
        # where_condition = { "summary.details.description": "" }
        # pipeline = [{"$match": where_condition}, {"$sample": {"size": numberofrecords}}]
        # organizations_to_update = list(crunch_organization_details.aggregate(pipeline))
        # return organizations_to_update
        
        organizations_to_update = []
        bulk_operations = []
        update_first_urls = []
        
        where_condition = {"financial.funding_round.table.1.money_raised":"obfuscatedobfuscated"}
        pipeline = [{"$match": where_condition}, {"$sample": {"size": numberofrecords}}]
        organizations_to_update = list(crunch_organization_details.aggregate(pipeline))
        if not organizations_to_update :
            
            for _ in range(5):
                    
                where_condition = {"update_first": 1}
                pipeline = [{"$match": where_condition}, {"$sample": {"size": numberofrecords}}]
                update_first_urls = list(crunch_raw_urls.aggregate(pipeline))
                
                if update_first_urls:
                    org_urls = [url_doc["url"] for url_doc in update_first_urls]
                    organizations_to_update = list(crunch_organization_details.find(
                        {
                            "organization_url": {"$in": org_urls},
                            "$or": [
                                {"updated_at": {"$lt": one_month_ago}},
                                {"updated_at": {"$exists": False}},
                                {"is_updated": 0}
                            ]
                        }
                    ))
                    
                    bulk_operations.extend(
                        UpdateOne(
                            {"_id": url_doc["_id"]},
                            {"$set": {"update_first": 0, "processed_at": current_time}}
                        )
                        for url_doc in update_first_urls
                    )
                if numberofrecords <= len(organizations_to_update) : break
                
        remaining_slots = numberofrecords - len(organizations_to_update)
        
        if remaining_slots > 0:
            pipeline = [
                {"$match": {
                    "$or": [
                        {"updated_at": {"$lt": one_month_ago}},
                        {"updated_at": {"$exists": False}},
                        {"is_updated": 0}
                    ]
                }},
                {"$sample": {"size": remaining_slots}},
                {"$project": {
                    "_id": 1,
                    "organization_url": 1,
                    "updated_at": 1,
                    "is_updated": 1
                }}
            ]
            
            additional_orgs = list(crunch_organization_details.aggregate(pipeline))
            organizations_to_update.extend(additional_orgs)

        if organizations_to_update:
            update_operations = [
                UpdateOne(
                    {"_id": org["_id"]},
                    {"$set": {
                        "is_updated": 0,
                        "update_queued_at": current_time
                    }}
                )
                for org in organizations_to_update
            ]
            bulk_operations.extend(update_operations)
            
            try:
                if bulk_operations:
                    crunch_organization_details.bulk_write(bulk_operations)
                    logger.log(f"Queued {len(organizations_to_update)} organizations for update")
            except BulkWriteError as e:
                logger.error(f"Bulk write error: {e.details}")
                return []
            
        return organizations_to_update
    
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
            url = f"https://api.scrape.do/?token={TOKEN}&url={encoded_url}&render=true&super=true&sessionid={session_id}&setCookies={cookies}"
            try:
                response = requests.get(url)
                if response.status_code == 429:
                    self.random_sleep(reason="Error status code 429")
                    
                elif response.status_code == 502:
                    self.logger.error(f"Company Not found : {company_name}")
                    file_name = "502_update.html"
                    with open(file_name, '+w') as f:f.write(response.text)
                    
                elif response.status_code == 404:
                    self.logger.error(f"Company Not found : {company_name}")
                    file_name = "404_update.html"
                    with open(file_name, '+w') as f:f.write(response.text)
                    
                    crunch_raw_urls.update_one(
                        {"url": company_name,},
                        {
                            "$set": {"status": "completed"},
                        }
                    )
                    return False, False
                
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


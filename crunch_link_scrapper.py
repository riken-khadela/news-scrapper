# Import necessary modules
import requests, time, os, datetime, re, random, json, urllib
from bs4 import BeautifulSoup
from datetime import date, timedelta
from logger import CustomLogger
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError, ConnectionFailure, DuplicateKeyError, PyMongoError

logger = CustomLogger(log_file_path="log/scrape_keywords2.log")
CONFIG_FILE = "config.json"

with open(CONFIG_FILE, "r") as f: config = json.load(f)
    
TOKEN = config["token_1"]
COOKIE_FILE = "session_data.json"
OUTPUT_DIR = "results"

numberofrecords = 100

MONGO_URI = "mongodb://test_user:asdfghjkl@65.108.33.28:27017/STARTUPSCRAPERDATA-BACKUP?authSource=STARTUPSCRAPERDATA-BACKUP&tls=true&tlsAllowInvalidCertificates=true"

# Connect to MongoDB with timeout settings
try:
    masterclient = MongoClient(MONGO_URI, serverSelectionTimeoutMS=50000)  # 50 seconds timeout
    # Verify the connection
    masterclient.admin.command('ping')  # Ping the server to check if it is available

    logger.log("MongoDB connection established successfully!")

except ConnectionFailure as e:
    logger.error(f"Could not connect to MongoDB: {e}")
clientdb = masterclient['STARTUPSCRAPERDATA']
crunch_raw_urls = clientdb.CrunchURLS
crunch_organization_details=clientdb.OrganiztionDetails
child_keywords = clientdb.Crunch_Keywords
blacklisted_keywords=clientdb.Blacklist_keywords

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

def read_crunch_keywords(numberofrecords):
    
    bulk_operations = []
    where_condition = {"status":0}
    where_condition = {"$or": [{"is_read": 0}, {"status": "pending"}]} #{"is_read":0}
    # "$or": [{"is_read": 0}, {"status": "pending"}]
    pipeline = [{"$match": where_condition}, {"$sample": {"size": numberofrecords}}]
    random_documents = list(child_keywords.aggregate(pipeline))
    
        
    # update each document with a new value
    return random_documents

def insert_multiple_urls_from_google(documents):
    # set a unique index on the collection
    logger.log("total records found: " +str(len(documents)))
    
    crunch_raw_urls.create_index([("url", 1)], unique=True)
    bulk_operations = []
    new_data_bulk_operations=[]
    update_org_details=[]
    for obj in documents:
        try:
            # Check if the URL already there
            existing_doc = crunch_raw_urls.find_one({"url": obj["url"]})
            if not existing_doc:
                new_data_bulk_operations.append(InsertOne(obj))
            
            if existing_doc:
                logger.log("found")
                
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
    logger.log("total new records found: " +str(len(new_data_bulk_operations)))
    
    logger.log("total old records updated: " +str(len(bulk_operations)))
    
    try:
        if bulk_operations:
            result = crunch_raw_urls.bulk_write(bulk_operations)
        if new_data_bulk_operations:
            result = crunch_raw_urls.bulk_write(new_data_bulk_operations)
        # added 15-09-2023
        if update_org_details:
            result = crunch_organization_details.bulk_write(update_org_details)
    except BulkWriteError as e:
        logger.error(e.details)

# Function Used in crunch_link_scraper
def insert_blacklist_keywords(documents):
    try:
        blacklisted_keywords.create_index([("orgkeyword", 1)], unique=True)
        result = blacklisted_keywords.insert_one(documents)
        logger.log("No Links Found - Keyword inserted into Blacklist_keywords.")
    except DuplicateKeyError:
        logger.error("Blacklist Keyword with the same key already exists. Skipping insertion.")
    except Exception as e:
        logger.error("An error occurred:", str(e))
        
# Define a function to get the html content of search results from Google 
# for a particular search query and page number

def get_proxies():
    plist = [
        '37.48.118.90:13082',
        '83.149.70.159:13082'
    ]
    prx = random.choice(plist)
    return {
        'http': 'http://' + prx,
        'https': 'http://' + prx
    }
    
def get_request(search, page):
    """
    This function sends a request to Google with a given search query and page number
    and returns the response object if successful.

    Args:
    - search (str): the search query to send to Google
    - page (int): the page number of the search results to request

    Returns:
    - tuple: a tuple containing a boolean value indicating whether the request was successful
    and the response object if successful, otherwise False
    """
    logger.log('searching for : '+search+' and page :' +str(page))
    proxies=get_proxies()
    isData=True
    while isData:
        try:
            getHeaders = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"}
            
            #Google request to get html body of the search
            
            #  working 
            # url = f"https://www.google.com/search?q={urllib.parse.quote_plus(search)}"
            # url = f"http://api.scrape.do/?token={TOKEN}&url={url}&render=true"
            # res = requests.get(url)
            
            # strom with brave
            url = f"https://search.brave.com/search?q={search}"
            res = requests.get(url, proxies=proxies, timeout=30)
            
            
            # https://www.google.com/search?q=loreal+site%3Awww.crunchbase.com%2Forganization%2F&oq=lo&gs_lcrp=EgZjaHJvbWUqCAgAEEUYJxg7MggIABBFGCcYOzIMCAEQABhDGIAEGIoFMgYIAhBFGDkyBggDEEUYPDIGCAQQRRg8MgYIBRBFGDwyBggGEEUYPDIGCAcQRRg80gEIMTAzOWowajeoAgCwAgA&sourceid=chrome&ie=UTF-8
            
            
            # queryParameters = {"q": str(search), "start": page}
            # getHeaders = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"}
            # res = requests.get("https://www.google.com/search", params=queryParameters, headers=getHeaders, timeout=60,proxies=proxies)
            # res = requests.get("https://www.google.com/search", params=queryParameters, headers=getHeaders, timeout=20)
            # res = requests.get(url, headers=getHeaders, timeout=20 ,proxies=proxies)
            logger.log(res.status_code)
            
            if res.status_code== 200:
                return True, res
            
            logger.log(res.status_code)
            
        except Exception as e:
            logger.error(e)
        time.sleep(0.1)
        logger.log("Retrying again for:" + str(search))
        
    return False, False

def collect_links_and_store(res, key,search,page):
    """
    This function collects links from the given search results and stores them in the database.

    Args:
    - res (requests.models.Response): the response object containing the search results
    - key (dict): a dictionary containing information about the search keywords
    - search (str): the search query used to obtain the search results
    - operator (str): the search operator used to obtain the search results
    - page (int): the page number of the search results

    Returns:
    - list: a list of dictionaries containing the links to be stored in the database
    """
    insert_links = []
    current_page = (page / 10) + 1
    try:
        
        data = BeautifulSoup(res.text, 'lxml')
        main_div = data.find_all('a')
        
        index_url = 0
        for link in main_div : 
            if link.get('href') and "https://www.crunchbase.com/organization/" in link.get('href') and link.find('div',{'class','title'}) :
                link.find('div',{'class','title'}).text
                index_url += 1
                
                href = link.get('href')
                if 'https://www.crunchbase.com/organization/' in href:
                    test_href=href.split('https://www.crunchbase.com/organization/')[1]
                    if '/' not in test_href:
                        obj={}
                        obj['sector'] = ''
                        obj['orgkey'] = ''
                        obj['tag'] = ''
                        if current_page == 1 and index_url == 1:
                            obj['sector'] = key['sector']
                            obj['orgkey'] = key['orgkey']
                            obj['tag'] = key['tag']
                        obj['search_string'] = search
                        obj['url'] = href
                        obj['created_at'] = datetime.datetime.now()
                        obj['is_read'] = 0
                        obj['status'] = 'pending'
                        obj['index'] = index_url
                        obj['google_page'] = current_page
                        obj['count'] = 1
                        obj['update_first'] = 1
                        insert_links.append(obj)
                        print(obj)
                        logger.log(href) 
        
    except Exception as e:
        logger.error(e)
        pass
    
    try:
        child_keywords.update_one({"_id": key["_id"]}, {"$set": {"status": 1, "is_read" : 1}})
    except PyMongoError as e:
        logger.error(e.details)
        
    if len(insert_links) > 0:
        insert_multiple_urls_from_google(insert_links)
    return insert_links

def collect_page_details():
    """
    This function collects search results from Google for a list of keywords and stores the relevant links in a database.
    """
    m = random.randrange(2,10)
    time.sleep(m)
    keywords = read_crunch_keywords(numberofrecords)
    for key in keywords:
        page = 0
        isfound=False
        while True:
            search = str(key['orgkey']+ ' site:www.crunchbase.com/organization/')

            isdone, res = get_request(search, page)
            if isdone:
                insert_links = collect_links_and_store(res, key,search,page)
                if isfound ==False and len(insert_links)==0:
                    obj={
                        'orgkeyword':key['orgkey'],
                        'search_string':search,
                        'created_at': datetime.datetime.now(),
                    }
                    insert_blacklist_keywords(obj)
                    break
                elif len(insert_links) > 0 and page < 20:
                    isfound = True
                    page = page + 10
                else:
                    break
            else:
                break
# collect_page_details()
# if __name__ == "__main__":
#     try:
#         file_path = os.path.dirname(os.path.realpath(__file__))
#         filename = os.path.join(file_path, "crunchlink_status1.txt")
#         f = open(filename, 'r')
#         txt = f.read()
#         f.close()
#         if str(txt) == "0":
#             f = open(filename, 'w')
#             f.write("1")
#             f.close()
#             try:
#                 separator_line =  '-' * 40
#                 print(separator_line)
#                 print("            Starting Scraper1           ")
#                 print(separator_line)
#                 collect_page_details()
#                 print(separator_line)
#                 print("            Stopping Scraper1           ")
#                 print(separator_line)
#             except Exception as e:
#                 print(e)
#                 pass
#             f = open(filename, 'w')
#             f.write("0")
#             f.close()
#         else:
#             print("***** Another process is already running *****")
#     except Exception as e:
#         print(f"Error before running the script: {e}")

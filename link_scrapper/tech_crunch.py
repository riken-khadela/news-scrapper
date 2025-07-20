import requests
import urllib.parse
import datetime
import logging
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time, os
import settings as cf

TOKEN = "50612111dbab405ca9c28aacbd4bf0e2dc7d7b4c269"

logger_file = os.path.join(os.getcwd(),'log','tech_crunch.log')

logging.basicConfig(
    filename=cf.check_log_file(logger_file),
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

def get_google_search_results(search_term = "Funding & Investment", country_code='US'):
    try:
        headers = {"User-Agent": UserAgent().random}
        query = f"{search_term} site:techcrunch.com"
        params = {
            "q": query,
            "tbs": "qdr:d",  
        }

        search_url = f"https://www.google.com/search?{urllib.parse.urlencode(params)}"

        response = requests.get(search_url, headers=headers, timeout=10, proxies=cf.proxies())
        if response.status_code == 200:
            logging.info("Fetched results for: %s", search_term)
            return response.text
        else:
            logging.warning("Failed to fetch search results: HTTP %s", response.status_code)
    except Exception as e:
        logging.error("Exception during Google Search fetch: %s", str(e))
    return None



def parse_google_results(html, key_data, tag, search_term):
    soup = BeautifulSoup(html, 'lxml')
    extracted_links = []
    index = 0
    exclude_domains = cf.get_exclude_domains()
    
    anchor_tags = soup.find_all("a", class_="zReHs")
    print(len(anchor_tags))
    for a_tag in anchor_tags:
        skip_links = [
            "https://techcrunch.com/latest/",
            'https://techcrunch.com/category/'
        ]
        
        href = a_tag.get("href")
        if not href or "techcrunch.com" not in href:
            continue
        elif href == "https://techcrunch.com/" or href in skip_links or href in skip_links:
            continue
        
        exclude_match = any(excl.lower() in href.lower() for excl in exclude_domains)
        if exclude_match:
            continue

        index += 1
        title_element = a_tag.find("h3")
        title = title_element.get_text(strip=True) if title_element else "No Title Found"

        obj = {
            "sector": key_data["sector"],
            "tag" : tag,
            "search_string": search_term,
            "url": href,
            "title": title,
            "pub_date": datetime.datetime.now().replace(hour=0, minute=0, second=0),
            "created_at": datetime.datetime.now(),
            "is_read": 0,
            "status": "pending",
            "index": index,
            "google_page": 1,
            "count": 1
        }

        extracted_links.append(obj)
        logging.info("Collected URL: %s", href)

    logging.info("Completed parsing results for: %s", search_term)
    return extracted_links

def collect_page_details(sector, keywords, geo_locations = ['US']):
    data = []
    for geo in geo_locations:
        search_term = f"{sector['sector']} + {keywords}"
        html = get_google_search_results(search_term, geo)
        if html:
            data = parse_google_results(html, sector, keywords, search_term)
            time.sleep(2)
        else:
            logging.warning("No HTML returned for: %s", search_term)
    
    return data
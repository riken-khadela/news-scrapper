import requests
import urllib.parse
import datetime
import logging
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time, random, os
import settings as cf 



# log_file = os.path.join(os.getcwd(),'log','inc42.log')

# print(
#     filename=cf.check_log_file(log_file),
#     level=print,
#     format='%(asctime)s [%(levelname)s] %(message)s'
# )


def get_google_search_results(search_term , country_code='US'):
    try:
        headers = {"User-Agent": UserAgent().random}
        query = f"{search_term} site:inc42.com"
        params = {
            "q": query, "tbs": "qdr:w", 
             
        }
        search_url = f"https://www.google.com/search?{urllib.parse.urlencode(params)}"

        response = requests.get(search_url, headers=headers, timeout=10, proxies=cf.proxies())
        
        if response.status_code == 200:
            print("Fetched results for: %s", search_term)
            return response.text
        
    except Exception as e:
        print("Exception during Google Search fetch: %s", str(e))
    return None



def parse_google_results(html, key_data, tag, search_term):
    soup = BeautifulSoup(html, 'lxml')
    extracted_links = []
    index = 0
    
    
    
    # Find all Google result anchors by their class
    anchor_tags = soup.find_all("a")
    for a_tag in anchor_tags:
        
        href = a_tag.get("href")
        print(href)
        if not href or "inc42.com" not in href:
            continue
        elif href == "https://www.inc42.com/" or "https://www.inc42.com/latest/" in href:
            continue
        elif href.startswith("/url?q") or href.startswith("/search?"):
            continue
        elif not "inc42.com" in href:
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
        print("Collected URL: %s", href)

    print("Completed parsing results for: %s", search_term)
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
            print("No HTML returned for: %s", search_term)
    
    return data


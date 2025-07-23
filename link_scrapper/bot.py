from bs4 import BeautifulSoup
import datetime
import requests
from fake_useragent import UserAgent
import urllib.parse
import settings as cf
import logging


def get_google_search_results(search_term, site_name, time, logger, country_code='US' ):
    if time not in ['d', 'w', 'm', 'y']:
        raise ValueError("Invalid time parameter. Use 'd' for day, 'w' for week, 'm' for month, or 'y' for year.")

    if not site_name:
        raise ValueError("Site name must be provided.")
    
    if not search_term:
        raise ValueError("Search term must be provided.")
    
    search_term = search_term.strip()
    if not search_term:
        raise ValueError("Search term cannot be empty.")
    
    try:
        headers = {"User-Agent": UserAgent().random}
        query = f"{search_term} site:{site_name}"
        params = {
            "q": query, "tbs": f"qdr:{time}",
             
        }
        search_url = f"https://www.google.com/search?{urllib.parse.urlencode(params)}"

        breakpoint()
        response = requests.get(search_url, headers=headers, timeout=10, proxies=cf.proxies())
        if response.status_code == 200:
            logger.info("Fetched results for: %s", search_term)
            return response.text
        
    except Exception as e:
        logger.error("Exception during Google Search fetch: %s", str(e))
        
    return None

def parse_google_results(html, key_data, tag, search_term, site_name, logger ):
    soup = BeautifulSoup(html, 'lxml')
    extracted_links = []
    index = 0

    anchor_tags = soup.find_all("a")
    for a_tag in anchor_tags:

        href = a_tag.get("href")
        if not href or site_name not in href:
            continue
        elif href == f"https://www.{site_name}/" or href == f"https://{site_name}/" or f"https://www.{site_name}/latest/" in href or f"https://{site_name}/latest/" in href:
            continue
        elif href.startswith("/url?q") or href.startswith("/search?"):
            continue
        elif "google.com" in href or "googleusercontent.com" in href:
            continue
            
        index += 1
        title_element = a_tag.find("h3")
        if not title_element :
            continue
        
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
        logger.info("Collected URL: %s", href)

    return extracted_links